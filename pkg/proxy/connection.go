package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/jackc/pgx/v5/stdlib" // Import pgx driver

	"github.com/verifiable-postgres/proxy/pkg/capture"
	"github.com/verifiable-postgres/proxy/pkg/commitment"
	"github.com/verifiable-postgres/proxy/pkg/interceptor"
	"github.com/verifiable-postgres/proxy/pkg/log"
	"github.com/verifiable-postgres/proxy/pkg/types"
)

// Connection represents a client connection to the proxy
type Connection struct {
	ctx             context.Context
	cancel          context.CancelFunc
	server          *Server
	id              uint64
	clientConn      net.Conn
	backendDB       *sql.DB
	backendTx       *sql.Tx
	txID            uint64
	inTransaction   bool
	transactionMode byte // 'I' (idle), 'T' (in transaction), 'E' (failed transaction)
	closed          bool
	mu              sync.Mutex
	queryCount      int // Count of queries in the current transaction
}

// NewConnection creates a new connection
func NewConnection(ctx context.Context, server *Server, id uint64, clientConn net.Conn) (*Connection, error) {
	// Create context with cancel
	ctx, cancel := context.WithCancel(ctx)

	// Connect to the backend database
	backendDB, err := sql.Open("pgx", server.cfg.BackendDB.DSN())
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to connect to backend DB: %w", err)
	}

	// Test the connection
	if err := backendDB.PingContext(ctx); err != nil {
		backendDB.Close()
		cancel()
		return nil, fmt.Errorf("failed to ping backend DB: %w", err)
	}

	// Create the connection
	conn := &Connection{
		ctx:             ctx,
		cancel:          cancel,
		server:          server,
		id:              id,
		clientConn:      clientConn,
		backendDB:       backendDB,
		inTransaction:   false,
		transactionMode: 'I', // Idle
		queryCount:      0,
	}

	// Update metrics
	server.metrics.ConnectionStarted()

	log.Info("New connection established", "id", id)
	return conn, nil
}

// Handle handles the connection
func (c *Connection) Handle() error {
	defer c.Close()

	// Initialize transaction mode
	c.transactionMode = 'I' // Idle - not in transaction

	// Handle the startup message
	startupMsg, err := c.readStartupMessage()
	if err != nil {
		return fmt.Errorf("failed to receive startup message: %w", err)
	}

	// Process the startup message based on its type
	switch msg := startupMsg.(type) {
	case *pgproto3.StartupMessage:
		// Handle normal startup
		if err := c.handleStartupMessage(msg); err != nil {
			return err
		}
	case *pgproto3.SSLRequest:
		// We don't support SSL/TLS in the V1 MVP
		_, err := c.clientConn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("failed to send SSL rejection: %w", err)
		}
		return fmt.Errorf("SSL/TLS not supported")
	case *pgproto3.CancelRequest:
		// We don't support cancel requests in the V1 MVP
		return fmt.Errorf("cancel requests not supported")
	default:
		return fmt.Errorf("unsupported startup message type: %T", msg)
	}

	// Main message loop
	for {
		// Read message type (first byte)
		typeBuf := make([]byte, 1)
		_, err := io.ReadFull(c.clientConn, typeBuf)
		if err != nil {
			if err == io.EOF {
				return nil // Clean disconnect
			}
			return fmt.Errorf("failed to read message type: %w", err)
		}

		// Read message length (4 bytes)
		lenBuf := make([]byte, 4)
		_, err = io.ReadFull(c.clientConn, lenBuf)
		if err != nil {
			return fmt.Errorf("failed to read message length: %w", err)
		}

		// Calculate message length (excludes the length itself)
		messageLen := int(lenBuf[0])<<24 | int(lenBuf[1])<<16 | int(lenBuf[2])<<8 | int(lenBuf[3])
		messageLen -= 4 // Subtract length field size

		// Read message body
		messageBuf := make([]byte, messageLen)
		if messageLen > 0 {
			_, err = io.ReadFull(c.clientConn, messageBuf)
			if err != nil {
				return fmt.Errorf("failed to read message body: %w", err)
			}
		}

		// Process message based on type code
		switch typeBuf[0] {
		case 'Q': // Query
			// Extract the query string (null-terminated)
			if len(messageBuf) < 1 {
				return fmt.Errorf("invalid query message format")
			}
			queryString := string(messageBuf[:len(messageBuf)-1]) // Remove null terminator
			if err := c.handleQuery(&pgproto3.Query{String: queryString}); err != nil {
				return err
			}
		case 'P': // Parse (Extended Query Protocol)
			if err := c.handleExtendedQuery(typeBuf[0], messageBuf); err != nil {
				return err
			}
		case 'B': // Bind (Extended Query Protocol)
			if err := c.handleExtendedQuery(typeBuf[0], messageBuf); err != nil {
				return err
			}
		case 'E': // Execute (Extended Query Protocol)
			if err := c.handleExtendedQuery(typeBuf[0], messageBuf); err != nil {
				return err
			}
		case 'D': // Describe (Extended Query Protocol)
			if err := c.handleExtendedQuery(typeBuf[0], messageBuf); err != nil {
				return err
			}
		case 'S': // Sync (Extended Query Protocol)
			if err := c.handleExtendedQuery(typeBuf[0], messageBuf); err != nil {
				return err
			}
		case 'X': // Terminate
			// End any open transaction before terminating
			if c.inTransaction && c.backendTx != nil {
				c.backendTx.Rollback()
				c.inTransaction = false
				c.backendTx = nil
				// Update metrics for implicit rollback on connection termination
				c.server.metrics.TransactionRolledBack()
			}
			return nil // Connection will be closed after this function returns
		default:
			// Log and handle the unsupported message, but don't disconnect
			log.Warn("Unsupported message type", "type", string(typeBuf))
			// Send error to client
			err := c.sendErrorResponse("ERROR", "08P01", fmt.Sprintf("Unsupported message type: %c", typeBuf[0]))
			if err != nil {
				return fmt.Errorf("failed to send error response: %w", err)
			}
			
			// Send ReadyForQuery to keep the connection alive
			err = c.sendReadyForQuery()
			if err != nil {
				return fmt.Errorf("failed to send ReadyForQuery: %w", err)
			}
		}
	}
}

// handleExtendedQuery handles extended query protocol messages
// This is a placeholder in V1 that just returns an error message
// to the client that extended query protocol is not supported yet
func (c *Connection) handleExtendedQuery(msgType byte, msgBody []byte) error {
	// Log attempt
	log.Warn("Extended Query Protocol not fully supported", "type", string(msgType))
	
	// Send error to client
	err := c.sendErrorResponse("ERROR", "0A000", "Extended Query Protocol not fully supported in V1")
	if err != nil {
		return fmt.Errorf("failed to send error response: %w", err)
	}
	
	// Send ReadyForQuery
	err = c.sendReadyForQuery()
	if err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}
	
	return nil
}

// readStartupMessage reads the startup message from the client
func (c *Connection) readStartupMessage() (interface{}, error) {
	// Read the first 4 bytes to get the message size
	sizeBuf := make([]byte, 4)
	_, err := io.ReadFull(c.clientConn, sizeBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to read message size: %w", err)
	}

	// Calculate message size (Big Endian)
	messageSize := int(sizeBuf[0])<<24 | int(sizeBuf[1])<<16 | int(sizeBuf[2])<<8 | int(sizeBuf[3])
	if messageSize < 8 {
		return nil, fmt.Errorf("invalid message size: %d", messageSize)
	}

	// Read the rest of the message
	messageBuf := make([]byte, messageSize-4)
	_, err = io.ReadFull(c.clientConn, messageBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to read message body: %w", err)
	}

	// Determine message type from protocol version
	protocolVersion := int(messageBuf[0])<<24 | int(messageBuf[1])<<16 | int(messageBuf[2])<<8 | int(messageBuf[3])

	// Handle different message types
	if protocolVersion == 80877103 {
		// SSL request
		return &pgproto3.SSLRequest{}, nil
	} else if protocolVersion == 80877102 {
		// Cancel request
		if len(messageBuf) < 12 {
			return nil, fmt.Errorf("cancel request message too short")
		}
		processID := uint32(messageBuf[4])<<24 | uint32(messageBuf[5])<<16 | uint32(messageBuf[6])<<8 | uint32(messageBuf[7])
		secretKey := uint32(messageBuf[8])<<24 | uint32(messageBuf[9])<<16 | uint32(messageBuf[10])<<8 | uint32(messageBuf[11])
		return &pgproto3.CancelRequest{
			ProcessID: processID,
			SecretKey: secretKey,
		}, nil
	} else if protocolVersion == 196608 {
		// Startup message
		parameters := make(map[string]string)
		
		// Parse parameters (null-terminated key-value pairs)
		i := 8 // Start after protocol version (4 bytes) and first 0 terminator
		for i < len(messageBuf) {
			// Extract key
			keyStart := i
			for i < len(messageBuf) && messageBuf[i] != 0 {
				i++
			}
			if i >= len(messageBuf) {
				break
			}
			key := string(messageBuf[keyStart:i])
			i++ // Skip null terminator
			
			// Extract value
			valueStart := i
			for i < len(messageBuf) && messageBuf[i] != 0 {
				i++
			}
			if i >= len(messageBuf) {
				break
			}
			value := string(messageBuf[valueStart:i])
			i++ // Skip null terminator
			
			parameters[key] = value
		}
		
		return &pgproto3.StartupMessage{
			ProtocolVersion: uint32(protocolVersion),
			Parameters:      parameters,
		}, nil
	}
	
	return nil, fmt.Errorf("unsupported protocol version: %d", protocolVersion)
}

// handleStartupMessage handles the startup message
func (c *Connection) handleStartupMessage(msg *pgproto3.StartupMessage) error {
	// Log startup
	log.Info("Client startup",
		"id", c.id,
		"user", msg.Parameters["user"],
		"database", msg.Parameters["database"])

	// In V1, we use a very simple trust authentication
	// Send AuthenticationOk message
	authOk := []byte{
		'R',                          // Authentication response
		0, 0, 0, 8,                   // Message length (including self)
		0, 0, 0, 0,                   // Authentication OK (0)
	}
	_, err := c.clientConn.Write(authOk)
	if err != nil {
		return fmt.Errorf("failed to send AuthenticationOk: %w", err)
	}

	// Send server parameters
	params := map[string]string{
		"server_version": "14.0.0",
		"client_encoding": "UTF8",
		"DateStyle":      "ISO, MDY",
		"TimeZone":       "UTC",
	}
	for name, value := range params {
		// Calculate message length
		nameBytes := []byte(name)
		valueBytes := []byte(value)
		messageLen := 4 + len(nameBytes) + 1 + len(valueBytes) + 1

		// Build message
		message := make([]byte, 1+4+messageLen)
		message[0] = 'S' // ParameterStatus
		message[1] = byte((messageLen + 4) >> 24)
		message[2] = byte((messageLen + 4) >> 16)
		message[3] = byte((messageLen + 4) >> 8)
		message[4] = byte(messageLen + 4)

		// Copy name and value
		copy(message[5:], nameBytes)
		message[5+len(nameBytes)] = 0 // Null terminator
		copy(message[5+len(nameBytes)+1:], valueBytes)
		message[5+len(nameBytes)+1+len(valueBytes)] = 0 // Null terminator

		_, err := c.clientConn.Write(message)
		if err != nil {
			return fmt.Errorf("failed to send ParameterStatus: %w", err)
		}
	}

	// Send BackendKeyData (dummy values)
	keyData := []byte{
		'K',                          // BackendKeyData
		0, 0, 0, 12,                  // Message length (including self)
		0, 0, 0, 1,                   // Process ID (1)
		0, 0, 0, 2,                   // Secret key (2)
	}
	_, err = c.clientConn.Write(keyData)
	if err != nil {
		return fmt.Errorf("failed to send BackendKeyData: %w", err)
	}

	// Send ReadyForQuery
	readyForQuery := []byte{
		'Z',                          // ReadyForQuery
		0, 0, 0, 5,                   // Message length (including self)
		'I',                          // Idle state
	}
	_, err = c.clientConn.Write(readyForQuery)
	if err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}

	return nil
}

// sendErrorResponse sends an error response to the client
func (c *Connection) sendErrorResponse(severity, code, message string) error {
	// Calculate message length
	severityBytes := []byte(severity)
	codeBytes := []byte(code)
	messageBytes := []byte(message)
	messageLen := 4 + 1 + len(severityBytes) + 1 + 1 + len(codeBytes) + 1 + 1 + len(messageBytes) + 1 + 1

	// Build message
	msg := make([]byte, 1+4+messageLen)
	msg[0] = 'E' // ErrorResponse
	msg[1] = byte((messageLen + 4) >> 24)
	msg[2] = byte((messageLen + 4) >> 16)
	msg[3] = byte((messageLen + 4) >> 8)
	msg[4] = byte(messageLen + 4)

	// Fill in fields
	pos := 5
	
	// Severity field
	msg[pos] = 'S'
	pos++
	copy(msg[pos:], severityBytes)
	pos += len(severityBytes)
	msg[pos] = 0 // Null terminator
	pos++
	
	// Code field
	msg[pos] = 'C'
	pos++
	copy(msg[pos:], codeBytes)
	pos += len(codeBytes)
	msg[pos] = 0 // Null terminator
	pos++
	
	// Message field
	msg[pos] = 'M'
	pos++
	copy(msg[pos:], messageBytes)
	pos += len(messageBytes)
	msg[pos] = 0 // Null terminator
	pos++
	
	// Zero terminator for the message
	msg[pos] = 0

	_, err := c.clientConn.Write(msg)
	return err
}

// sendReadyForQuery sends a ReadyForQuery message to the client
func (c *Connection) sendReadyForQuery() error {
	// Use the appropriate transaction status
	txStatus := 'I' // Idle - not in transaction
	if c.inTransaction {
		if c.transactionMode == 'E' {
			txStatus = 'E' // Failed transaction
		} else {
			txStatus = 'T' // In transaction
		}
	}

	readyForQuery := []byte{
		'Z',                          // ReadyForQuery
		0, 0, 0, 5,                   // Message length (including self)
		byte(txStatus),               // Transaction status
	}
	_, err := c.clientConn.Write(readyForQuery)
	return err
}

// sendRowDescription sends a RowDescription message to the client
func (c *Connection) sendRowDescription(columnNames []string) error {
	// Calculate number of fields
	numFields := int16(len(columnNames))
	
	// Calculate message length (header + numFields + field descriptions)
	messageLen := 4 + 2 // Length + number of fields
	
	// Precalculate field descriptions to get the total message length
	fieldDescs := make([][]byte, len(columnNames))
	
	for i, name := range columnNames {
		nameBytes := []byte(name)
		
		// Field description: name + tableOID + colAttrNum + dataTypeOID + dataTypeSize + typeModifier + formatCode
		fieldLen := len(nameBytes) + 1 + 4 + 2 + 4 + 2 + 4 + 2
		fieldDesc := make([]byte, fieldLen)
		
		pos := 0
		
		// Column name
		copy(fieldDesc[pos:], nameBytes)
		pos += len(nameBytes)
		fieldDesc[pos] = 0 // Null terminator
		pos++
		
		// Table OID (0 for unnamed)
		fieldDesc[pos] = 0
		fieldDesc[pos+1] = 0
		fieldDesc[pos+2] = 0
		fieldDesc[pos+3] = 0
		pos += 4
		
		// Column attribute number (0 for unnamed)
		fieldDesc[pos] = 0
		fieldDesc[pos+1] = 0
		pos += 2
		
		// Data type OID (25 = TEXT for simplicity)
		fieldDesc[pos] = 0
		fieldDesc[pos+1] = 0
		fieldDesc[pos+2] = 0
		fieldDesc[pos+3] = 25
		pos += 4
		
		// Data type size (-1 for variable length)
		fieldDesc[pos] = 0xFF
		fieldDesc[pos+1] = 0xFF
		pos += 2
		
		// Type modifier (-1 for none)
		fieldDesc[pos] = 0xFF
		fieldDesc[pos+1] = 0xFF
		fieldDesc[pos+2] = 0xFF
		fieldDesc[pos+3] = 0xFF
		pos += 4
		
		// Format code (0 for text)
		fieldDesc[pos] = 0
		fieldDesc[pos+1] = 0
		
		fieldDescs[i] = fieldDesc
		messageLen += fieldLen
	}
	
	// Build the message
	msg := make([]byte, 1+4+messageLen)
	msg[0] = 'T' // RowDescription
	msg[1] = byte((messageLen + 4) >> 24)
	msg[2] = byte((messageLen + 4) >> 16)
	msg[3] = byte((messageLen + 4) >> 8)
	msg[4] = byte(messageLen + 4)
	
	// Number of fields
	msg[5] = byte(numFields >> 8)
	msg[6] = byte(numFields)
	
	// Copy field descriptions
	pos := 7
	for _, fieldDesc := range fieldDescs {
		copy(msg[pos:], fieldDesc)
		pos += len(fieldDesc)
	}
	
	_, err := c.clientConn.Write(msg)
	return err
}

// sendDataRow sends a DataRow message to the client
func (c *Connection) sendDataRow(values [][]byte) error {
	// Calculate message length
	numFields := int16(len(values))
	messageLen := 4 + 2 // Length + number of fields
	
	for _, value := range values {
		if value == nil {
			messageLen += 4 // Length field for NULL (-1)
		} else {
			messageLen += 4 + len(value) // Length field + value bytes
		}
	}
	
	// Build the message
	msg := make([]byte, 1+4+messageLen)
	msg[0] = 'D' // DataRow
	msg[1] = byte((messageLen + 4) >> 24)
	msg[2] = byte((messageLen + 4) >> 16)
	msg[3] = byte((messageLen + 4) >> 8)
	msg[4] = byte(messageLen + 4)
	
	// Number of fields
	msg[5] = byte(numFields >> 8)
	msg[6] = byte(numFields)
	
	// Copy values
	pos := 7
	for _, value := range values {
		if value == nil {
			// NULL value (-1)
			msg[pos] = 0xFF
			msg[pos+1] = 0xFF
			msg[pos+2] = 0xFF
			msg[pos+3] = 0xFF
			pos += 4
		} else {
			// Value length
			valueLen := len(value)
			msg[pos] = byte(valueLen >> 24)
			msg[pos+1] = byte(valueLen >> 16)
			msg[pos+2] = byte(valueLen >> 8)
			msg[pos+3] = byte(valueLen)
			pos += 4
			
			// Value bytes
			copy(msg[pos:], value)
			pos += valueLen
		}
	}
	
	_, err := c.clientConn.Write(msg)
	return err
}

// sendCommandComplete sends a CommandComplete message to the client
func (c *Connection) sendCommandComplete(tag string) error {
	tagBytes := []byte(tag)
	messageLen := 4 + len(tagBytes) + 1
	
	msg := make([]byte, 1+4+messageLen)
	msg[0] = 'C' // CommandComplete
	msg[1] = byte((messageLen + 4) >> 24)
	msg[2] = byte((messageLen + 4) >> 16)
	msg[3] = byte((messageLen + 4) >> 8)
	msg[4] = byte(messageLen + 4)
	
	copy(msg[5:], tagBytes)
	msg[5+len(tagBytes)] = 0 // Null terminator
	
	_, err := c.clientConn.Write(msg)
	return err
}

// handleQuery handles a query message
func (c *Connection) handleQuery(msg *pgproto3.Query) error {
	// Check for transaction commands
	trimmedQuery := strings.TrimSpace(msg.String)
	upperQuery := strings.ToUpper(trimmedQuery)

	// Handle transaction commands
	if upperQuery == "BEGIN" || upperQuery == "START TRANSACTION" {
		return c.handleBeginTransaction()
	} else if upperQuery == "COMMIT" || upperQuery == "END" {
		return c.handleCommitTransaction()
	} else if upperQuery == "ROLLBACK" {
		return c.handleRollbackTransaction()
	} else if strings.HasPrefix(upperQuery, "SAVEPOINT") {
		return c.handleSavepoint(msg.String)
	} else if strings.HasPrefix(upperQuery, "ROLLBACK TO SAVEPOINT") {
		return c.handleRollbackToSavepoint(msg.String)
	} else if strings.HasPrefix(upperQuery, "RELEASE SAVEPOINT") {
		return c.handleReleaseSavepoint(msg.String)
	}

	// Analyze the query
	queryInfo, err := interceptor.AnalyzeQuery(msg.String)
	if err != nil {
		log.Error("Failed to analyze query", "error", err, "query", msg.String)
		// Send error to client
		errMsg := fmt.Sprintf("Error analyzing query: %s", err)
		err := c.sendErrorResponse("ERROR", "42000", errMsg)
		if err != nil {
			return fmt.Errorf("failed to send error response: %w", err)
		}
		
		// Send ReadyForQuery
		err = c.sendReadyForQuery()
		if err != nil {
			return fmt.Errorf("failed to send ReadyForQuery: %w", err)
		}
		return nil
	}

	// Log non-deterministic warnings if enabled
	if c.server.cfg.Features.LogNonDeterministicWarnings && !queryInfo.IsDeterministic {
		log.LogNonDeterministicWarning(c.ctx, queryInfo.Warning, msg.String)
	}

	// For DML queries, we need to capture state
	var preStateData map[string][]types.Row
	var preRootCaptured [32]byte
	var tableSchemas map[string]types.TableSchema
	var needVerification bool

	// Flag for DML queries that need state capture
	needVerification = queryInfo.Type == types.Insert || queryInfo.Type == types.Update || queryInfo.Type == types.Delete

	// Capture pre-state for affected tables (NOTE: This still has the race condition in V1)
	// A future V2 implementation should use WAL/logical decoding to eliminate this race condition
	if needVerification {
		var captureErr error
		
		// Use transaction if in one, otherwise use the direct DB connection
		if c.inTransaction && c.backendTx != nil {
			preStateData, tableSchemas, captureErr = capture.CapturePreStateInTx(c.ctx, queryInfo, c.backendTx)
		} else {
			preStateData, tableSchemas, captureErr = capture.CapturePreState(c.ctx, queryInfo, c.backendDB)
		}
		
		if captureErr != nil {
			log.Error("Failed to capture pre-state", "error", captureErr)
			// Continue anyway, we'll just have less accurate verification
		}

		// Calculate pre-state roots
		tableRoots := make(map[string][32]byte)
		for tableName, rows := range preStateData {
			tableState := &types.TableState{
				Name:   tableName,
				Rows:   rows,
				Schema: tableSchemas[tableName],
			}
			tableRoot, err := commitment.GenerateTableRoot(tableState)
			if err != nil {
				log.Error("Failed to generate table root", "error", err, "table", tableName)
				continue
			}
			tableRoots[tableName] = tableRoot
		}

		// Calculate database pre-state root
		var rootErr error
		preRootCaptured, rootErr = commitment.GenerateDatabaseRoot(tableRoots)
		if rootErr != nil {
			log.Error("Failed to generate database root", "error", rootErr)
		}
	}

	// Create a transaction for non-transaction queries to ensure post-state capture is atomic
	// This eliminates the race condition for post-state capture
	var tx *sql.Tx
	var isLocalTx bool
	var executeErr error
	
	if !c.inTransaction && needVerification {
		// For DML queries not in a transaction, create a local transaction
		tx, executeErr = c.backendDB.BeginTx(c.ctx, nil)
		if executeErr != nil {
			log.Error("Failed to begin local transaction", "error", executeErr)
			err := c.sendErrorResponse("ERROR", "XX000", fmt.Sprintf("Error beginning transaction: %s", executeErr))
			if err != nil {
				return fmt.Errorf("failed to send error response: %w", err)
			}
			return c.sendReadyForQuery()
		}
		isLocalTx = true
	} else if c.inTransaction {
		// Use existing transaction
		tx = c.backendTx
	}

	// Execute the query
	var rows *sql.Rows
	var postStateData map[string][]types.Row
	var postRootClaimed [32]byte
	
	// Execute the query in the appropriate context (transaction or direct DB)
	if needVerification || c.inTransaction {
		// Use transaction (either existing or local)
		rows, executeErr = tx.QueryContext(c.ctx, msg.String)
	} else {
		// Simple query, no transaction needed
		rows, executeErr = c.backendDB.QueryContext(c.ctx, msg.String)
	}
	
	// Handle execution error
	if executeErr != nil {
		// If we created a local transaction, roll it back
		if isLocalTx && tx != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback local transaction", "error", rbErr)
			}
		}
		
		// If in a transaction, mark it as failed
		if c.inTransaction {
			c.transactionMode = 'E' // Error state
		}
		
		// Send error to client
		errMsg := fmt.Sprintf("Error executing query: %s", executeErr)
		err := c.sendErrorResponse("ERROR", "42000", errMsg)
		if err != nil {
			return fmt.Errorf("failed to send error response: %w", err)
		}
		
		return c.sendReadyForQuery()
	}
	
	defer rows.Close()

	// Process the result rows and send to client
	columnNames, err := rows.Columns()
	if err != nil {
		if isLocalTx && tx != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback local transaction", "error", rbErr)
			}
		}
		return fmt.Errorf("failed to get column names: %w", err)
	}

	// Send row description
	err = c.sendRowDescription(columnNames)
	if err != nil {
		if isLocalTx && tx != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback local transaction", "error", rbErr)
			}
		}
		return fmt.Errorf("failed to send row description: %w", err)
	}

	// Send data rows
	rowCount := 0
	for rows.Next() {
		rowCount++
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			if isLocalTx && tx != nil {
				if rbErr := tx.Rollback(); rbErr != nil {
					log.Error("Failed to rollback local transaction", "error", rbErr)
				}
			}
			return fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert values to strings
		stringValues := make([][]byte, len(columnNames))
		for i, v := range values {
			if v == nil {
				stringValues[i] = nil
			} else {
				stringValues[i] = []byte(fmt.Sprintf("%v", v))
			}
		}

		// Send data row
		err = c.sendDataRow(stringValues)
		if err != nil {
			if isLocalTx && tx != nil {
				if rbErr := tx.Rollback(); rbErr != nil {
					log.Error("Failed to rollback local transaction", "error", rbErr)
				}
			}
			return fmt.Errorf("failed to send data row: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		if isLocalTx && tx != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback local transaction", "error", rbErr)
			}
		}
		return fmt.Errorf("error iterating rows: %w", err)
	}

	// For DML queries, capture post-state BEFORE committing the transaction
	// This ensures the post-state is captured atomically with the query execution
	if needVerification {
		// Capture post-state within the transaction (either user's or our local one)
		var captureErr error
		
		// Always use transaction here - it's either user's transaction or our local one
		postStateData, _, captureErr = capture.CapturePostStateInTx(c.ctx, queryInfo, tx)
		
		if captureErr != nil {
			log.Error("Failed to capture post-state", "error", captureErr)
			// Continue anyway, we'll just have less accurate verification
		}

		// Calculate post-state roots
		tableRoots := make(map[string][32]byte)
		for tableName, rows := range postStateData {
			tableState := &types.TableState{
				Name:   tableName,
				Rows:   rows,
				Schema: tableSchemas[tableName],
			}
			tableRoot, err := commitment.GenerateTableRoot(tableState)
			if err != nil {
				log.Error("Failed to generate table root", "error", err, "table", tableName)
				continue
			}
			tableRoots[tableName] = tableRoot
		}

		// Calculate database post-state root
		var rootErr error
		postRootClaimed, rootErr = commitment.GenerateDatabaseRoot(tableRoots)
		if rootErr != nil {
			log.Error("Failed to generate database root", "error", rootErr)
		}

		// Get transaction ID
		txID := c.server.GetNextTxID()

		// Log state commitment record
		record := types.StateCommitmentRecord{
			TxID:            txID,
			QuerySequence:   []string{msg.String},
			PreRootCaptured: preRootCaptured,
			PostRootClaimed: postRootClaimed,
			PreStateData:    preStateData,
			Timestamp:       time.Now(),
		}
		log.LogStateCommitment(c.ctx, record)

		// Only send verification job if not in a transaction
		// For transactions, we'll accumulate queries and send at COMMIT time
		if !c.inTransaction && c.server.cfg.Features.EnableVerification {
			job := types.VerificationJob{
				TxID:            txID,
				QuerySequence:   []string{msg.String},
				PreStateData:    preStateData,
				PreRootCaptured: preRootCaptured,
				PostRootClaimed: postRootClaimed,
				TableSchemas:    tableSchemas,
			}
			c.server.SendVerificationJob(job)
		}
	}

	// If we created a local transaction, commit it now after capturing post-state
	if isLocalTx && tx != nil {
		if commitErr := tx.Commit(); commitErr != nil {
			log.Error("Failed to commit local transaction", "error", commitErr)
			err := c.sendErrorResponse("ERROR", "XX000", fmt.Sprintf("Error committing transaction: %s", commitErr))
			if err != nil {
				return fmt.Errorf("failed to send error response: %w", err)
			}
			return c.sendReadyForQuery()
		}
	}

	// Send command complete
	cmdTag := "SELECT"
	switch queryInfo.Type {
	case types.Select:
		cmdTag = fmt.Sprintf("SELECT %d", rowCount)
	case types.Insert:
		cmdTag = "INSERT 0 1" // Simplified, should be actual count
	case types.Update:
		cmdTag = "UPDATE 1" // Simplified, should be actual count
	case types.Delete:
		cmdTag = "DELETE 1" // Simplified, should be actual count
	}

	err = c.sendCommandComplete(cmdTag)
	if err != nil {
		return fmt.Errorf("failed to send command complete: %w", err)
	}

	// Send ready for query with the correct transaction status
	err = c.sendReadyForQuery()
	if err != nil {
		return fmt.Errorf("failed to send ready for query: %w", err)
	}

	return nil
}

// handleBeginTransaction starts a new transaction
func (c *Connection) handleBeginTransaction() error {
	// Check if already in a transaction
	if c.inTransaction {
		// Already in a transaction, send warning
		err := c.sendErrorResponse("WARNING", "25001", "Already in a transaction block")
		if err != nil {
			return fmt.Errorf("failed to send warning: %w", err)
		}
		
		// Send ready for query with transaction status
		err = c.sendReadyForQuery()
		if err != nil {
			return fmt.Errorf("failed to send ReadyForQuery: %w", err)
		}
		
		return nil
	}

	// Start a new transaction
	tx, err := c.backendDB.BeginTx(c.ctx, nil)
	if err != nil {
		log.Error("Failed to begin transaction", "error", err)
		
		// Send error to client
		errMsg := fmt.Sprintf("Error beginning transaction: %s", err)
		err := c.sendErrorResponse("ERROR", "XX000", errMsg)
		if err != nil {
			return fmt.Errorf("failed to send error response: %w", err)
		}
		
		// Send ready for query
		err = c.sendReadyForQuery()
		if err != nil {
			return fmt.Errorf("failed to send ReadyForQuery: %w", err)
		}
		
		return nil
	}

	// Set transaction state
	c.backendTx = tx
	c.inTransaction = true
	c.transactionMode = 'T' // In transaction
	c.queryCount = 0
	
	// Update metrics
	c.server.metrics.TransactionStarted()

	// Send command complete
	err = c.sendCommandComplete("BEGIN")
	if err != nil {
		return fmt.Errorf("failed to send command complete: %w", err)
	}

	// Send ready for query with transaction status
	err = c.sendReadyForQuery()
	if err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}

	return nil
}

// handleCommitTransaction commits the current transaction
func (c *Connection) handleCommitTransaction() error {
	// Check if in a transaction
	if !c.inTransaction || c.backendTx == nil {
		// Not in a transaction, send warning
		err := c.sendErrorResponse("WARNING", "25P01", "No transaction is in progress")
		if err != nil {
			return fmt.Errorf("failed to send warning: %w", err)
		}
		
		// Send ready for query
		err = c.sendReadyForQuery()
		if err != nil {
			return fmt.Errorf("failed to send ReadyForQuery: %w", err)
		}
		
		return nil
	}

	// Commit the transaction
	err := c.backendTx.Commit()
	if err != nil {
		log.Error("Failed to commit transaction", "error", err)
		
		// Send error to client
		errMsg := fmt.Sprintf("Error committing transaction: %s", err)
		err := c.sendErrorResponse("ERROR", "XX000", errMsg)
		if err != nil {
			return fmt.Errorf("failed to send error response: %w", err)
		}
		
		// Reset transaction state
		c.backendTx = nil
		c.inTransaction = false
		c.transactionMode = 'I' // Idle
		
		// Send ready for query
		err = c.sendReadyForQuery()
		if err != nil {
			return fmt.Errorf("failed to send ReadyForQuery: %w", err)
		}
		
		return nil
	}

	// Reset transaction state
	c.backendTx = nil
	c.inTransaction = false
	c.transactionMode = 'I' // Idle

	// Update metrics
	c.server.metrics.TransactionCommitted()

	// Send command complete
	err = c.sendCommandComplete("COMMIT")
	if err != nil {
		return fmt.Errorf("failed to send command complete: %w", err)
	}

	// Send ready for query
	err = c.sendReadyForQuery()
	if err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}

	return nil
}

// handleRollbackTransaction rolls back the current transaction
func (c *Connection) handleRollbackTransaction() error {
	// Check if in a transaction
	if !c.inTransaction || c.backendTx == nil {
		// Not in a transaction, send warning
		err := c.sendErrorResponse("WARNING", "25P01", "No transaction is in progress")
		if err != nil {
			return fmt.Errorf("failed to send warning: %w", err)
		}
		
		// Send ready for query
		err = c.sendReadyForQuery()
		if err != nil {
			return fmt.Errorf("failed to send ReadyForQuery: %w", err)
		}
		
		return nil
	}

	// Rollback the transaction
	err := c.backendTx.Rollback()
	if err != nil {
		log.Error("Failed to rollback transaction", "error", err)
		
		// Send error to client
		errMsg := fmt.Sprintf("Error rolling back transaction: %s", err)
		err := c.sendErrorResponse("ERROR", "XX000", errMsg)
		if err != nil {
			return fmt.Errorf("failed to send error response: %w", err)
		}
		
		// Reset transaction state anyway
		c.backendTx = nil
		c.inTransaction = false
		c.transactionMode = 'I' // Idle
		
		// Send ready for query
		err = c.sendReadyForQuery()
		if err != nil {
			return fmt.Errorf("failed to send ReadyForQuery: %w", err)
		}
		
		return nil
	}

	// Reset transaction state
	c.backendTx = nil
	c.inTransaction = false
	c.transactionMode = 'I' // Idle

	// Update metrics
	c.server.metrics.TransactionRolledBack()

	// Send command complete
	err = c.sendCommandComplete("ROLLBACK")
	if err != nil {
		return fmt.Errorf("failed to send command complete: %w", err)
	}

	// Send ready for query
	err = c.sendReadyForQuery()
	if err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}

	return nil
}

// handleSavepoint handles a SAVEPOINT command
func (c *Connection) handleSavepoint(query string) error {
	// Check if in a transaction
	if !c.inTransaction || c.backendTx == nil {
		// Automatically start a transaction for the savepoint
		err := c.handleBeginTransaction()
		if err != nil {
			return err
		}
	}

	// Execute the savepoint command
	_, err := c.backendTx.ExecContext(c.ctx, query)
	if err != nil {
		log.Error("Failed to create savepoint", "error", err)
		
		// Send error to client
		errMsg := fmt.Sprintf("Error creating savepoint: %s", err)
		err := c.sendErrorResponse("ERROR", "XX000", errMsg)
		if err != nil {
			return fmt.Errorf("failed to send error response: %w", err)
		}
		
		// Send ready for query with transaction status
		err = c.sendReadyForQuery()
		if err != nil {
			return fmt.Errorf("failed to send ReadyForQuery: %w", err)
		}
		
		return nil
	}

	// Send command complete
	err = c.sendCommandComplete("SAVEPOINT")
	if err != nil {
		return fmt.Errorf("failed to send command complete: %w", err)
	}

	// Send ready for query with transaction status
	err = c.sendReadyForQuery()
	if err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}

	return nil
}

// handleRollbackToSavepoint handles a ROLLBACK TO SAVEPOINT command
func (c *Connection) handleRollbackToSavepoint(query string) error {
	// Check if in a transaction
	if !c.inTransaction || c.backendTx == nil {
		// Not in a transaction, send error
		err := c.sendErrorResponse("ERROR", "25P01", "No transaction is in progress")
		if err != nil {
			return fmt.Errorf("failed to send error: %w", err)
		}
		
		// Send ready for query
		err = c.sendReadyForQuery()
		if err != nil {
			return fmt.Errorf("failed to send ReadyForQuery: %w", err)
		}
		
		return nil
	}

	// Execute the rollback to savepoint command
	_, err := c.backendTx.ExecContext(c.ctx, query)
	if err != nil {
		log.Error("Failed to rollback to savepoint", "error", err)
		
		// Send error to client
		errMsg := fmt.Sprintf("Error rolling back to savepoint: %s", err)
		err := c.sendErrorResponse("ERROR", "XX000", errMsg)
		if err != nil {
			return fmt.Errorf("failed to send error response: %w", err)
		}
		
		// Send ready for query with transaction status
		err = c.sendReadyForQuery()
		if err != nil {
			return fmt.Errorf("failed to send ReadyForQuery: %w", err)
		}
		
		return nil
	}

	// Send command complete
	err = c.sendCommandComplete("ROLLBACK")
	if err != nil {
		return fmt.Errorf("failed to send command complete: %w", err)
	}

	// Send ready for query with transaction status
	err = c.sendReadyForQuery()
	if err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}

	return nil
}

// handleReleaseSavepoint handles a RELEASE SAVEPOINT command
func (c *Connection) handleReleaseSavepoint(query string) error {
	// Check if in a transaction
	if !c.inTransaction || c.backendTx == nil {
		// Not in a transaction, send error
		err := c.sendErrorResponse("ERROR", "25P01", "No transaction is in progress")
		if err != nil {
			return fmt.Errorf("failed to send error: %w", err)
		}
		
		// Send ready for query
		err = c.sendReadyForQuery()
		if err != nil {
			return fmt.Errorf("failed to send ReadyForQuery: %w", err)
		}
		
		return nil
	}

	// Execute the release savepoint command
	_, err := c.backendTx.ExecContext(c.ctx, query)
	if err != nil {
		log.Error("Failed to release savepoint", "error", err)
		
		// Send error to client
		errMsg := fmt.Sprintf("Error releasing savepoint: %s", err)
		err := c.sendErrorResponse("ERROR", "XX000", errMsg)
		if err != nil {
			return fmt.Errorf("failed to send error response: %w", err)
		}
		
		// Send ready for query with transaction status
		err = c.sendReadyForQuery()
		if err != nil {
			return fmt.Errorf("failed to send ReadyForQuery: %w", err)
		}
		
		return nil
	}

	// Send command complete
	err = c.sendCommandComplete("RELEASE")
	if err != nil {
		return fmt.Errorf("failed to send command complete: %w", err)
	}

	// Send ready for query with transaction status
	err = c.sendReadyForQuery()
	if err != nil {
		return fmt.Errorf("failed to send ReadyForQuery: %w", err)
	}

	return nil
}

// Close closes the connection
func (c *Connection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	c.cancel()

	// Close connections
	if c.clientConn != nil {
		c.clientConn.Close()
	}
	if c.backendDB != nil {
		c.backendDB.Close()
	}
	
	// Update metrics
	c.server.metrics.ConnectionClosed()

	log.Info("Connection closed", "id", c.id)
	return nil
}