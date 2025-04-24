package types

import (
	"time"
)

// QueryType represents the different types of SQL statements
type QueryType int

const (
	Unknown QueryType = iota
	Select
	Insert
	Update
	Delete
	Other
)

// QueryInfo contains information about a SQL query after analysis
type QueryInfo struct {
	SQL             string     // Original SQL query
	Type            QueryType  // Type of the query
	AffectedTables  []string   // Tables affected by the query
	IsDeterministic bool       // Whether the query is deterministic
	Warning         string     // Warning message for non-deterministic queries
	Timestamp       time.Time  // When the query was processed
}

// Value represents a typed database value
type Value interface{}

// RowID represents a unique identifier for a row in a table
type RowID string

// Row represents a database row with its unique identifier
type Row struct {
	ID     RowID             // Unique row identifier
	Values map[string]Value  // Column name to value mapping
}

// TableState represents the state of a table at a point in time
type TableState struct {
	Name    string  // Table name
	Rows    []Row   // Rows in the table
	Schema  TableSchema  // Schema information
}

// TableSchema represents the structure of a table
type TableSchema struct {
	Name    string             // Table name
	Columns []ColumnDefinition // Column definitions
	PKColumns []string         // Primary key column names
}

// ColumnDefinition represents the definition of a column in a table
type ColumnDefinition struct {
	Name     string  // Column name
	Type     string  // Data type
	Nullable bool    // Whether the column can be null
}

// StateCommitmentRecord represents a record of a state transition
type StateCommitmentRecord struct {
	TxID            uint64    // Transaction ID
	QuerySequence   []string  // Sequence of queries in the transaction
	PreRootCaptured [32]byte  // Merkle root of pre-state
	PostRootClaimed [32]byte  // Claimed Merkle root of post-state
	PreStateData    map[string][]Row  // Pre-state data for affected tables
	Timestamp       time.Time  // When the record was created
}

// VerificationJob represents a job for the replay engine
type VerificationJob struct {
	TxID            uint64    // Transaction ID
	QuerySequence   []string  // Sequence of queries to replay
	PreStateData    map[string][]Row  // Pre-state data to restore
	PreRootCaptured [32]byte  // Merkle root of pre-state
	PostRootClaimed [32]byte  // Claimed Merkle root of post-state
	TableSchemas    map[string]TableSchema  // Schema information for affected tables
}

// VerificationResult represents the result of a verification job
type VerificationResult struct {
	TxID              uint64    // Transaction ID
	Success           bool      // Whether verification was successful
	PreRootCaptured   [32]byte  // Merkle root of pre-state
	PostRootClaimed   [32]byte  // Claimed Merkle root from proxy
	PostRootCalculated [32]byte // Calculated Merkle root from replay
	Mismatches        []string  // Description of any mismatches found
	Error             string    // Error message if verification failed
	Timestamp         time.Time // When the verification was completed
}