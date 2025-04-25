package replay

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // Import pgx driver
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/verifiable-postgres/proxy/pkg/commitment"
	"github.com/verifiable-postgres/proxy/pkg/config"
	"github.com/verifiable-postgres/proxy/pkg/log"
	"github.com/verifiable-postgres/proxy/pkg/metrics"
	"github.com/verifiable-postgres/proxy/pkg/types"
)

// Engine represents the replay engine
type Engine struct {
	cfg        *config.Config
	jobQueue   chan types.VerificationJob
	workers    []context.CancelFunc
	workerWg   sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	metrics    *metrics.Metrics
	dbPool     *pgxpool.Pool  // Connection pool for verification database
}

// NewEngine creates a new replay engine
func NewEngine(cfg *config.Config, jobQueue chan types.VerificationJob, metrics *metrics.Metrics) (*Engine, error) {
	// Create context with cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Create engine instance
	engine := &Engine{
		cfg:      cfg,
		jobQueue: jobQueue,
		ctx:      ctx,
		cancel:   cancel,
		metrics:  metrics,
	}

	// Initialize connection pool for verification database
	poolConfig, err := pgxpool.ParseConfig(cfg.VerificationDB.DSN())
	if err != nil {
		cancel() // Clean up context
		return nil, fmt.Errorf("failed to parse DB pool config: %w", err)
	}

	// Set pool configuration options
	poolConfig.MaxConns = int32(cfg.ReplayEngine.WorkerPoolSize * 2) // Allow some extra connections for scalability
	poolConfig.MinConns = int32(cfg.ReplayEngine.WorkerPoolSize)
	
	// Create the pool
	dbPool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		cancel() // Clean up context
		return nil, fmt.Errorf("failed to create DB connection pool: %w", err)
	}
	
	engine.dbPool = dbPool

	return engine, nil
}

// Start starts the replay engine
func (e *Engine) Start() error {
	// Create worker pool
	e.workers = make([]context.CancelFunc, e.cfg.ReplayEngine.WorkerPoolSize)

	// Start workers
	for i := 0; i < e.cfg.ReplayEngine.WorkerPoolSize; i++ {
		workerCtx, workerCancel := context.WithCancel(e.ctx)
		e.workers[i] = workerCancel

		// Start worker
		e.workerWg.Add(1)
		go e.worker(workerCtx, i)
	}

	log.Info("Replay engine started", "workers", e.cfg.ReplayEngine.WorkerPoolSize)
	return nil
}

// Stop stops the replay engine
func (e *Engine) Stop() {
	// Cancel worker contexts
	for _, cancel := range e.workers {
		cancel()
	}

	// Wait for workers to finish
	e.workerWg.Wait()

	// Close the DB pool
	if e.dbPool != nil {
		e.dbPool.Close()
	}

	// Cancel main context
	e.cancel()

	log.Info("Replay engine stopped")
}

// worker is a verification worker
func (e *Engine) worker(ctx context.Context, id int) {
	defer e.workerWg.Done()

	log.Info("Worker started", "id", id)

	// Process jobs until context is cancelled
	for {
		select {
		case <-ctx.Done():
			log.Info("Worker stopped", "id", id)
			return
		case job, ok := <-e.jobQueue:
			if !ok {
				// Channel is closed
				log.Info("Job queue closed, worker stopping", "id", id)
				return
			}

			// Process the job
			log.Info("Processing verification job", "id", id, "txID", job.TxID)
			result := e.processJob(ctx, job)

			// Log the result
			log.LogVerificationResult(ctx, result)
		}
	}
}

// processJob processes a verification job
func (e *Engine) processJob(ctx context.Context, job types.VerificationJob) types.VerificationResult {
	// Record metrics for verification job
	startTime := time.Now()
	e.metrics.VerificationStarted()
	
	// Create result with basic info
	result := types.VerificationResult{
		TxID:            job.TxID,
		Success:         false,
		PreRootCaptured: job.PreRootCaptured,
		PostRootClaimed: job.PostRootClaimed,
		Timestamp:       time.Now(),
	}

	// Acquire a connection from the pool
	conn, err := e.dbPool.Acquire(ctx)
	if err != nil {
		result.Error = fmt.Sprintf("Failed to acquire DB connection: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}
	// Release the connection back to the pool when done
	defer conn.Release()

	// Begin transaction
	tx, err := conn.Begin(ctx)
	if err != nil {
		result.Error = fmt.Sprintf("Failed to begin transaction: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}

	// Always ensure transaction is rolled back when we're done
	// to clean the state for the next verification
	defer func() {
		if tx != nil {
			tx.Rollback(ctx)
		}
	}()

	// Setup verification database with pre-state
	if err := SetupVerificationDBWithPgx(ctx, tx, job.TableSchemas, job.PreStateData); err != nil {
		result.Error = fmt.Sprintf("Failed to setup verification DB: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}

	// Set deterministic session parameters
	if err := SetDeterministicSessionParamsWithPgx(ctx, tx); err != nil {
		result.Error = fmt.Sprintf("Failed to set deterministic session parameters: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}

	// Execute queries deterministically
	if err := ExecuteQueriesDeterministicallyWithPgx(ctx, tx, job.QuerySequence); err != nil {
		result.Error = fmt.Sprintf("Failed to execute queries: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}

	// Convert job to query info for post-state capture
	queryInfo := &types.QueryInfo{
		SQL:            job.QuerySequence[0], // Simplified for V1, should handle multiple queries
		AffectedTables: make([]string, 0, len(job.TableSchemas)),
	}

	// Add affected tables
	for tableName := range job.TableSchemas {
		queryInfo.AffectedTables = append(queryInfo.AffectedTables, tableName)
	}

	// Capture post-state BEFORE committing the transaction
	// This ensures we capture the exact state produced by the transaction
	postStateData, err := CapturePostStateWithPgx(ctx, queryInfo, tx, job.TableSchemas)
	if err != nil {
		result.Error = fmt.Sprintf("Failed to capture post-state: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}

	// Calculate post-state roots
	tableRoots := make(map[string][32]byte)
	for tableName, rows := range postStateData {
		tableState := &types.TableState{
			Name:   tableName,
			Rows:   rows,
			Schema: job.TableSchemas[tableName],
		}
		tableRoot, err := commitment.GenerateTableRoot(tableState)
		if err != nil {
			result.Error = fmt.Sprintf("Failed to generate table root: %v", err)
			e.metrics.VerificationCompleted(false, time.Since(startTime))
			return result
		}
		tableRoots[tableName] = tableRoot
	}

	// Calculate database post-state root
	postRootCalculated, err := commitment.GenerateDatabaseRoot(tableRoots)
	if err != nil {
		result.Error = fmt.Sprintf("Failed to generate database root: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}

	// Save calculated root
	result.PostRootCalculated = postRootCalculated

	// Compare roots
	success := postRootCalculated == job.PostRootClaimed
	result.Success = success
	
	if !success {
		result.Mismatches = []string{
			fmt.Sprintf("Post-state root mismatch: claimed %x, calculated %x", 
				job.PostRootClaimed, postRootCalculated),
		}
	}
	
	// Transaction will be automatically rolled back by the defer function
	// We don't want to commit it because we want a clean state for the next verification
	
	// Record metrics for completed verification
	e.metrics.VerificationCompleted(success, time.Since(startTime))

	return result
}

// CapturePostStateWithPgx captures the post-state using pgx transaction
func CapturePostStateWithPgx(ctx context.Context, queryInfo *types.QueryInfo, tx pgx.Tx, schemas map[string]types.TableSchema) (map[string][]types.Row, error) {
	if queryInfo == nil || len(queryInfo.AffectedTables) == 0 {
		return nil, fmt.Errorf("no tables affected")
	}

	// Capture state for each affected table
	postState := make(map[string][]types.Row)
	for _, tableName := range queryInfo.AffectedTables {
		schema, ok := schemas[tableName]
		if !ok {
			log.Warn("Schema not found for table", "table", tableName)
			continue
		}

		// Capture all rows from the table for simplicity in V1
		rows, err := captureTableStateWithPgx(ctx, tx, schema)
		if err != nil {
			log.Error("Failed to capture table state", "table", tableName, "error", err)
			continue
		}

		postState[tableName] = rows
	}

	return postState, nil
}

// captureTableStateWithPgx captures the current state of a table using pgx transaction
func captureTableStateWithPgx(ctx context.Context, tx pgx.Tx, schema types.TableSchema) ([]types.Row, error) {
	// Build query to select all columns
	columnNames := make([]string, len(schema.Columns))
	for i, col := range schema.Columns {
		columnNames[i] = col.Name
	}
	
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columnNames, ", "), schema.Name)
	
	// Execute query
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query table state: %w", err)
	}
	defer rows.Close()

	// Process rows
	var result []types.Row
	for rows.Next() {
		// Create a slice to hold the values
		values := make([]interface{}, len(columnNames))
		scans := make([]interface{}, len(columnNames))
		
		// Create pointers to scan into
		for i := range values {
			scans[i] = &values[i]
		}
		
		// Scan row values
		if err := rows.Scan(scans...); err != nil {
			return nil, fmt.Errorf("failed to scan row values: %w", err)
		}
		
		// Create row with values
		row := types.Row{
			Values: make(map[string]types.Value),
		}
		
		// Generate row ID using primary key values
		if len(schema.PKColumns) > 0 {
			pkValues := make(map[string]types.Value)
			for _, pkCol := range schema.PKColumns {
				for i, colName := range columnNames {
					if colName == pkCol {
						pkValues[pkCol] = values[i]
						break
					}
				}
			}
			
			// Use standardized row ID generation from commitment package
			row.ID = commitment.GenerateRowID(schema.Name, pkValues)
		} else {
			// Fallback if no primary key
			row.ID = types.RowID(fmt.Sprintf("%s:fallback:%d", schema.Name, len(result)))
		}
		
		// Map column names to values
		for i, colName := range columnNames {
			row.Values[colName] = values[i]
		}
		
		result = append(result, row)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}
	
	return result, nil
}