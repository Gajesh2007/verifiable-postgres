package replay

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // Import pgx driver

	"github.com/verifiable-postgres/proxy/pkg/capture"
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

	// Connect to verification database
	db, err := sql.Open("pgx", e.cfg.VerificationDB.DSN())
	if err != nil {
		result.Error = fmt.Sprintf("Failed to connect to verification DB: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}
	defer db.Close()

	// Begin transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		result.Error = fmt.Sprintf("Failed to begin transaction: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}

	// Ensure transaction is rolled back
	defer tx.Rollback()

	// Setup verification database with pre-state
	if err := SetupVerificationDB(ctx, tx, job.TableSchemas, job.PreStateData); err != nil {
		result.Error = fmt.Sprintf("Failed to setup verification DB: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}

	// Set deterministic session parameters
	if err := SetDeterministicSessionParams(ctx, tx); err != nil {
		result.Error = fmt.Sprintf("Failed to set deterministic session parameters: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}

	// Execute queries deterministically
	if err := ExecuteQueriesDeterministically(ctx, tx, job.QuerySequence); err != nil {
		result.Error = fmt.Sprintf("Failed to execute queries: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}

	// Commit transaction to apply changes
	if err := tx.Commit(); err != nil {
		result.Error = fmt.Sprintf("Failed to commit transaction: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}

	// Start a new transaction for post-state capture
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		result.Error = fmt.Sprintf("Failed to begin post-state transaction: %v", err)
		e.metrics.VerificationCompleted(false, time.Since(startTime))
		return result
	}
	defer tx.Rollback()

	// Convert job to query info for post-state capture
	queryInfo := &types.QueryInfo{
		SQL:            job.QuerySequence[0], // Simplified for V1, should handle multiple queries
		AffectedTables: make([]string, 0, len(job.TableSchemas)),
	}

	// Add affected tables
	for tableName := range job.TableSchemas {
		queryInfo.AffectedTables = append(queryInfo.AffectedTables, tableName)
	}

	// Capture post-state
	postStateData, _, err := capture.CapturePostState(ctx, queryInfo, db)
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
	
	// Record metrics for completed verification
	e.metrics.VerificationCompleted(success, time.Since(startTime))

	return result
}