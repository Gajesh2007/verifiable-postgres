package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // Import pgx driver
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/verifiable-postgres/proxy/pkg/config"
	"github.com/verifiable-postgres/proxy/pkg/log"
	"github.com/verifiable-postgres/proxy/pkg/proxy"
	"github.com/verifiable-postgres/proxy/pkg/types"
)

// TestIntegration runs integration tests with the proxy and databases
func TestIntegration(t *testing.T) {
	// Skip if not explicitly enabled
	if os.Getenv("ENABLE_INTEGRATION_TESTS") != "1" {
		t.Skip("Integration tests disabled")
	}

	// Create test configuration
	cfg := config.DefaultConfig()
	cfg.ListenAddr = "localhost:5435" // Use a different port for testing
	cfg.BackendDB.Port = 5433          // Backend DB port
	cfg.VerificationDB.Port = 5434     // Verification DB port
	cfg.Log.Level = "debug"

	// Create a log buffer to capture logs
	logBuffer := &bytes.Buffer{}

	// Initialize logs to write to our buffer
	log.SetupWithWriter(&cfg.Log, logBuffer)

	// Start the proxy server
	server, err := proxy.NewServer(cfg)
	require.NoError(t, err, "Failed to create server")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the server in a goroutine
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Start()
		if ctx.Err() == nil { // If context wasn't canceled
			assert.NoError(t, err, "Server error")
		}
	}()

	// Wait for the server to start
	time.Sleep(2 * time.Second)

	// Run the tests
	t.Run("TestSimpleQueries", func(t *testing.T) { testSimpleQueries(t, cfg) })
	t.Run("TestTransactions", func(t *testing.T) { testTransactions(t, cfg) })
	t.Run("TestNonDeterministicWarnings", func(t *testing.T) { testNonDeterministicWarnings(t, cfg, logBuffer) })
	t.Run("TestVerification", func(t *testing.T) { testVerification(t, cfg, logBuffer) })
	t.Run("TestFailedVerification", func(t *testing.T) { testFailedVerification(t, cfg, logBuffer) })

	// Stop the server
	cancel()
	server.Stop()
	wg.Wait()
}

// setupTestDatabase sets up the test database
func setupTestDatabase(t *testing.T, cfg *config.Config) *pgxpool.Pool {
	// Connect to the proxy
	connStr := "postgres://postgres:postgres@" + cfg.ListenAddr + "/postgres"
	pool, err := pgxpool.New(context.Background(), connStr)
	require.NoError(t, err, "Failed to connect to proxy")

	// Create test tables
	_, err = pool.Exec(context.Background(), `
		DROP TABLE IF EXISTS test_users;
		CREATE TABLE test_users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	require.NoError(t, err, "Failed to create test table")

	return pool
}

// testSimpleQueries tests basic CRUD operations
func testSimpleQueries(t *testing.T, cfg *config.Config) {
	pool := setupTestDatabase(t, cfg)
	defer pool.Close()

	ctx := context.Background()

	// Test INSERT
	r, err := pool.Exec(ctx, `INSERT INTO test_users (name, email) VALUES ('Alice', 'alice@example.com');
		                   INSERT INTO test_users (name, email) VALUES ('Bob', 'bob@example.com');`)
	require.NoError(t, err, "Failed to insert test data")
	assert.Equal(t, int64(1), r.RowsAffected(), "Expected 1 row affected")

	// Test SELECT
	rows, err := pool.Query(ctx, "SELECT id, name, email FROM test_users ORDER BY id")
	require.NoError(t, err, "Failed to select test data")

	users := make([]struct {
		ID    int
		Name  string
		Email string
	}, 0, 2)

	for rows.Next() {
		var user struct {
			ID    int
			Name  string
			Email string
		}
		err := rows.Scan(&user.ID, &user.Name, &user.Email)
		require.NoError(t, err, "Failed to scan row")
		users = append(users, user)
	}
	rows.Close()

	require.Len(t, users, 2, "Expected 2 users")
	assert.Equal(t, "Alice", users[0].Name, "Expected first user to be Alice")
	assert.Equal(t, "Bob", users[1].Name, "Expected second user to be Bob")

	// Test UPDATE
	r, err = pool.Exec(ctx, "UPDATE test_users SET name = 'Alice Smith' WHERE name = 'Alice'")
	require.NoError(t, err, "Failed to update test data")
	assert.Equal(t, int64(1), r.RowsAffected(), "Expected 1 row affected")

	// Verify UPDATE
	var name string
	err = pool.QueryRow(ctx, "SELECT name FROM test_users WHERE id = 1").Scan(&name)
	require.NoError(t, err, "Failed to select updated name")
	assert.Equal(t, "Alice Smith", name, "Expected updated name to be Alice Smith")

	// Test DELETE
	r, err = pool.Exec(ctx, "DELETE FROM test_users WHERE name = 'Bob'")
	require.NoError(t, err, "Failed to delete test data")
	assert.Equal(t, int64(1), r.RowsAffected(), "Expected 1 row affected")

	// Verify DELETE
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_users").Scan(&count)
	require.NoError(t, err, "Failed to count users")
	assert.Equal(t, 1, count, "Expected 1 user remaining")
}

// testTransactions tests transaction support
func testTransactions(t *testing.T, cfg *config.Config) {
	pool := setupTestDatabase(t, cfg)
	defer pool.Close()

	ctx := context.Background()

	// Start a transaction
	tx, err := pool.Begin(ctx)
	require.NoError(t, err, "Failed to begin transaction")

	// Insert data in transaction
	_, err = tx.Exec(ctx, "INSERT INTO test_users (name, email) VALUES ('Carol', 'carol@example.com')")
	require.NoError(t, err, "Failed to insert in transaction")

	// Verify data exists within transaction
	var count int
	err = tx.QueryRow(ctx, "SELECT COUNT(*) FROM test_users WHERE name = 'Carol'").Scan(&count)
	require.NoError(t, err, "Failed to query in transaction")
	assert.Equal(t, 1, count, "Expected 1 matching user in transaction")

	// Verify data doesn't exist outside transaction (isolation)
	var outerCount int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_users WHERE name = 'Carol'").Scan(&outerCount)
	require.NoError(t, err, "Failed to query outside transaction")
	assert.Equal(t, 0, outerCount, "Expected 0 matching users outside transaction")

	// Commit transaction
	err = tx.Commit(ctx)
	require.NoError(t, err, "Failed to commit transaction")

	// Verify data exists after commit
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_users WHERE name = 'Carol'").Scan(&outerCount)
	require.NoError(t, err, "Failed to query after commit")
	assert.Equal(t, 1, outerCount, "Expected 1 matching user after commit")

	// Test rollback
	tx, err = pool.Begin(ctx)
	require.NoError(t, err, "Failed to begin transaction")

	// Insert data in transaction
	_, err = tx.Exec(ctx, "INSERT INTO test_users (name, email) VALUES ('Dave', 'dave@example.com')")
	require.NoError(t, err, "Failed to insert in transaction")

	// Rollback transaction
	err = tx.Rollback(ctx)
	require.NoError(t, err, "Failed to rollback transaction")

	// Verify data doesn't exist after rollback
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_users WHERE name = 'Dave'").Scan(&count)
	require.NoError(t, err, "Failed to query after rollback")
	assert.Equal(t, 0, count, "Expected 0 matching users after rollback")

	// Test savepoints
	tx, err = pool.Begin(ctx)
	require.NoError(t, err, "Failed to begin transaction")

	// Create savepoint
	_, err = tx.Exec(ctx, "SAVEPOINT sp1")
	require.NoError(t, err, "Failed to create savepoint")

	// Insert data after savepoint
	_, err = tx.Exec(ctx, "INSERT INTO test_users (name, email) VALUES ('Eve', 'eve@example.com')")
	require.NoError(t, err, "Failed to insert after savepoint")

	// Rollback to savepoint
	_, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT sp1")
	require.NoError(t, err, "Failed to rollback to savepoint")

	// Verify data doesn't exist after rollback to savepoint
	err = tx.QueryRow(ctx, "SELECT COUNT(*) FROM test_users WHERE name = 'Eve'").Scan(&count)
	require.NoError(t, err, "Failed to query after rollback to savepoint")
	assert.Equal(t, 0, count, "Expected 0 matching users after rollback to savepoint")

	// Insert different data and commit
	_, err = tx.Exec(ctx, "INSERT INTO test_users (name, email) VALUES ('Frank', 'frank@example.com')")
	require.NoError(t, err, "Failed to insert after rollback to savepoint")
	err = tx.Commit(ctx)
	require.NoError(t, err, "Failed to commit transaction")

	// Verify final state
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_users WHERE name = 'Frank'").Scan(&count)
	require.NoError(t, err, "Failed to query final state")
	assert.Equal(t, 1, count, "Expected 1 user named Frank")

	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_users WHERE name = 'Eve'").Scan(&count)
	require.NoError(t, err, "Failed to query final state")
	assert.Equal(t, 0, count, "Expected 0 users named Eve")
}

// testNonDeterministicWarnings tests detection of non-deterministic functions
func testNonDeterministicWarnings(t *testing.T, cfg *config.Config, logBuffer *bytes.Buffer) {
	pool := setupTestDatabase(t, cfg)
	defer pool.Close()

	ctx := context.Background()

	// Reset the log buffer
	logBuffer.Reset()

	// Run a query with non-deterministic function
	_, err := pool.Exec(ctx, "SELECT id, name, now() FROM test_users")
	require.NoError(t, err, "Failed to execute query")

	// Give some time for logging
	time.Sleep(500 * time.Millisecond)

	// Check logs for non-deterministic warning
	logOutput := logBuffer.String()
	assert.Contains(t, logOutput, "non-deterministic", "Expected non-deterministic function warning")
	assert.Contains(t, logOutput, "now()", "Expected warning for now() function")

	// Reset the buffer for next test
	logBuffer.Reset()

	// Run another query with multiple non-deterministic functions
	_, err = pool.Exec(ctx, "SELECT id, random(), uuid_generate_v4() FROM test_users")
	require.NoError(t, err, "Failed to execute query")

	// Give some time for logging
	time.Sleep(500 * time.Millisecond)

	// Check logs for non-deterministic warnings
	logOutput = logBuffer.String()
	assert.Contains(t, logOutput, "non-deterministic", "Expected non-deterministic function warning")
	assert.Contains(t, logOutput, "random()", "Expected warning for random() function")
}

// findVerificationResults searches the log buffer for verification results matching the query
func findVerificationResults(logBuffer *bytes.Buffer, tableNameContains string) ([]types.VerificationResult, bool) {
	logOutput := logBuffer.String()

	// Define a regex pattern to find verification result JSON
	pattern := `"level":"info".*?"message":"Verification result".*?"result":({.*?})`
	regex := regexp.MustCompile(pattern)
	matches := regex.FindAllSubmatch([]byte(logOutput), -1)

	var results []types.VerificationResult
	found := false

	// Also look for state commitment records to match with transaction IDs
	commitRecordPattern := `"level":"info".*?"message":"State commitment".*?"record":({.*?})`
	commitRegex := regexp.MustCompile(commitRecordPattern)
	commitMatches := commitRegex.FindAllSubmatch([]byte(logOutput), -1)

	// Map of TX IDs to queries for later matching
	txToQuery := make(map[uint64][]string)

	// Extract transaction IDs and queries from state commitment records
	for _, match := range commitMatches {
		if len(match) >= 2 {
			jsonStr := match[1]
			var record types.StateCommitmentRecord
			if err := json.Unmarshal(jsonStr, &record); err == nil {
				for _, query := range record.QuerySequence {
					if tableNameContains == "" || (tableNameContains != "" && containsIgnoreCase(query, tableNameContains)) {
						txToQuery[record.TxID] = record.QuerySequence
						break
					}
				}
			}
		}
	}

	// Extract verification results and match with the TX IDs we found
	for _, match := range matches {
		if len(match) >= 2 {
			jsonStr := match[1]
			var result types.VerificationResult
			if err := json.Unmarshal(jsonStr, &result); err == nil {
				// Check if this result is for a transaction that affected our target table
				if _, ok := txToQuery[result.TxID]; ok {
					found = true
					results = append(results, result)
				}
			}
		}
	}

	return results, found
}

// containsIgnoreCase checks if a string contains another string, case-insensitive
func containsIgnoreCase(s, substr string) bool {
	s, substr = strings.ToLower(s), strings.ToLower(substr)
	return strings.Contains(s, substr)
}

// testVerification tests successful verification
func testVerification(t *testing.T, cfg *config.Config, logBuffer *bytes.Buffer) {
	pool := setupTestDatabase(t, cfg)
	defer pool.Close()

	ctx := context.Background()

	// Reset the log buffer
	logBuffer.Reset()

	// Clean start
	_, err := pool.Exec(ctx, `DROP TABLE IF EXISTS test_verif; CREATE TABLE test_verif (id SERIAL PRIMARY KEY, value TEXT);`)
	require.NoError(t, err, "Failed to create test table")

	// Insert data that should be verified
	_, err = pool.Exec(ctx, "INSERT INTO test_verif (value) VALUES ('test1')")
	require.NoError(t, err, "Failed to insert test data")

	// Update data that should be verified
	_, err = pool.Exec(ctx, "UPDATE test_verif SET value = 'test2' WHERE id = 1")
	require.NoError(t, err, "Failed to update test data")

	// Delete data that should be verified
	_, err = pool.Exec(ctx, "DELETE FROM test_verif WHERE id = 1")
	require.NoError(t, err, "Failed to delete test data")

	// Give verification some time to complete (increased for more reliable test)
	time.Sleep(3 * time.Second)

	// Verify count is 0
	var count int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM test_verif").Scan(&count)
	require.NoError(t, err, "Failed to count records")
	assert.Equal(t, 0, count, "Expected 0 records")

	// Now verify that verification was successful by checking logs
	results, found := findVerificationResults(logBuffer, "test_verif")
	assert.True(t, found, "Expected to find verification results in logs")

	// Check that we have successful verification results
	successfulVerifications := 0
	for _, result := range results {
		if result.Success {
			successfulVerifications++
		}
	}

	// We should have at least the insert, update, and delete verifications
	assert.GreaterOrEqual(t, successfulVerifications, 3, "Expected at least 3 successful verifications")
}

// testFailedVerification intentionally tests verification failure by modifying backend data directly
func testFailedVerification(t *testing.T, cfg *config.Config, logBuffer *bytes.Buffer) {
	pool := setupTestDatabase(t, cfg)
	defer pool.Close()

	ctx := context.Background()

	// Reset the log buffer
	logBuffer.Reset()

	// Clean start
	_, err := pool.Exec(ctx, `DROP TABLE IF EXISTS test_fail; CREATE TABLE test_fail (id SERIAL PRIMARY KEY, value TEXT);`)
	require.NoError(t, err, "Failed to create test table")

	// Insert initial data
	_, err = pool.Exec(ctx, "INSERT INTO test_fail (value) VALUES ('initial')")
	require.NoError(t, err, "Failed to insert test data")

	// Give verification some time to complete
	time.Sleep(1 * time.Second)

	// Connect directly to the backend database to modify data, bypassing the proxy
	// This will create a state that won't match during verification
	backendConnStr := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres", cfg.BackendDB.Port)
	backendPool, err := pgxpool.New(ctx, backendConnStr)
	require.NoError(t, err, "Failed to connect to backend database")
	defer backendPool.Close()

	// Modify data directly in the backend
	_, err = backendPool.Exec(ctx, "UPDATE test_fail SET value = 'modified_directly' WHERE id = 1")
	require.NoError(t, err, "Failed to update backend directly")

	// Now perform an update through the proxy
	// This should fail verification because the pre-state won't match
	_, err = pool.Exec(ctx, "UPDATE test_fail SET value = 'through_proxy' WHERE id = 1")
	require.NoError(t, err, "Failed to update through proxy")

	// Give verification sufficient time to complete
	time.Sleep(3 * time.Second)

	// Find verification results for the test_fail table
	results, found := findVerificationResults(logBuffer, "test_fail")
	assert.True(t, found, "Expected to find verification results for test_fail")

	// At least one verification should have failed
	failedVerifications := 0
	for _, result := range results {
		if !result.Success {
			failedVerifications++
		}
	}

	assert.GreaterOrEqual(t, failedVerifications, 1, "Expected at least one failed verification for test_fail")
}