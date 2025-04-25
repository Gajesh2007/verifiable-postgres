package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects performance metrics for the proxy
type Metrics struct {
	// Connection metrics
	ActiveConnections    int64
	TotalConnections     int64

	// Query metrics
	TotalQueries         int64
	SelectQueries        int64
	InsertQueries        int64
	UpdateQueries        int64
	DeleteQueries        int64
	OtherQueries         int64
	FailedQueries        int64

	// Transaction metrics
	ActiveTransactions   int64
	TotalTransactions    int64
	CommittedTransactions int64
	RolledBackTransactions int64

	// Verification metrics
	TotalVerifications   int64
	SuccessfulVerifications int64
	FailedVerifications  int64

	// Latency metrics (in nanoseconds)
	QueryLatencyTotal    int64
	QueryLatencyCount    int64
	DMLLatencyTotal      int64
	DMLLatencyCount      int64
	VerificationLatencyTotal int64
	VerificationLatencyCount int64

	// Warning metrics
	NonDeterministicWarnings int64

	// Mutex for atomic updates to complex structures
	mu sync.Mutex
}

// NewMetrics creates a new metrics collector
func NewMetrics() *Metrics {
	return &Metrics{}
}

// ConnectionStarted records the start of a new connection
func (m *Metrics) ConnectionStarted() {
	atomic.AddInt64(&m.TotalConnections, 1)
	atomic.AddInt64(&m.ActiveConnections, 1)
}

// ConnectionClosed records the end of a connection
func (m *Metrics) ConnectionClosed() {
	atomic.AddInt64(&m.ActiveConnections, -1)
}

// QueryExecuted records the execution of a query
func (m *Metrics) QueryExecuted(queryType string, latency time.Duration) {
	atomic.AddInt64(&m.TotalQueries, 1)
	atomic.AddInt64(&m.QueryLatencyTotal, int64(latency))
	atomic.AddInt64(&m.QueryLatencyCount, 1)

	switch queryType {
	case "SELECT":
		atomic.AddInt64(&m.SelectQueries, 1)
	case "INSERT":
		atomic.AddInt64(&m.InsertQueries, 1)
		atomic.AddInt64(&m.DMLLatencyTotal, int64(latency))
		atomic.AddInt64(&m.DMLLatencyCount, 1)
	case "UPDATE":
		atomic.AddInt64(&m.UpdateQueries, 1)
		atomic.AddInt64(&m.DMLLatencyTotal, int64(latency))
		atomic.AddInt64(&m.DMLLatencyCount, 1)
	case "DELETE":
		atomic.AddInt64(&m.DeleteQueries, 1)
		atomic.AddInt64(&m.DMLLatencyTotal, int64(latency))
		atomic.AddInt64(&m.DMLLatencyCount, 1)
	default:
		atomic.AddInt64(&m.OtherQueries, 1)
	}
}

// QueryFailed records a failed query
func (m *Metrics) QueryFailed() {
	atomic.AddInt64(&m.FailedQueries, 1)
}

// TransactionStarted records the start of a transaction
func (m *Metrics) TransactionStarted() {
	atomic.AddInt64(&m.TotalTransactions, 1)
	atomic.AddInt64(&m.ActiveTransactions, 1)
}

// TransactionCommitted records a committed transaction
func (m *Metrics) TransactionCommitted() {
	atomic.AddInt64(&m.CommittedTransactions, 1)
	atomic.AddInt64(&m.ActiveTransactions, -1)
}

// TransactionRolledBack records a rolled back transaction
func (m *Metrics) TransactionRolledBack() {
	atomic.AddInt64(&m.RolledBackTransactions, 1)
	atomic.AddInt64(&m.ActiveTransactions, -1)
}

// VerificationStarted records the start of a verification job
func (m *Metrics) VerificationStarted() {
	atomic.AddInt64(&m.TotalVerifications, 1)
}

// VerificationCompleted records a completed verification job
func (m *Metrics) VerificationCompleted(success bool, latency time.Duration) {
	atomic.AddInt64(&m.VerificationLatencyTotal, int64(latency))
	atomic.AddInt64(&m.VerificationLatencyCount, 1)

	if success {
		atomic.AddInt64(&m.SuccessfulVerifications, 1)
	} else {
		atomic.AddInt64(&m.FailedVerifications, 1)
	}
}

// NonDeterministicWarningLogged records a non-deterministic function warning
func (m *Metrics) NonDeterministicWarningLogged() {
	atomic.AddInt64(&m.NonDeterministicWarnings, 1)
}

// GetMetrics returns a copy of the current metrics
func (m *Metrics) GetMetrics() map[string]interface{} {
	metrics := make(map[string]interface{})

	// Connection metrics
	metrics["active_connections"] = atomic.LoadInt64(&m.ActiveConnections)
	metrics["total_connections"] = atomic.LoadInt64(&m.TotalConnections)

	// Query metrics
	metrics["total_queries"] = atomic.LoadInt64(&m.TotalQueries)
	metrics["select_queries"] = atomic.LoadInt64(&m.SelectQueries)
	metrics["insert_queries"] = atomic.LoadInt64(&m.InsertQueries)
	metrics["update_queries"] = atomic.LoadInt64(&m.UpdateQueries)
	metrics["delete_queries"] = atomic.LoadInt64(&m.DeleteQueries)
	metrics["other_queries"] = atomic.LoadInt64(&m.OtherQueries)
	metrics["failed_queries"] = atomic.LoadInt64(&m.FailedQueries)

	// Transaction metrics
	metrics["active_transactions"] = atomic.LoadInt64(&m.ActiveTransactions)
	metrics["total_transactions"] = atomic.LoadInt64(&m.TotalTransactions)
	metrics["committed_transactions"] = atomic.LoadInt64(&m.CommittedTransactions)
	metrics["rolled_back_transactions"] = atomic.LoadInt64(&m.RolledBackTransactions)

	// Verification metrics
	metrics["total_verifications"] = atomic.LoadInt64(&m.TotalVerifications)
	metrics["successful_verifications"] = atomic.LoadInt64(&m.SuccessfulVerifications)
	metrics["failed_verifications"] = atomic.LoadInt64(&m.FailedVerifications)

	// Warning metrics
	metrics["non_deterministic_warnings"] = atomic.LoadInt64(&m.NonDeterministicWarnings)

	// Calculated latency metrics (in milliseconds)
	queryLatencyTotal := atomic.LoadInt64(&m.QueryLatencyTotal)
	queryLatencyCount := atomic.LoadInt64(&m.QueryLatencyCount)
	if queryLatencyCount > 0 {
		metrics["avg_query_latency_ms"] = float64(queryLatencyTotal) / float64(queryLatencyCount) / float64(time.Millisecond)
	} else {
		metrics["avg_query_latency_ms"] = 0.0
	}

	dmlLatencyTotal := atomic.LoadInt64(&m.DMLLatencyTotal)
	dmlLatencyCount := atomic.LoadInt64(&m.DMLLatencyCount)
	if dmlLatencyCount > 0 {
		metrics["avg_dml_latency_ms"] = float64(dmlLatencyTotal) / float64(dmlLatencyCount) / float64(time.Millisecond)
	} else {
		metrics["avg_dml_latency_ms"] = 0.0
	}

	verificationLatencyTotal := atomic.LoadInt64(&m.VerificationLatencyTotal)
	verificationLatencyCount := atomic.LoadInt64(&m.VerificationLatencyCount)
	if verificationLatencyCount > 0 {
		metrics["avg_verification_latency_ms"] = float64(verificationLatencyTotal) / float64(verificationLatencyCount) / float64(time.Millisecond)
	} else {
		metrics["avg_verification_latency_ms"] = 0.0
	}

	return metrics
}

// Reset resets all metrics
func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ActiveConnections = 0
	m.TotalConnections = 0
	m.TotalQueries = 0
	m.SelectQueries = 0
	m.InsertQueries = 0
	m.UpdateQueries = 0
	m.DeleteQueries = 0
	m.OtherQueries = 0
	m.FailedQueries = 0
	m.ActiveTransactions = 0
	m.TotalTransactions = 0
	m.CommittedTransactions = 0
	m.RolledBackTransactions = 0
	m.TotalVerifications = 0
	m.SuccessfulVerifications = 0
	m.FailedVerifications = 0
	m.QueryLatencyTotal = 0
	m.QueryLatencyCount = 0
	m.DMLLatencyTotal = 0
	m.DMLLatencyCount = 0
	m.VerificationLatencyTotal = 0
	m.VerificationLatencyCount = 0
	m.NonDeterministicWarnings = 0
}