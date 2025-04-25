package metrics

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// MetricsServer is an HTTP server that exposes metrics
type MetricsServer struct {
	metrics     *Metrics
	server      *http.Server
	registry    *prometheus.Registry
	collectors  map[string]prometheus.Collector
	initialized bool
}

// NewMetricsServer creates a new metrics HTTP server
func NewMetricsServer(metrics *Metrics, addr string) *MetricsServer {
	ms := &MetricsServer{
		metrics:    metrics,
		registry:   prometheus.NewRegistry(),
		collectors: make(map[string]prometheus.Collector),
	}

	// Create HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", ms.handleMetrics)
	mux.HandleFunc("/metrics/prometheus", ms.handlePrometheusMetrics)
	mux.HandleFunc("/metrics/json", ms.handleJSONMetrics)

	ms.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	return ms
}

// Start starts the metrics server
func (ms *MetricsServer) Start() error {
	// Initialize Prometheus metrics if not already initialized
	if !ms.initialized {
		ms.initPrometheusMetrics()
	}

	// Start HTTP server in a goroutine
	go func() {
		if err := ms.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Log error but don't crash the application
			// This is a non-critical component
		}
	}()

	return nil
}

// Stop stops the metrics server
func (ms *MetricsServer) Stop() error {
	return ms.server.Close()
}

// initPrometheusMetrics initializes Prometheus metrics
func (ms *MetricsServer) initPrometheusMetrics() {
	// Connection metrics
	ms.collectors["connections_active"] = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "verifiable_postgres_connections_active",
			Help: "Number of active connections",
		},
		func() float64 { return float64(ms.metrics.ActiveConnections) },
	)
	ms.collectors["connections_total"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_connections_total",
			Help: "Total number of connections",
		},
		func() float64 { return float64(ms.metrics.TotalConnections) },
	)

	// Query metrics
	ms.collectors["queries_total"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_queries_total",
			Help: "Total number of queries",
		},
		func() float64 { return float64(ms.metrics.TotalQueries) },
	)
	ms.collectors["queries_select"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_queries_select",
			Help: "Number of SELECT queries",
		},
		func() float64 { return float64(ms.metrics.SelectQueries) },
	)
	ms.collectors["queries_insert"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_queries_insert",
			Help: "Number of INSERT queries",
		},
		func() float64 { return float64(ms.metrics.InsertQueries) },
	)
	ms.collectors["queries_update"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_queries_update",
			Help: "Number of UPDATE queries",
		},
		func() float64 { return float64(ms.metrics.UpdateQueries) },
	)
	ms.collectors["queries_delete"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_queries_delete",
			Help: "Number of DELETE queries",
		},
		func() float64 { return float64(ms.metrics.DeleteQueries) },
	)
	ms.collectors["queries_failed"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_queries_failed",
			Help: "Number of failed queries",
		},
		func() float64 { return float64(ms.metrics.FailedQueries) },
	)

	// Transaction metrics
	ms.collectors["transactions_active"] = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "verifiable_postgres_transactions_active",
			Help: "Number of active transactions",
		},
		func() float64 { return float64(ms.metrics.ActiveTransactions) },
	)
	ms.collectors["transactions_total"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_transactions_total",
			Help: "Total number of transactions",
		},
		func() float64 { return float64(ms.metrics.TotalTransactions) },
	)
	ms.collectors["transactions_committed"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_transactions_committed",
			Help: "Number of committed transactions",
		},
		func() float64 { return float64(ms.metrics.CommittedTransactions) },
	)
	ms.collectors["transactions_rolled_back"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_transactions_rolled_back",
			Help: "Number of rolled back transactions",
		},
		func() float64 { return float64(ms.metrics.RolledBackTransactions) },
	)

	// Verification metrics
	ms.collectors["verifications_total"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_verifications_total",
			Help: "Total number of verifications",
		},
		func() float64 { return float64(ms.metrics.TotalVerifications) },
	)
	ms.collectors["verifications_successful"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_verifications_successful",
			Help: "Number of successful verifications",
		},
		func() float64 { return float64(ms.metrics.SuccessfulVerifications) },
	)
	ms.collectors["verifications_failed"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_verifications_failed",
			Help: "Number of failed verifications",
		},
		func() float64 { return float64(ms.metrics.FailedVerifications) },
	)

	// Warning metrics
	ms.collectors["warnings_non_deterministic"] = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Name: "verifiable_postgres_warnings_non_deterministic",
			Help: "Number of non-deterministic function warnings",
		},
		func() float64 { return float64(ms.metrics.NonDeterministicWarnings) },
	)

	// Latency metrics
	ms.collectors["query_latency_seconds"] = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "verifiable_postgres_query_latency_seconds",
			Help: "Average query latency in seconds",
		},
		func() float64 {
			count := ms.metrics.QueryLatencyCount
			if count == 0 {
				return 0
			}
			return float64(ms.metrics.QueryLatencyTotal) / float64(count) / float64(time.Second)
		},
	)
	ms.collectors["dml_latency_seconds"] = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "verifiable_postgres_dml_latency_seconds",
			Help: "Average DML latency in seconds",
		},
		func() float64 {
			count := ms.metrics.DMLLatencyCount
			if count == 0 {
				return 0
			}
			return float64(ms.metrics.DMLLatencyTotal) / float64(count) / float64(time.Second)
		},
	)
	ms.collectors["verification_latency_seconds"] = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "verifiable_postgres_verification_latency_seconds",
			Help: "Average verification latency in seconds",
		},
		func() float64 {
			count := ms.metrics.VerificationLatencyCount
			if count == 0 {
				return 0
			}
			return float64(ms.metrics.VerificationLatencyTotal) / float64(count) / float64(time.Second)
		},
	)

	// Register all collectors with the registry
	for _, collector := range ms.collectors {
		ms.registry.MustRegister(collector)
	}

	ms.initialized = true
}

// handleMetrics handles the /metrics endpoint
func (ms *MetricsServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// Default to Prometheus format
	http.Redirect(w, r, "/metrics/prometheus", http.StatusSeeOther)
}

// handlePrometheusMetrics handles the /metrics/prometheus endpoint
func (ms *MetricsServer) handlePrometheusMetrics(w http.ResponseWriter, r *http.Request) {
	// Serve Prometheus metrics
	h := promhttp.HandlerFor(ms.registry, promhttp.HandlerOpts{})
	h.ServeHTTP(w, r)
}

// handleJSONMetrics handles the /metrics/json endpoint
func (ms *MetricsServer) handleJSONMetrics(w http.ResponseWriter, r *http.Request) {
	// Get current metrics
	metricsData := ms.metrics.GetMetrics()

	// Marshal to JSON
	jsonData, err := json.MarshalIndent(metricsData, "", "  ")
	if err != nil {
		http.Error(w, "Failed to marshal metrics", http.StatusInternalServerError)
		return
	}

	// Set content type and write response
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(jsonData)))
	w.Write(jsonData)
}
