package proxy

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/verifiable-postgres/proxy/pkg/config"
	"github.com/verifiable-postgres/proxy/pkg/log"
	"github.com/verifiable-postgres/proxy/pkg/metrics"
	"github.com/verifiable-postgres/proxy/pkg/replay"
	"github.com/verifiable-postgres/proxy/pkg/types"
)

// Server represents the PostgreSQL proxy server
type Server struct {
	cfg          *config.Config
	listener     net.Listener
	connections  map[uint64]*Connection
	mu           sync.Mutex
	nextConnID   uint64
	nextTxID     uint64
	replayEngine *replay.Engine
	jobQueue     chan types.VerificationJob
	metrics      *metrics.Metrics
	closed       bool
}

// NewServer creates a new PostgreSQL proxy server
func NewServer(cfg *config.Config) (*Server, error) {
	// Create job queue for verification
	jobQueue := make(chan types.VerificationJob, cfg.ReplayEngine.JobQueueSize)
	
	// Create metrics collector
	metricsCollector := metrics.NewMetrics()

	// Create server instance
	server := &Server{
		cfg:         cfg,
		connections: make(map[uint64]*Connection),
		jobQueue:    jobQueue,
		metrics:     metricsCollector,
	}

	// Create replay engine if verification is enabled
	if cfg.Features.EnableVerification {
		engine, err := replay.NewEngine(cfg, jobQueue, metricsCollector)
		if err != nil {
			return nil, err
		}
		server.replayEngine = engine
	}

	return server, nil
}

// GetMetrics returns the current metrics
func (s *Server) GetMetrics() map[string]interface{} {
	return s.metrics.GetMetrics()
}

// Start starts the server and listens for incoming connections
func (s *Server) Start() error {
	// Start listening
	listener, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return err
	}
	s.listener = listener

	log.Info("Server started", "address", s.cfg.ListenAddr)

	// Start the replay engine if enabled
	if s.cfg.Features.EnableVerification && s.replayEngine != nil {
		if err := s.replayEngine.Start(); err != nil {
			return err
		}
		log.Info("Replay engine started")
	}

	// Accept connections
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed {
				return nil
			}
			// Log error and continue accepting connections
			log.Error("Error accepting connection", "error", err)
			continue
		}

		// Check if we've reached the maximum number of connections
		s.mu.Lock()
		if len(s.connections) >= s.cfg.MaxConnections {
			s.mu.Unlock()
			log.Warn("Maximum connections reached, rejecting connection")
			conn.Close()
			continue
		}

		// Assign a connection ID and increment the counter
		connID := atomic.AddUint64(&s.nextConnID, 1) - 1
		s.mu.Unlock()

		// Create a new connection
		pgConn, err := NewConnection(context.Background(), s, connID, conn)
		if err != nil {
			log.Error("Error creating connection", "error", err)
			conn.Close()
			continue
		}

		// Register the connection
		s.mu.Lock()
		s.connections[connID] = pgConn
		s.mu.Unlock()

		// Handle the connection in a goroutine
		go func() {
			if err := pgConn.Handle(); err != nil {
				if !errors.Is(err, net.ErrClosed) {
					log.Error("Connection error", "id", connID, "error", err)
				}
			}

			// Unregister the connection
			s.mu.Lock()
			delete(s.connections, connID)
			s.mu.Unlock()
		}()
	}
}

// Stop stops the server and closes all connections
func (s *Server) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Mark as closed
	s.closed = true

	// Close the listener
	if s.listener != nil {
		s.listener.Close()
	}

	// Close all connections
	for _, conn := range s.connections {
		conn.Close()
	}

	// Stop the replay engine
	if s.replayEngine != nil {
		s.replayEngine.Stop()
	}

	// Close the job queue
	close(s.jobQueue)

	log.Info("Server stopped")
	return nil
}

// GetNextTxID returns the next transaction ID
func (s *Server) GetNextTxID() uint64 {
	return atomic.AddUint64(&s.nextTxID, 1) - 1
}

// SendVerificationJob sends a verification job to the replay engine
func (s *Server) SendVerificationJob(job types.VerificationJob) {
	if !s.cfg.Features.EnableVerification {
		return
	}

	// Send the job to the queue
	select {
	case s.jobQueue <- job:
		log.Debug("Sent verification job", "txID", job.TxID)
	default:
		log.Warn("Job queue full, dropping verification job", "txID", job.TxID)
	}
}