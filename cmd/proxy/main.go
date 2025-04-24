package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/verifiable-postgres/proxy/pkg/config"
	"github.com/verifiable-postgres/proxy/pkg/log"
	"github.com/verifiable-postgres/proxy/pkg/proxy"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	// Set up logging
	log.Setup(&cfg.Log)
	log.Info("Starting Verifiable Postgres Proxy",
		"listen_addr", cfg.ListenAddr,
		"backend_host", cfg.BackendDB.Host,
		"backend_port", cfg.BackendDB.Port,
		"verification_enabled", cfg.Features.EnableVerification)

	// Create server
	server, err := proxy.NewServer(cfg)
	if err != nil {
		log.Error("Failed to create server", "error", err)
		os.Exit(1)
	}

	// Handle graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		// Catch signals
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

		// Wait for signal
		sig := <-sigChan
		log.Info("Received signal, shutting down", "signal", sig)

		// Cancel context
		cancel()
	}()

	// Start the server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Start()
	}()

	// Wait for shutdown or error
	select {
	case <-ctx.Done():
		// Graceful shutdown
		if err := server.Stop(); err != nil {
			log.Error("Error stopping server", "error", err)
		}
	case err := <-errChan:
		// Server error
		log.Error("Server error", "error", err)
	}

	log.Info("Server shutdown complete")
}