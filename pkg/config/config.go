package config

import (
	"flag"
	"fmt"
)

// Config represents the application configuration
type Config struct {
	// Proxy settings
	ListenAddr     string
	MaxConnections int

	// Backend database settings
	BackendDB DatabaseConfig

	// Verification database settings
	VerificationDB DatabaseConfig

	// Replay engine settings
	ReplayEngine ReplayEngineConfig

	// Logging settings
	Log LogConfig

	// Feature flags
	Features FeatureFlags
}

// DatabaseConfig represents database connection settings
type DatabaseConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

// ReplayEngineConfig represents replay engine settings
type ReplayEngineConfig struct {
	WorkerPoolSize int
	JobQueueSize   int
}

// LogConfig represents logging settings
type LogConfig struct {
	Level  string
	Format string
}

// FeatureFlags represents feature flags
type FeatureFlags struct {
	EnableVerification         bool
	LogNonDeterministicWarnings bool
}

// DSN returns a PostgreSQL connection string
func (db DatabaseConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		db.Host, db.Port, db.User, db.Password, db.DBName)
}

// Load loads the configuration from command line flags
func Load() (*Config, error) {
	cfg := &Config{}

	// Proxy settings
	flag.StringVar(&cfg.ListenAddr, "listen", "localhost:5432", "Address to listen on")
	flag.IntVar(&cfg.MaxConnections, "max-connections", 100, "Maximum number of client connections")

	// Backend database settings
	flag.StringVar(&cfg.BackendDB.Host, "backend-host", "localhost", "Backend database host")
	flag.IntVar(&cfg.BackendDB.Port, "backend-port", 5433, "Backend database port")
	flag.StringVar(&cfg.BackendDB.User, "backend-user", "postgres", "Backend database user")
	flag.StringVar(&cfg.BackendDB.Password, "backend-password", "postgres", "Backend database password")
	flag.StringVar(&cfg.BackendDB.DBName, "backend-dbname", "postgres", "Backend database name")

	// Verification database settings
	flag.StringVar(&cfg.VerificationDB.Host, "verification-host", "localhost", "Verification database host")
	flag.IntVar(&cfg.VerificationDB.Port, "verification-port", 5434, "Verification database port")
	flag.StringVar(&cfg.VerificationDB.User, "verification-user", "postgres", "Verification database user")
	flag.StringVar(&cfg.VerificationDB.Password, "verification-password", "postgres", "Verification database password")
	flag.StringVar(&cfg.VerificationDB.DBName, "verification-dbname", "verification", "Verification database name")

	// Replay engine settings
	flag.IntVar(&cfg.ReplayEngine.WorkerPoolSize, "worker-pool-size", 4, "Number of verification workers")
	flag.IntVar(&cfg.ReplayEngine.JobQueueSize, "job-queue-size", 1000, "Size of the verification job queue")

	// Logging settings
	flag.StringVar(&cfg.Log.Level, "log-level", "info", "Log level (debug, info, warn, error)")
	flag.StringVar(&cfg.Log.Format, "log-format", "json", "Log format (json, text)")

	// Feature flags
	flag.BoolVar(&cfg.Features.EnableVerification, "enable-verification", true, "Enable verification")
	flag.BoolVar(&cfg.Features.LogNonDeterministicWarnings, "log-nondeterministic-warnings", true, "Log warnings for non-deterministic queries")

	// Parse flags
	flag.Parse()

	return cfg, nil
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		ListenAddr:     "localhost:5432",
		MaxConnections: 100,
		BackendDB: DatabaseConfig{
			Host:     "localhost",
			Port:     5433,
			User:     "postgres",
			Password: "postgres",
			DBName:   "postgres",
		},
		VerificationDB: DatabaseConfig{
			Host:     "localhost",
			Port:     5434,
			User:     "postgres",
			Password: "postgres",
			DBName:   "verification",
		},
		ReplayEngine: ReplayEngineConfig{
			WorkerPoolSize: 4,
			JobQueueSize:   1000,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "json",
		},
		Features: FeatureFlags{
			EnableVerification:         true,
			LogNonDeterministicWarnings: true,
		},
	}
}