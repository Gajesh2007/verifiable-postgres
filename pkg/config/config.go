package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config represents the application configuration
type Config struct {
	// Proxy settings
	ListenAddr     string
	MaxConnections int
	MetricsAddr    string

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
	Host         string
	Port         int
	User         string
	Password     string
	DBName       string
	MaxOpenConns int
	MaxIdleConns int
	ConnLifetime time.Duration
}

// ReplayEngineConfig represents replay engine settings
type ReplayEngineConfig struct {
	WorkerPoolSize int
	JobQueueSize   int
	RetryAttempts  int
	RetryDelay     time.Duration
}

// LogConfig represents logging settings
type LogConfig struct {
	Level   string
	Format  string
	File    string
	Console bool
}

// FeatureFlags represents feature flags
type FeatureFlags struct {
	EnableVerification         bool
	LogNonDeterministicWarnings bool
	UseWALForStateCapture      bool // Reserved for V2
}

// DSN returns a PostgreSQL connection string
func (db DatabaseConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		db.Host, db.Port, db.User, db.Password, db.DBName)
}

// Load loads the configuration from environment variables, config file, and command line flags
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Set default values
	setDefaults(v)

	// Environment variable configuration
	v.SetEnvPrefix("VPG")                                 // VPG = Verifiable Postgres
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))   // Convert nested keys to env var format
	v.AutomaticEnv()                                      // Read environment variables

	// Config file support
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		// Look for config in default locations
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("$HOME/.verifiable-postgres")
		v.AddConfigPath("/etc/verifiable-postgres")
	}

	// Load config file if it exists
	if err := v.ReadInConfig(); err != nil {
		// Config file not found, use only environment variables and defaults
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	// Create and populate the Config struct
	cfg := &Config{}

	// Proxy settings
	cfg.ListenAddr = v.GetString("proxy.listen_addr")
	cfg.MaxConnections = v.GetInt("proxy.max_connections")
	cfg.MetricsAddr = v.GetString("proxy.metrics_addr")

	// Backend database settings
	cfg.BackendDB = DatabaseConfig{
		Host:         v.GetString("backend.host"),
		Port:         v.GetInt("backend.port"),
		User:         v.GetString("backend.user"),
		Password:     v.GetString("backend.password"),
		DBName:       v.GetString("backend.dbname"),
		MaxOpenConns: v.GetInt("backend.max_open_conns"),
		MaxIdleConns: v.GetInt("backend.max_idle_conns"),
		ConnLifetime: v.GetDuration("backend.conn_lifetime"),
	}

	// Verification database settings
	cfg.VerificationDB = DatabaseConfig{
		Host:         v.GetString("verification.host"),
		Port:         v.GetInt("verification.port"),
		User:         v.GetString("verification.user"),
		Password:     v.GetString("verification.password"),
		DBName:       v.GetString("verification.dbname"),
		MaxOpenConns: v.GetInt("verification.max_open_conns"),
		MaxIdleConns: v.GetInt("verification.max_idle_conns"),
		ConnLifetime: v.GetDuration("verification.conn_lifetime"),
	}

	// Replay engine settings
	cfg.ReplayEngine = ReplayEngineConfig{
		WorkerPoolSize: v.GetInt("replay.worker_pool_size"),
		JobQueueSize:   v.GetInt("replay.job_queue_size"),
		RetryAttempts:  v.GetInt("replay.retry_attempts"),
		RetryDelay:     v.GetDuration("replay.retry_delay"),
	}

	// Logging settings
	cfg.Log = LogConfig{
		Level:   v.GetString("log.level"),
		Format:  v.GetString("log.format"),
		File:    v.GetString("log.file"),
		Console: v.GetBool("log.console"),
	}

	// Feature flags
	cfg.Features = FeatureFlags{
		EnableVerification:         v.GetBool("features.enable_verification"),
		LogNonDeterministicWarnings: v.GetBool("features.log_nondeterministic_warnings"),
		UseWALForStateCapture:      v.GetBool("features.use_wal_for_state_capture"),
	}

	return cfg, nil
}

// setDefaults sets the default configuration values
func setDefaults(v *viper.Viper) {
	// Proxy settings
	v.SetDefault("proxy.listen_addr", "localhost:5432")
	v.SetDefault("proxy.max_connections", 100)
	v.SetDefault("proxy.metrics_addr", "localhost:2112")

	// Backend database settings
	v.SetDefault("backend.host", "localhost")
	v.SetDefault("backend.port", 5433)
	v.SetDefault("backend.user", "postgres")
	v.SetDefault("backend.password", "postgres")
	v.SetDefault("backend.dbname", "postgres")
	v.SetDefault("backend.max_open_conns", 20)
	v.SetDefault("backend.max_idle_conns", 10)
	v.SetDefault("backend.conn_lifetime", time.Minute*5)

	// Verification database settings
	v.SetDefault("verification.host", "localhost")
	v.SetDefault("verification.port", 5434)
	v.SetDefault("verification.user", "postgres")
	v.SetDefault("verification.password", "postgres")
	v.SetDefault("verification.dbname", "verification")
	v.SetDefault("verification.max_open_conns", 20)
	v.SetDefault("verification.max_idle_conns", 10)
	v.SetDefault("verification.conn_lifetime", time.Minute*5)

	// Replay engine settings
	v.SetDefault("replay.worker_pool_size", 4)
	v.SetDefault("replay.job_queue_size", 1000)
	v.SetDefault("replay.retry_attempts", 3)
	v.SetDefault("replay.retry_delay", time.Second*1)

	// Logging settings
	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "json")
	v.SetDefault("log.file", "")
	v.SetDefault("log.console", true)

	// Feature flags
	v.SetDefault("features.enable_verification", true)
	v.SetDefault("features.log_nondeterministic_warnings", true)
	v.SetDefault("features.use_wal_for_state_capture", false) // Reserved for V2
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	cfg := &Config{
		ListenAddr:     "localhost:5432",
		MaxConnections: 100,
		MetricsAddr:    "localhost:2112",
		BackendDB: DatabaseConfig{
			Host:         "localhost",
			Port:         5433,
			User:         "postgres",
			Password:     "postgres",
			DBName:       "postgres",
			MaxOpenConns: 20,
			MaxIdleConns: 10,
			ConnLifetime: time.Minute * 5,
		},
		VerificationDB: DatabaseConfig{
			Host:         "localhost",
			Port:         5434,
			User:         "postgres",
			Password:     "postgres",
			DBName:       "verification",
			MaxOpenConns: 20,
			MaxIdleConns: 10,
			ConnLifetime: time.Minute * 5,
		},
		ReplayEngine: ReplayEngineConfig{
			WorkerPoolSize: 4,
			JobQueueSize:   1000,
			RetryAttempts:  3,
			RetryDelay:     time.Second * 1,
		},
		Log: LogConfig{
			Level:   "info",
			Format:  "json",
			File:    "",
			Console: true,
		},
		Features: FeatureFlags{
			EnableVerification:         true,
			LogNonDeterministicWarnings: true,
			UseWALForStateCapture:      false, // Reserved for V2
		},
	}
	return cfg
}