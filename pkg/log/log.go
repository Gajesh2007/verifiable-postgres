package log

import (
	"context"
	"io"
	"log/slog"
	"os"

	"github.com/verifiable-postgres/proxy/pkg/config"
)

// Logger is the global logger instance
var Logger *slog.Logger

// Setup initializes the logger with the given configuration
func Setup(cfg *config.LogConfig) {
	var handler slog.Handler

	// Configure the log level
	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	// Configure the log format
	if cfg.Format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: level,
		})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: level,
		})
	}

	// Create the logger
	Logger = slog.New(handler)
	slog.SetDefault(Logger)
}

// SetOutput sets the output destination for the logger
func SetOutput(w io.Writer) {
	var handler slog.Handler

	// Use the existing logger's attributes
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}

	// Create a new handler with the same format
	handler = slog.NewJSONHandler(w, opts)

	// Update the logger
	Logger = slog.New(handler)
	slog.SetDefault(Logger)
}

// With returns a logger with the given attributes
func With(attrs ...any) *slog.Logger {
	return Logger.With(attrs...)
}

// Debug logs a debug message
func Debug(msg string, attrs ...any) {
	Logger.Debug(msg, attrs...)
}

// Info logs an info message
func Info(msg string, attrs ...any) {
	Logger.Info(msg, attrs...)
}

// Warn logs a warning message
func Warn(msg string, attrs ...any) {
	Logger.Warn(msg, attrs...)
}

// Error logs an error message
func Error(msg string, attrs ...any) {
	Logger.Error(msg, attrs...)
}

// LogStateCommitment logs a state commitment record
func LogStateCommitment(ctx context.Context, record any) {
	Logger.LogAttrs(ctx, slog.LevelInfo, "State Commitment Record", 
		slog.Any("record", record))
}

// LogVerificationResult logs a verification result
func LogVerificationResult(ctx context.Context, result any) {
	Logger.LogAttrs(ctx, slog.LevelInfo, "Verification Result", 
		slog.Any("result", result))
}

// LogNonDeterministicWarning logs a warning about non-deterministic functions
func LogNonDeterministicWarning(ctx context.Context, warning string, query string) {
	Logger.LogAttrs(ctx, slog.LevelWarn, "Non-deterministic Query", 
		slog.String("warning", warning),
		slog.String("query", query))
}