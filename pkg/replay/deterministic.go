package replay

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/verifiable-postgres/proxy/pkg/types"
)

// SetupVerificationDB sets up the verification database with the pre-state data
func SetupVerificationDB(ctx context.Context, tx *sql.Tx, schemas map[string]types.TableSchema, preStateData map[string][]types.Row) error {
	// Create schema if it doesn't exist
	_, err := tx.ExecContext(ctx, "CREATE SCHEMA IF NOT EXISTS verification")
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Process each table
	for tableName, schema := range schemas {
		// Drop table if it exists
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
		_, err := tx.ExecContext(ctx, dropSQL)
		if err != nil {
			return fmt.Errorf("failed to drop table %s: %w", tableName, err)
		}

		// Create table
		createSQL := buildCreateTableSQL(schema)
		_, err = tx.ExecContext(ctx, createSQL)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}

		// Insert pre-state data
		rows, ok := preStateData[tableName]
		if !ok {
			continue // No data for this table
		}

		for _, row := range rows {
			insertSQL, args := buildInsertSQL(schema, row)
			_, err = tx.ExecContext(ctx, insertSQL, args...)
			if err != nil {
				return fmt.Errorf("failed to insert row into table %s: %w", tableName, err)
			}
		}
	}

	return nil
}

// SetupVerificationDBWithPgx sets up the verification database with the pre-state data using pgx
func SetupVerificationDBWithPgx(ctx context.Context, tx pgx.Tx, schemas map[string]types.TableSchema, preStateData map[string][]types.Row) error {
	// Create schema if it doesn't exist
	_, err := tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS verification")
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Process each table
	for tableName, schema := range schemas {
		// Drop table if it exists
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
		_, err := tx.Exec(ctx, dropSQL)
		if err != nil {
			return fmt.Errorf("failed to drop table %s: %w", tableName, err)
		}

		// Create table
		createSQL := buildCreateTableSQL(schema)
		_, err = tx.Exec(ctx, createSQL)
		if err != nil {
			return fmt.Errorf("failed to create table %s: %w", tableName, err)
		}

		// Insert pre-state data
		rows, ok := preStateData[tableName]
		if !ok {
			continue // No data for this table
		}

		for _, row := range rows {
			insertSQL, args := buildInsertSQL(schema, row)
			_, err = tx.Exec(ctx, insertSQL, args...)
			if err != nil {
				return fmt.Errorf("failed to insert row into table %s: %w", tableName, err)
			}
		}
	}

	return nil
}

// SetDeterministicSessionParams sets session parameters for deterministic execution
func SetDeterministicSessionParams(ctx context.Context, tx *sql.Tx) error {
	// Set timezone to UTC
	_, err := tx.ExecContext(ctx, "SET timezone = 'UTC'")
	if err != nil {
		return fmt.Errorf("failed to set timezone: %w", err)
	}

	// Disable parallel query execution
	_, err = tx.ExecContext(ctx, "SET max_parallel_workers_per_gather = 0")
	if err != nil {
		return fmt.Errorf("failed to set max_parallel_workers_per_gather: %w", err)
	}

	// Disable specific join types for deterministic execution
	paramValues := map[string]string{
		"enable_hashjoin":    "off",
		"enable_mergejoin":   "off",
		"enable_nestloop":    "on",
		"enable_seqscan":     "on",
		"enable_indexscan":   "off",
		"enable_indexonlyscan": "off",
		"enable_bitmapscan":  "off",
		"enable_tidscan":     "off",
		"enable_sort":        "on",
		"enable_incremental_sort": "off",
		"enable_material":    "off",
		"enable_gathermerge": "off",
	}

	for param, value := range paramValues {
		_, err := tx.ExecContext(ctx, fmt.Sprintf("SET %s = %s", param, value))
		if err != nil {
			return fmt.Errorf("failed to set %s: %w", param, err)
		}
	}

	return nil
}

// SetDeterministicSessionParamsWithPgx sets session parameters for deterministic execution using pgx
func SetDeterministicSessionParamsWithPgx(ctx context.Context, tx pgx.Tx) error {
	// Set timezone to UTC
	_, err := tx.Exec(ctx, "SET timezone = 'UTC'")
	if err != nil {
		return fmt.Errorf("failed to set timezone: %w", err)
	}

	// Disable parallel query execution
	_, err = tx.Exec(ctx, "SET max_parallel_workers_per_gather = 0")
	if err != nil {
		return fmt.Errorf("failed to set max_parallel_workers_per_gather: %w", err)
	}

	// Disable specific join types for deterministic execution
	paramValues := map[string]string{
		"enable_hashjoin":    "off",
		"enable_mergejoin":   "off",
		"enable_nestloop":    "on",
		"enable_seqscan":     "on",
		"enable_indexscan":   "off",
		"enable_indexonlyscan": "off",
		"enable_bitmapscan":  "off",
		"enable_tidscan":     "off",
		"enable_sort":        "on",
		"enable_incremental_sort": "off",
		"enable_material":    "off",
		"enable_gathermerge": "off",
	}

	for param, value := range paramValues {
		_, err := tx.Exec(ctx, fmt.Sprintf("SET %s = %s", param, value))
		if err != nil {
			return fmt.Errorf("failed to set %s: %w", param, err)
		}
	}

	return nil
}

// ExecuteQueriesDeterministically executes queries deterministically
func ExecuteQueriesDeterministically(ctx context.Context, tx *sql.Tx, queries []string) error {
	for i, query := range queries {
		_, err := tx.ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to execute query %d: %w", i, err)
		}
	}

	return nil
}

// ExecuteQueriesDeterministicallyWithPgx executes queries deterministically using pgx
func ExecuteQueriesDeterministicallyWithPgx(ctx context.Context, tx pgx.Tx, queries []string) error {
	for i, query := range queries {
		_, err := tx.Exec(ctx, query)
		if err != nil {
			return fmt.Errorf("failed to execute query %d: %w", i, err)
		}
	}

	return nil
}

// buildCreateTableSQL builds a CREATE TABLE SQL statement from a schema
func buildCreateTableSQL(schema types.TableSchema) string {
	var sb strings.Builder
	
	// Use double quotes for identifiers to prevent SQL injection and handle special characters
	sb.WriteString(fmt.Sprintf("CREATE TABLE \"%s\" (\n", schema.Name))
	
	// Add columns
	for i, col := range schema.Columns {
		if i > 0 {
			sb.WriteString(",\n")
		}
		
		// Quote identifiers to prevent SQL injection
		sb.WriteString(fmt.Sprintf("  \"%s\" %s", col.Name, col.Type))
		
		if !col.Nullable {
			sb.WriteString(" NOT NULL")
		}
	}
	
	// Add primary key constraint if available
	if len(schema.PKColumns) > 0 {
		sb.WriteString(",\n  PRIMARY KEY (")
		
		for i, pkCol := range schema.PKColumns {
			if i > 0 {
				sb.WriteString(", ")
			}
			// Quote identifiers to prevent SQL injection
			sb.WriteString(fmt.Sprintf("\"%s\"", pkCol))
		}
		
		sb.WriteString(")")
	}
	
	sb.WriteString("\n)")
	
	return sb.String()
}

// buildInsertSQL builds an INSERT SQL statement and arguments for a row
func buildInsertSQL(schema types.TableSchema, row types.Row) (string, []interface{}) {
	var sb strings.Builder
	
	// Quote identifiers to prevent SQL injection
	sb.WriteString(fmt.Sprintf("INSERT INTO \"%s\" (", schema.Name))
	
	// Get column names
	cols := make([]string, 0, len(row.Values))
	for colName := range row.Values {
		cols = append(cols, colName)
	}
	
	// Add column names
	for i, colName := range cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		// Quote identifiers to prevent SQL injection
		sb.WriteString(fmt.Sprintf("\"%s\"", colName))
	}
	
	sb.WriteString(") VALUES (")
	
	// Add placeholders
	args := make([]interface{}, len(cols))
	for i, colName := range cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("$%d", i+1))
		args[i] = row.Values[colName]
	}
	
	sb.WriteString(")")
	
	return sb.String(), args
}