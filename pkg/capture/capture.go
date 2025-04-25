package capture

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/verifiable-postgres/proxy/pkg/log"
	"github.com/verifiable-postgres/proxy/pkg/types"
)

// Interface for database operations (either sql.DB or sql.Tx)
type dbExecutor interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// CapturePreState captures the pre-state of tables affected by a query using a database connection
func CapturePreState(ctx context.Context, queryInfo *types.QueryInfo, db *sql.DB) (map[string][]types.Row, map[string]types.TableSchema, error) {
	return captureState(ctx, queryInfo, db)
}

// CapturePreStateInTx captures the pre-state of tables affected by a query within a transaction
func CapturePreStateInTx(ctx context.Context, queryInfo *types.QueryInfo, tx *sql.Tx) (map[string][]types.Row, map[string]types.TableSchema, error) {
	return captureState(ctx, queryInfo, tx)
}

// CapturePostState captures the post-state of tables affected by a query using a database connection
func CapturePostState(ctx context.Context, queryInfo *types.QueryInfo, db *sql.DB) (map[string][]types.Row, map[string]types.TableSchema, error) {
	// For V1, post-state capture is the same as pre-state capture
	return captureState(ctx, queryInfo, db)
}

// CapturePostStateInTx captures the post-state of tables affected by a query within a transaction
func CapturePostStateInTx(ctx context.Context, queryInfo *types.QueryInfo, tx *sql.Tx) (map[string][]types.Row, map[string]types.TableSchema, error) {
	// For V1, post-state capture is the same as pre-state capture
	return captureState(ctx, queryInfo, tx)
}

// captureState is the generic implementation of state capture that works with both DB and Tx
func captureState(ctx context.Context, queryInfo *types.QueryInfo, executor dbExecutor) (map[string][]types.Row, map[string]types.TableSchema, error) {
	if queryInfo == nil || len(queryInfo.AffectedTables) == 0 {
		return nil, nil, fmt.Errorf("no tables affected")
	}

	// Get schema information for affected tables
	schemas, err := getTableSchemas(ctx, executor, queryInfo.AffectedTables)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get table schemas: %w", err)
	}

	// Capture state for each affected table
	preState := make(map[string][]types.Row)
	for _, tableName := range queryInfo.AffectedTables {
		schema, ok := schemas[tableName]
		if !ok {
			log.Warn("Schema not found for table", "table", tableName)
			continue
		}

		// Capture all rows from the table for simplicity in V1
		// In a real implementation, we would parse the WHERE clause
		rows, err := captureTableState(ctx, executor, schema)
		if err != nil {
			log.Error("Failed to capture table state", "table", tableName, "error", err)
			continue
		}

		preState[tableName] = rows
	}

	return preState, schemas, nil
}

// getTableSchemas gets schema information for the specified tables
func getTableSchemas(ctx context.Context, executor dbExecutor, tableNames []string) (map[string]types.TableSchema, error) {
	schemas := make(map[string]types.TableSchema)

	for _, tableName := range tableNames {
		// Get column information
		query := `
			SELECT 
				column_name, 
				data_type, 
				is_nullable
			FROM 
				information_schema.columns
			WHERE 
				table_name = $1
			ORDER BY 
				ordinal_position
		`
		rows, err := executor.QueryContext(ctx, query, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get column information for table %s: %w", tableName, err)
		}

		var columns []types.ColumnDefinition
		for rows.Next() {
			var columnName, dataType, isNullable string
			if err := rows.Scan(&columnName, &dataType, &isNullable); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan column information: %w", err)
			}

			columns = append(columns, types.ColumnDefinition{
				Name:     columnName,
				Type:     dataType,
				Nullable: isNullable == "YES",
			})
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating column rows: %w", err)
		}

		// Get primary key information
		pkQuery := `
			SELECT 
				kcu.column_name
			FROM 
				information_schema.table_constraints tc
				JOIN information_schema.key_column_usage kcu
					ON tc.constraint_name = kcu.constraint_name
					AND tc.table_schema = kcu.table_schema
			WHERE 
				tc.constraint_type = 'PRIMARY KEY'
				AND tc.table_name = $1
			ORDER BY 
				kcu.ordinal_position
		`
		pkRows, err := executor.QueryContext(ctx, pkQuery, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to get primary key information for table %s: %w", tableName, err)
		}

		var pkColumns []string
		for pkRows.Next() {
			var columnName string
			if err := pkRows.Scan(&columnName); err != nil {
				pkRows.Close()
				return nil, fmt.Errorf("failed to scan primary key information: %w", err)
			}

			pkColumns = append(pkColumns, columnName)
		}
		pkRows.Close()

		if err := pkRows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating primary key rows: %w", err)
		}

		schemas[tableName] = types.TableSchema{
			Name:      tableName,
			Columns:   columns,
			PKColumns: pkColumns,
		}
	}

	return schemas, nil
}

// captureTableState captures the current state of a table
func captureTableState(ctx context.Context, executor dbExecutor, schema types.TableSchema) ([]types.Row, error) {
	// Build query to select all columns
	columnNames := make([]string, len(schema.Columns))
	for i, col := range schema.Columns {
		columnNames[i] = col.Name
	}
	
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(columnNames, ", "), schema.Name)
	
	// Execute query
	rows, err := executor.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query table state: %w", err)
	}
	defer rows.Close()
	
	// Get column names (we already have types from schema)
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error preparing rows: %w", err)
	}

	// Process rows
	var result []types.Row
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))
		
		// Create pointers to scan into
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		
		// Scan row values
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row values: %w", err)
		}
		
		// Create row with values
		row := types.Row{
			Values: make(map[string]types.Value),
		}
		
		// Generate row ID using primary key values
		if len(schema.PKColumns) > 0 {
			pkValues := make(map[string]types.Value)
			for _, pkCol := range schema.PKColumns {
				for i, colName := range columnNames {
					if colName == pkCol {
						pkValues[pkCol] = values[i]
						break
					}
				}
			}
			
			// Create a simple row ID for V1
			// In a real implementation, we would use a more robust method
			idStr := schema.Name + ":"
			for _, pkCol := range schema.PKColumns {
				idStr += fmt.Sprintf("%v:", pkValues[pkCol])
			}
			row.ID = types.RowID(idStr)
		} else {
			// Fallback if no primary key
			row.ID = types.RowID(fmt.Sprintf("%s:row:%d", schema.Name, len(result)))
		}
		
		// Map column names to values
		for i, colName := range columnNames {
			row.Values[colName] = values[i]
		}
		
		result = append(result, row)
	}
	
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}
	
	return result, nil
}