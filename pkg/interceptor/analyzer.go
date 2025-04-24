package interceptor

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/verifiable-postgres/proxy/pkg/types"
)

// Non-deterministic function patterns
var nonDeterministicPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\bNOW\(\s*\)`),
	regexp.MustCompile(`(?i)\bCURRENT_TIMESTAMP\b`),
	regexp.MustCompile(`(?i)\bCURRENT_TIME\b`),
	regexp.MustCompile(`(?i)\bCURRENT_DATE\b`),
	regexp.MustCompile(`(?i)\bRANDOM\(\s*\)`),
	regexp.MustCompile(`(?i)\bGEN_RANDOM_UUID\(\s*\)`),
	regexp.MustCompile(`(?i)\bUUID_GENERATE_V\d\(\s*\)`),
}

// AnalyzeQuery analyzes a SQL query and returns query information
func AnalyzeQuery(sql string) (*types.QueryInfo, error) {
	if sql == "" {
		return nil, fmt.Errorf("empty query")
	}

	queryInfo := &types.QueryInfo{
		SQL:             sql,
		IsDeterministic: true, // Assume deterministic by default
	}

	// Determine query type
	queryInfo.Type = determineQueryType(sql)

	// Extract affected tables
	tables, err := extractTables(sql, queryInfo.Type)
	if err != nil {
		return nil, fmt.Errorf("failed to extract tables: %w", err)
	}
	queryInfo.AffectedTables = tables

	// Check for non-deterministic functions
	isDeterministic, warning := checkDeterminism(sql)
	queryInfo.IsDeterministic = isDeterministic
	queryInfo.Warning = warning

	return queryInfo, nil
}

// determineQueryType determines the type of query
func determineQueryType(sql string) types.QueryType {
	sql = strings.TrimSpace(sql)
	sqlUpper := strings.ToUpper(sql)

	if strings.HasPrefix(sqlUpper, "SELECT") {
		return types.Select
	} else if strings.HasPrefix(sqlUpper, "INSERT") {
		return types.Insert
	} else if strings.HasPrefix(sqlUpper, "UPDATE") {
		return types.Update
	} else if strings.HasPrefix(sqlUpper, "DELETE") {
		return types.Delete
	} else {
		return types.Other
	}
}

// extractTables extracts table names from a SQL query
// Note: This is a simple implementation that doesn't handle all cases
func extractTables(sql string, queryType types.QueryType) ([]string, error) {
	sql = strings.TrimSpace(sql)
	sqlUpper := strings.ToUpper(sql)
	
	tables := make([]string, 0)
	
	switch queryType {
	case types.Select:
		// Basic SELECT FROM table [JOIN table2...]
		fromIdx := strings.Index(sqlUpper, "FROM")
		if fromIdx < 0 {
			return nil, fmt.Errorf("invalid SELECT query, missing FROM clause")
		}
		
		// Extract text after FROM
		afterFrom := sql[fromIdx+4:]
		
		// Find the end of the FROM clause (WHERE, GROUP BY, etc.)
		endIdx := strings.IndexAny(strings.ToUpper(afterFrom), "WHERE GROUP ORDER LIMIT HAVING")
		if endIdx > 0 {
			afterFrom = afterFrom[:endIdx]
		}
		
		// Split by commas and extract table names
		parts := strings.Split(afterFrom, ",")
		for _, part := range parts {
			tableName := extractTableNameFromPart(part)
			if tableName != "" {
				tables = append(tables, tableName)
			}
		}
		
	case types.Insert:
		// INSERT INTO table
		intoIdx := strings.Index(sqlUpper, "INTO")
		if intoIdx < 0 {
			return nil, fmt.Errorf("invalid INSERT query, missing INTO clause")
		}
		
		// Extract text after INTO
		afterInto := sql[intoIdx+4:]
		
		// Find the end of the table name
		endIdx := strings.IndexAny(strings.ToUpper(afterInto), "( VALUES SELECT")
		if endIdx > 0 {
			afterInto = afterInto[:endIdx]
		}
		
		tableName := extractTableNameFromPart(afterInto)
		if tableName != "" {
			tables = append(tables, tableName)
		}
		
	case types.Update:
		// UPDATE table
		updateIdx := 6 // After "UPDATE"
		if updateIdx >= len(sql) {
			return nil, fmt.Errorf("invalid UPDATE query")
		}
		
		// Extract text after UPDATE
		afterUpdate := sql[updateIdx:]
		
		// Find the end of the table name
		endIdx := strings.Index(strings.ToUpper(afterUpdate), "SET")
		if endIdx > 0 {
			afterUpdate = afterUpdate[:endIdx]
		}
		
		tableName := extractTableNameFromPart(afterUpdate)
		if tableName != "" {
			tables = append(tables, tableName)
		}
		
	case types.Delete:
		// DELETE FROM table
		fromIdx := strings.Index(sqlUpper, "FROM")
		if fromIdx < 0 {
			return nil, fmt.Errorf("invalid DELETE query, missing FROM clause")
		}
		
		// Extract text after FROM
		afterFrom := sql[fromIdx+4:]
		
		// Find the end of the table name
		endIdx := strings.Index(strings.ToUpper(afterFrom), "WHERE")
		if endIdx > 0 {
			afterFrom = afterFrom[:endIdx]
		}
		
		tableName := extractTableNameFromPart(afterFrom)
		if tableName != "" {
			tables = append(tables, tableName)
		}
	}
	
	return tables, nil
}

// extractTableNameFromPart extracts a table name from a part of a SQL query
func extractTableNameFromPart(part string) string {
	// Trim whitespace
	part = strings.TrimSpace(part)
	
	// Check for table name with schema
	if strings.Contains(part, ".") {
		parts := strings.Split(part, ".")
		if len(parts) >= 2 {
			return strings.TrimSpace(parts[1])
		}
	}
	
	// Check for table name with alias
	if strings.Contains(part, " ") {
		parts := strings.SplitN(part, " ", 2)
		return strings.TrimSpace(parts[0])
	}
	
	// Simple table name
	return part
}

// checkDeterminism checks if a query contains non-deterministic functions
func checkDeterminism(sql string) (bool, string) {
	for _, pattern := range nonDeterministicPatterns {
		if pattern.MatchString(sql) {
			match := pattern.FindString(sql)
			warning := fmt.Sprintf("Non-deterministic function detected: %s", match)
			return false, warning
		}
	}
	
	return true, ""
}