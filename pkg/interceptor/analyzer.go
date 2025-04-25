package interceptor

import (
	"fmt"
	"regexp"

	"github.com/blastrain/vitess-sqlparser/sqlparser"
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

	// Parse the SQL query using vitess parser
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL query: %w", err)
	}

	// Determine query type and extract affected tables
	tables, queryType, err := extractTablesFromAST(stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to extract tables: %w", err)
	}

	queryInfo.Type = queryType
	queryInfo.AffectedTables = tables

	// Check for non-deterministic functions
	isDeterministic, warning := checkDeterminism(sql)
	queryInfo.IsDeterministic = isDeterministic
	queryInfo.Warning = warning

	return queryInfo, nil
}

// extractTablesFromAST extracts tables from the SQL AST and determines the query type
func extractTablesFromAST(stmt sqlparser.Statement) ([]string, types.QueryType, error) {
	tableMap := make(map[string]struct{}) // Use map to ensure uniqueness
	var queryType types.QueryType

	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		queryType = types.Select
		extractTablesFromSelectStatement(stmt, tableMap)

	case *sqlparser.Insert:
		queryType = types.Insert
		extractTablesFromInsertStatement(stmt, tableMap)

	case *sqlparser.Update:
		queryType = types.Update
		extractTablesFromUpdateStatement(stmt, tableMap)

	case *sqlparser.Delete:
		queryType = types.Delete
		extractTablesFromDeleteStatement(stmt, tableMap)

	default:
		queryType = types.Other
	}

	// Convert map to slice
	tables := make([]string, 0, len(tableMap))
	for table := range tableMap {
		tables = append(tables, table)
	}

	return tables, queryType, nil
}

// extractTablesFromSelectStatement extracts tables from a SELECT statement
func extractTablesFromSelectStatement(stmt *sqlparser.Select, tableMap map[string]struct{}) {
	// Extract tables from FROM clause
	extractTablesFromTableExprs(stmt.From, tableMap)

	// Extract tables from WHERE clause
	if stmt.Where != nil {
		extractTablesFromExpr(stmt.Where.Expr, tableMap)
	}

	// Extract tables from JOIN conditions
	for _, join := range extractJoins(stmt.From) {
		if join.On != nil {
			extractTablesFromExpr(join.On, tableMap)
		}
	}
}

// extractTablesFromInsertStatement extracts tables from an INSERT statement
func extractTablesFromInsertStatement(stmt *sqlparser.Insert, tableMap map[string]struct{}) {
	// Get the target table
	tableName := getFullTableName(stmt.Table)
	tableMap[tableName] = struct{}{}

	// If INSERT ... SELECT, also extract tables from the SELECT part
	if selectStmt, ok := stmt.Rows.(*sqlparser.Select); ok {
		extractTablesFromSelectStatement(selectStmt, tableMap)
	}
}

// extractTablesFromUpdateStatement extracts tables from an UPDATE statement
func extractTablesFromUpdateStatement(stmt *sqlparser.Update, tableMap map[string]struct{}) {
	// Extract the table being updated
	for _, tableExpr := range stmt.TableExprs {
		extractTablesFromTableExpr(tableExpr, tableMap)
	}

	// Extract tables from WHERE clause
	if stmt.Where != nil {
		extractTablesFromExpr(stmt.Where.Expr, tableMap)
	}
}

// extractTablesFromDeleteStatement extracts tables from a DELETE statement
func extractTablesFromDeleteStatement(stmt *sqlparser.Delete, tableMap map[string]struct{}) {
	// Extract the table being deleted from
	for _, tableExpr := range stmt.TableExprs {
		extractTablesFromTableExpr(tableExpr, tableMap)
	}

	// Extract tables from WHERE clause
	if stmt.Where != nil {
		extractTablesFromExpr(stmt.Where.Expr, tableMap)
	}
}

// extractTablesFromTableExprs extracts tables from a list of table expressions
func extractTablesFromTableExprs(tableExprs sqlparser.TableExprs, tableMap map[string]struct{}) {
	for _, tableExpr := range tableExprs {
		extractTablesFromTableExpr(tableExpr, tableMap)
	}
}

// extractTablesFromTableExpr extracts tables from a single table expression
func extractTablesFromTableExpr(tableExpr sqlparser.TableExpr, tableMap map[string]struct{}) {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch expr := tableExpr.Expr.(type) {
		case sqlparser.TableName:
			tableName := getFullTableName(expr)
			tableMap[tableName] = struct{}{}
		case *sqlparser.Subquery:
			if selectStmt, ok := expr.Select.(*sqlparser.Select); ok {
				extractTablesFromSelectStatement(selectStmt, tableMap)
			}
		}
	case *sqlparser.JoinTableExpr:
		extractTablesFromTableExpr(tableExpr.LeftExpr, tableMap)
		extractTablesFromTableExpr(tableExpr.RightExpr, tableMap)
		if tableExpr.On != nil {
			extractTablesFromExpr(tableExpr.On, tableMap)
		}
	case *sqlparser.ParenTableExpr:
		extractTablesFromTableExprs(tableExpr.Exprs, tableMap)
	}
}

// extractTablesFromExpr extracts tables from expressions (like WHERE conditions)
func extractTablesFromExpr(expr sqlparser.Expr, tableMap map[string]struct{}) {
	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
		extractTablesFromExpr(expr.Left, tableMap)
		extractTablesFromExpr(expr.Right, tableMap)
	case *sqlparser.OrExpr:
		extractTablesFromExpr(expr.Left, tableMap)
		extractTablesFromExpr(expr.Right, tableMap)
	case *sqlparser.ComparisonExpr:
		extractTablesFromExpr(expr.Left, tableMap)
		extractTablesFromExpr(expr.Right, tableMap)
	case *sqlparser.Subquery:
		if selectStmt, ok := expr.Select.(*sqlparser.Select); ok {
			extractTablesFromSelectStatement(selectStmt, tableMap)
		}
	}
	// Add more cases as needed for complex expressions
}

// extractJoins extracts join expressions from table expressions
func extractJoins(tableExprs sqlparser.TableExprs) []*sqlparser.JoinTableExpr {
	var joins []*sqlparser.JoinTableExpr
	for _, tableExpr := range tableExprs {
		if join, ok := tableExpr.(*sqlparser.JoinTableExpr); ok {
			joins = append(joins, join)
			// Recursively extract joins from nested join expressions
			if joinExprs := extractJoins(sqlparser.TableExprs{join.LeftExpr}); len(joinExprs) > 0 {
				joins = append(joins, joinExprs...)
			}
			if joinExprs := extractJoins(sqlparser.TableExprs{join.RightExpr}); len(joinExprs) > 0 {
				joins = append(joins, joinExprs...)
			}
		} else if paren, ok := tableExpr.(*sqlparser.ParenTableExpr); ok {
			if joinExprs := extractJoins(paren.Exprs); len(joinExprs) > 0 {
				joins = append(joins, joinExprs...)
			}
		}
	}
	return joins
}

// getFullTableName returns the full table name including schema if present
func getFullTableName(tableName sqlparser.TableName) string {
	if tableName.Qualifier.IsEmpty() {
		return tableName.Name.String()
	}
	return fmt.Sprintf("%s.%s", tableName.Qualifier.String(), tableName.Name.String())
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