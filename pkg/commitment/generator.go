package commitment

import (
	"encoding/json"
	"sort"

	"github.com/verifiable-postgres/proxy/pkg/types"
)

// GenerateTableRoot generates a Merkle root for a table state
func GenerateTableRoot(tableState *types.TableState) ([32]byte, error) {
	if tableState == nil || len(tableState.Rows) == 0 {
		return [32]byte{}, nil // Empty root for empty table
	}

	// Sort rows deterministically by ID
	rows := make([]types.Row, len(tableState.Rows))
	copy(rows, tableState.Rows)
	sort.Slice(rows, func(i, j int) bool {
		return string(rows[i].ID) < string(rows[j].ID)
	})

	// Serialize each row and create leaf data
	leafData := make([][]byte, len(rows))
	for i, row := range rows {
		// Sort column names for deterministic serialization
		colNames := make([]string, 0, len(row.Values))
		for colName := range row.Values {
			colNames = append(colNames, colName)
		}
		sort.Strings(colNames)

		// Create a sorted map by iterating over sorted keys
		sortedValues := make(map[string]types.Value)
		for _, colName := range colNames {
			sortedValues[colName] = row.Values[colName]
		}

		// Serialize the sorted row
		serializedRow, err := json.Marshal(map[string]interface{}{
			"id":     row.ID,
			"values": sortedValues,
		})
		if err != nil {
			return [32]byte{}, err
		}

		leafData[i] = serializedRow
	}

	// Build the Merkle tree and return the root
	return BuildMerkleTree(leafData)
}

// GenerateDatabaseRoot generates a Merkle root for a database state
func GenerateDatabaseRoot(tableRoots map[string][32]byte) ([32]byte, error) {
	if len(tableRoots) == 0 {
		return [32]byte{}, nil // Empty root for empty database
	}

	// Get table names and sort them
	tableNames := make([]string, 0, len(tableRoots))
	for tableName := range tableRoots {
		tableNames = append(tableNames, tableName)
	}
	sort.Strings(tableNames)

	// Create leaf data for each table
	leafData := make([][]byte, len(tableNames))
	for i, tableName := range tableNames {
		// Combine table name and root
		tableRoot := tableRoots[tableName]
		
		// Create a JSON representation for deterministic serialization
		serialized, err := json.Marshal(map[string]interface{}{
			"name": tableName,
			"root": tableRoot[:],
		})
		if err != nil {
			return [32]byte{}, err
		}
		
		// Use the hash with domain as leaf data
		leafData[i] = serialized
	}

	// Build the Merkle tree and return the root
	return BuildMerkleTree(leafData)
}

// GenerateRowID generates a deterministic row ID from primary key values
func GenerateRowID(tableName string, pkValues map[string]types.Value) types.RowID {
	// Get primary key column names
	pkNames := make([]string, 0, len(pkValues))
	for name := range pkValues {
		pkNames = append(pkNames, name)
	}
	sort.Strings(pkNames)

	// Build a map with sorted keys
	sortedPK := make(map[string]types.Value)
	for _, name := range pkNames {
		sortedPK[name] = pkValues[name]
	}

	// Serialize the primary key values
	serialized, err := json.Marshal(map[string]interface{}{
		"table": tableName,
		"pk":    sortedPK,
	})
	if err != nil {
		// Fallback to a simple string if serialization fails
		return types.RowID(tableName + "_pk_error")
	}

	// Convert to a more readable format (hex or base64)
	return types.RowID(tableName + "_" + string(serialized))
}