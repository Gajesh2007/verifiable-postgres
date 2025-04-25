package commitment

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"math"
	"sort"
	"time"

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
	// Get primary key column names and sort them for deterministic ordering
	pkNames := make([]string, 0, len(pkValues))
	for name := range pkValues {
		pkNames = append(pkNames, name)
	}
	sort.Strings(pkNames)

	// Create a hash to build our deterministic ID
	h := sha256.New()

	// Add table name to the hash
	h.Write([]byte(tableName))

	// Process each primary key column in sorted order
	for _, name := range pkNames {
		value := pkValues[name]
		// Add column name
		h.Write([]byte(name))
		
		// Serialize the value based on its type
		serializeValueToHash(h, value)
	}

	// Generate the hash
	hashBytes := h.Sum(nil)
	
	// Return the base64-encoded hash as the row ID, prefixed with table name for readability
	return types.RowID(fmt.Sprintf("%s_%s", tableName, base64.StdEncoding.EncodeToString(hashBytes)[:22]))
}

// serializeValueToHash serializes a value to the hash in a type-specific deterministic way
func serializeValueToHash(h hash.Hash, value interface{}) {
	if value == nil {
		h.Write([]byte("nil"))
		return
	}

	switch v := value.(type) {
	case int:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		h.Write(buf)
	case int64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		h.Write(buf)
	case float64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, math.Float64bits(v))
		h.Write(buf)
	case bool:
		if v {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	case string:
		h.Write([]byte(v))
	case []byte:
		h.Write(v)
	case time.Time:
		// Convert to nanoseconds since epoch for deterministic ordering
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v.UnixNano()))
		h.Write(buf)
	default:
		// For other types, use JSON as a last resort
		jsonData, err := json.Marshal(v)
		if err != nil {
			// If marshaling fails, write a placeholder
			h.Write([]byte("error_serializing"))
		} else {
			h.Write(jsonData)
		}
	}
}