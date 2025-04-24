package tests

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // Import pgx driver
	
	"github.com/verifiable-postgres/proxy/pkg/commitment"
	"github.com/verifiable-postgres/proxy/pkg/types"
)

// Basic test of Merkle tree functionality
func TestMerkleTree(t *testing.T) {
	// Create some test data
	data1 := []byte("test data 1")
	data2 := []byte("test data 2")
	data3 := []byte("test data 3")

	// Build a Merkle tree
	root, err := commitment.BuildMerkleTree([][]byte{data1, data2, data3})
	if err != nil {
		t.Fatalf("Failed to build Merkle tree: %v", err)
	}

	// Ensure root is not empty
	emptyRoot := [32]byte{}
	if root == emptyRoot {
		t.Fatal("Merkle root is empty")
	}

	// Verify that the same data produces the same root
	root2, err := commitment.BuildMerkleTree([][]byte{data1, data2, data3})
	if err != nil {
		t.Fatalf("Failed to build second Merkle tree: %v", err)
	}

	if root != root2 {
		t.Fatalf("Merkle roots don't match: %x != %x", root, root2)
	}

	// Verify that different data produces a different root
	root3, err := commitment.BuildMerkleTree([][]byte{data1, data3, data2})
	if err != nil {
		t.Fatalf("Failed to build third Merkle tree: %v", err)
	}

	if root == root3 {
		t.Fatalf("Merkle roots should be different: %x == %x", root, root3)
	}
}

// Test table state commitment
func TestTableStateCommitment(t *testing.T) {
	// Create a test table state
	tableState := &types.TableState{
		Name: "test_table",
		Schema: types.TableSchema{
			Name: "test_table",
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: "int", Nullable: false},
				{Name: "name", Type: "text", Nullable: true},
			},
			PKColumns: []string{"id"},
		},
		Rows: []types.Row{
			{
				ID: "test_table:1",
				Values: map[string]types.Value{
					"id":   1,
					"name": "Alice",
				},
			},
			{
				ID: "test_table:2",
				Values: map[string]types.Value{
					"id":   2,
					"name": "Bob",
				},
			},
		},
	}

	// Generate table root
	root, err := commitment.GenerateTableRoot(tableState)
	if err != nil {
		t.Fatalf("Failed to generate table root: %v", err)
	}

	// Ensure root is not empty
	emptyRoot := [32]byte{}
	if root == emptyRoot {
		t.Fatal("Table root is empty")
	}

	// Modify a row and verify that the root changes
	tableState2 := &types.TableState{
		Name:   tableState.Name,
		Schema: tableState.Schema,
		Rows: []types.Row{
			{
				ID: "test_table:1",
				Values: map[string]types.Value{
					"id":   1,
					"name": "Alice",
				},
			},
			{
				ID: "test_table:2",
				Values: map[string]types.Value{
					"id":   2,
					"name": "Charlie", // Changed from Bob
				},
			},
		},
	}

	root2, err := commitment.GenerateTableRoot(tableState2)
	if err != nil {
		t.Fatalf("Failed to generate second table root: %v", err)
	}

	if root == root2 {
		t.Fatalf("Table roots should be different: %x == %x", root, root2)
	}
}

// Test database state commitment
func TestDatabaseStateCommitment(t *testing.T) {
	// Create test table roots
	tableRoots := map[string][32]byte{
		"table1": {1, 2, 3},
		"table2": {4, 5, 6},
	}

	// Generate database root
	root, err := commitment.GenerateDatabaseRoot(tableRoots)
	if err != nil {
		t.Fatalf("Failed to generate database root: %v", err)
	}

	// Ensure root is not empty
	emptyRoot := [32]byte{}
	if root == emptyRoot {
		t.Fatal("Database root is empty")
	}

	// Modify a table root and verify that the database root changes
	tableRoots2 := map[string][32]byte{
		"table1": {1, 2, 3},
		"table2": {7, 8, 9}, // Changed from {4, 5, 6}
	}

	root2, err := commitment.GenerateDatabaseRoot(tableRoots2)
	if err != nil {
		t.Fatalf("Failed to generate second database root: %v", err)
	}

	if root == root2 {
		t.Fatalf("Database roots should be different: %x == %x", root, root2)
	}
}

// Integration tests require PostgreSQL to be running
// Skip them unless explicitly enabled
func TestIntegration(t *testing.T) {
	if os.Getenv("ENABLE_INTEGRATION_TESTS") != "1" {
		t.Skip("Integration tests disabled")
	}

	// Test database connection
	db, err := sql.Open("pgx", "host=localhost port=5433 user=postgres password=postgres dbname=postgres sslmode=disable")
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = db.PingContext(ctx)
	if err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	t.Log("Database connection successful")
}