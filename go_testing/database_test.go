package go_testing

import (
	"database/sql"
	"testing"

	_ "github.com/lib/pq"
)

func TestDatabaseOperations(t *testing.T) {
	db, err := sql.Open("postgres", "postgres://username:password@localhost:5432/testdb?sslmode=disable")
	if err != nil {
		t.Skip("Database not available, skipping integration test")
	}
	defer db.Close()

	// cleanup test table after test
	t.Cleanup(func() {
		_, err := db.Exec("DROP TABLE IF EXISTS test_messages")
		if err != nil {
			t.Logf("Failed to cleanup test table: %v", err)
		}
	})

	err = db.Ping()
	if err != nil {
		t.Fatal("Could not connect to database:", err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS test_messages (
        id SERIAL PRIMARY KEY,
        payload TEXT NOT NULL
    )`)
	if err != nil {
		t.Fatal("Could not create table:", err)
	}

	tests := []struct {
		name    string
		payload string
	}{
		{"simple message", "test payload"},
		{"empty message", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := db.Exec("INSERT INTO test_messages (payload) VALUES ($1)", tc.payload)
			if err != nil {
				t.Errorf("failed to insert: %v", err)
			}
			rows, err := result.RowsAffected()
			if err != nil || rows != 1 {
				t.Errorf("expected 1 row affected, got %d", rows)
			}
		})
	}
}
