package main

import (
	"database/sql"
	"fmt"
	"log"
)

// checkFatal is a utility that logs and exits on fatal errors.
func checkFatal(err error, context string) {
	if err != nil {
		log.Fatalf("Error [%s]: %v", context, err)
	}
}

// execSQL runs an SQL statement and logs it if verbose
func execSQL(db *sql.DB, query string, context string) {
	fmt.Printf("→ %s...\n", context)
	_, err := db.Exec(query)
	checkFatal(err, context)
}

// columnExists checks if a column already exists in the given table
func columnExists(db *sql.DB, schema, table, column string) bool {
	var exists bool
	query := `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = $1
			AND table_name = $2
			AND column_name = $3
		)
	`
	err := db.QueryRow(query, schema, table, column).Scan(&exists)
	if err != nil {
		log.Fatalf("Could not check if column exists: %v", err)
	}
	return exists
}

// quote safely wraps identifiers like column names in double quotes
func quote(identifier string) string {
	return `"` + identifier + `"`
}
