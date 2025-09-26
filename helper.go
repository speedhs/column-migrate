package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"
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

// execSQLWithOpts runs SQL or prints it when dry-run; prints duration when verbose
func execSQLWithOpts(db *sql.DB, query string, context string, dryRun bool, verbose bool) {
	if dryRun {
		fmt.Printf("→ %s (dry-run)\n", context)
		fmt.Println(query)
		return
	}
	if verbose {
		start := time.Now()
		fmt.Printf("→ %s...\n", context)
		_, err := db.Exec(query)
		checkFatal(err, context)
		fmt.Printf("  ↳ done in %s\n", time.Since(start))
		return
	}
	execSQL(db, query, context)
}

// explainQuery prints EXPLAIN output for the given query
func explainQuery(db *sql.DB, query string) {
	rows, err := db.Query("EXPLAIN " + query)
	if err != nil {
		fmt.Printf("  ↳ EXPLAIN error: %v\n", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var plan string
		if err := rows.Scan(&plan); err != nil {
			fmt.Printf("  ↳ EXPLAIN scan error: %v\n", err)
			return
		}
		fmt.Println("  " + plan)
	}
	if err := rows.Err(); err != nil {
		fmt.Printf("  ↳ EXPLAIN rows error: %v\n", err)
	}
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
