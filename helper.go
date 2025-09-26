package main

import (
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
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

// sanitizeDataType validates and normalizes a PostgreSQL type expression
func sanitizeDataType(input string) (string, error) {
	s := strings.TrimSpace(input)
	if s == "" {
		return "", fmt.Errorf("empty type")
	}
	// allow only safe characters for type tokens and typmods
	allowed := regexp.MustCompile(`^[A-Za-z0-9_\s\(\),\[\]]+$`)
	if !allowed.MatchString(s) {
		return "", fmt.Errorf("invalid type: contains disallowed characters")
	}
	// collapse whitespace
	space := regexp.MustCompile(`\s+`)
	s = space.ReplaceAllString(s, " ")
	return s, nil
}

// schemaExists checks if a schema exists
func schemaExists(db *sql.DB, schema string) (bool, error) {
	var exists bool
	err := db.QueryRow(`SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name=$1)`, schema).Scan(&exists)
	return exists, err
}

// tableExists checks if a table exists
func tableExists(db *sql.DB, schema, table string) (bool, error) {
	var exists bool
	err := db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, schema+"."+table).Scan(&exists)
	return exists, err
}

// hasAlterPrivilege checks if current user has ALTER privilege on the table
func hasAlterPrivilege(db *sql.DB, schema, table string) (bool, error) {
	var ok bool
	err := db.QueryRow(`SELECT has_table_privilege(($1)::regclass, 'ALTER')`, schema+"."+table).Scan(&ok)
	return ok, err
}

// typeIsValid verifies that PostgreSQL recognizes the base type token
func typeIsValid(db *sql.DB, typeStr string) (bool, error) {
	// strip typmod like (10,2)
	base := regexp.MustCompile(`\s*\([^)]+\)`).ReplaceAllString(typeStr, "")
	base = strings.TrimSpace(base)
	var t string
	err := db.QueryRow(`SELECT to_regtype($1)::text`, base).Scan(&t)
	if err != nil {
		return false, err
	}
	return t != "", nil
}

// pendingBackfillCount returns rows requiring backfill
func pendingBackfillCount(db *sql.DB, schema, table, column string) (int64, error) {
	tempColumn := column + "_new"
	q := fmt.Sprintf(`SELECT count(*) FROM %s.%s WHERE %s IS NOT NULL AND %s IS NULL`,
		quote(schema), quote(table), quote(column), quote(tempColumn))
	var n int64
	err := db.QueryRow(q).Scan(&n)
	return n, err
}

// preflight performs existence checks, privileges, type validity, and shows pending rows
func preflight(db *sql.DB, schema, table, column, pkColumn, newType string, verbose bool) error {
	fmt.Println("Running preflight checks...")
	exists, err := schemaExists(db, schema)
	if err != nil {
		return fmt.Errorf("schema check failed: %w", err)
	}
	if !exists {
		return fmt.Errorf("schema %s does not exist", schema)
	}

	exists, err = tableExists(db, schema, table)
	if err != nil {
		return fmt.Errorf("table check failed: %w", err)
	}
	if !exists {
		return fmt.Errorf("table %s.%s does not exist", schema, table)
	}

	if !columnExists(db, schema, table, column) {
		return fmt.Errorf("column %s not found on %s.%s", column, schema, table)
	}

	if pkColumn != "" && !columnExists(db, schema, table, pkColumn) {
		return fmt.Errorf("pk column %s not found on %s.%s", pkColumn, schema, table)
	}

	ok, err := hasAlterPrivilege(db, schema, table)
	if err != nil {
		return fmt.Errorf("privilege check failed: %w", err)
	}
	if !ok {
		return fmt.Errorf("current user lacks ALTER privilege on %s.%s", schema, table)
	}

	valid, err := typeIsValid(db, newType)
	if err != nil {
		return fmt.Errorf("type check failed: %w", err)
	}
	if !valid {
		return fmt.Errorf("unrecognized type: %s", newType)
	}

	n, err := pendingBackfillCount(db, schema, table, column)
	if err == nil {
		fmt.Printf("Planned rows to backfill: %d\n", n)
	} else if verbose {
		fmt.Printf("Could not compute backfill count: %v\n", err)
	}

	return nil
}
