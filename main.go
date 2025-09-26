package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

func main() {
	// CLI flags
	connStr := flag.String("conn", "", "PostgreSQL connection string")
	schema := flag.String("schema", "public", "Schema name (default: public)")
	table := flag.String("table", "", "Table name to migrate")
	column := flag.String("column", "", "Column name to migrate")
	newType := flag.String("type", "", "New data type (e.g. bigint)")
	batchSize := flag.Int("batch", 1000, "Batch size for backfill")
	pkColumn := flag.String("pk", "", "Primary key column for ordered backfill (optional)")
	dryRun := flag.Bool("dry-run", false, "Print SQL and EXPLAIN, do not execute")
	verbose := flag.Bool("verbose", false, "Verbose logging with timings")

	flag.Parse()

	// Required flag validation
	if *connStr == "" || *table == "" || *column == "" || *newType == "" {
		fmt.Fprintln(os.Stderr, "Error: Missing required flags.\n")
		flag.Usage()
		os.Exit(1)
	}

	// Sanitize type
	sanitizedType, err := sanitizeDataType(*newType)
	checkFatal(err, "Validate new type")

	// Connect to the DB
	fmt.Println("Connecting to database...")
	db, err := sql.Open("postgres", *connStr)
	checkFatal(err, "DB connection")

	defer db.Close()

	err = db.Ping()
	checkFatal(err, "DB ping")

	// Preflight checks
	err = preflight(db, *schema, *table, *column, *pkColumn, sanitizedType, *verbose)
	checkFatal(err, "Preflight checks")

	// Start migration
	err = runMigration(db, *schema, *table, *column, sanitizedType, *batchSize, *pkColumn, *dryRun, *verbose)
	checkFatal(err, "Migration process")
}
