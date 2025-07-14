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

	flag.Parse()

	// Required flag validation
	if *connStr == "" || *table == "" || *column == "" || *newType == "" {
		fmt.Fprintln(os.Stderr, "Error: Missing required flags.\n")
		flag.Usage()
		os.Exit(1)
	}

	// Connect to the DB
	fmt.Println("Connecting to database...")
	db, err := sql.Open("postgres", *connStr)
	checkFatal(err, "DB connection")

	defer db.Close()

	err = db.Ping()
	checkFatal(err, "DB ping")

	// Start migration
	err = runMigration(db, *schema, *table, *column, *newType, *batchSize)
	checkFatal(err, "Migration process")
}
