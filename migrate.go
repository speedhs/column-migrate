package main

import (
	"database/sql"
	"fmt"
	"time"
)

func runMigration(db *sql.DB, schema, table, column, newType string, batchSize int, pkColumn string, dryRun bool, verbose bool) error {
	tempColumn := column + "_new"
	funcName := fmt.Sprintf("sync_%s_%s", table, column)
	triggerName := fmt.Sprintf("trg_sync_%s_%s", table, column)

	// Step 1: Add temp column
	if !columnExists(db, schema, table, tempColumn) {
		query := fmt.Sprintf(
			`ALTER TABLE %s.%s ADD COLUMN %s %s;`,
			quote(schema), quote(table), quote(tempColumn), newType,
		)
		execSQLWithOpts(db, query, "Adding new column", dryRun, verbose)
	} else {
		fmt.Println("Temp column already exists, skipping add.")
	}

	// Step 2: Create sync trigger
	triggerSQL := fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.%s()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.%s = NEW.%s;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		DROP TRIGGER IF EXISTS %s ON %s.%s;

		CREATE TRIGGER %s
		BEFORE INSERT OR UPDATE ON %s.%s
		FOR EACH ROW EXECUTE FUNCTION %s.%s();
	`, quote(schema), quote(funcName),
		quote(tempColumn), quote(column),
		quote(triggerName), quote(schema), quote(table),
		quote(triggerName), quote(schema), quote(table),
		quote(schema), quote(funcName))

	execSQLWithOpts(db, triggerSQL, "Creating trigger for real-time sync", dryRun, verbose)

	// Step 3: Backfill in batches
	fmt.Println("→ Backfilling data in batches...")
	for {
		var query string
		if pkColumn != "" {
			// Deterministic batches by primary key
			query = fmt.Sprintf(`
				UPDATE %s.%s AS t
				SET %s = t.%s
				FROM (
					SELECT %s
					FROM %s.%s
					WHERE %s IS NOT NULL
					  AND %s IS NULL
					ORDER BY %s
					LIMIT %d
				) AS s
				WHERE t.%s = s.%s;
			`, quote(schema), quote(table),
				quote(tempColumn), quote(column),
				quote(pkColumn),
				quote(schema), quote(table),
				quote(column), quote(tempColumn),
				quote(pkColumn), batchSize,
				quote(pkColumn), quote(pkColumn))
		} else {
			// Fallback: ctid pagination
			query = fmt.Sprintf(`
				UPDATE %s.%s AS t
				SET %s = t.%s
				FROM (
					SELECT ctid
					FROM %s.%s
					WHERE %s IS NOT NULL
					  AND %s IS NULL
					LIMIT %d
				) AS s
				WHERE t.ctid = s.ctid;
			`, quote(schema), quote(table),
				quote(tempColumn), quote(column),
				quote(schema), quote(table),
				quote(column), quote(tempColumn), batchSize)
		}

		if dryRun {
			fmt.Println("→ Backfill batch (dry-run)")
			fmt.Println(query)
			fmt.Println("  EXPLAIN plan:")
			explainQuery(db, query)
			break
		}

		start := time.Now()
		res, err := db.Exec(query)
		checkFatal(err, "Batch update")

		rows, _ := res.RowsAffected()
		if verbose {
			fmt.Printf("  ↳ Batch updated %d rows in %s\n", rows, time.Since(start))
		} else if rows > 0 {
			fmt.Printf("  ↳ Backfilled %d rows...\n", rows)
		}
		if rows == 0 {
			break
		}
		time.Sleep(200 * time.Millisecond) // throttle
	}

	// Step 4: Drop trigger
	triggerCleanup := fmt.Sprintf(`
		DROP TRIGGER IF EXISTS %s ON %s.%s;
		DROP FUNCTION IF EXISTS %s.%s();
	`, quote(triggerName), quote(schema), quote(table),
		quote(schema), quote(funcName))

	execSQLWithOpts(db, triggerCleanup, "Dropping trigger and function", dryRun, verbose)

	// Step 5: Swap columns
	swapSQL := fmt.Sprintf(`
		ALTER TABLE %s.%s DROP COLUMN %s;
		ALTER TABLE %s.%s RENAME COLUMN %s TO %s;
	`, quote(schema), quote(table), quote(column),
		quote(schema), quote(table), quote(tempColumn), quote(column))

	execSQLWithOpts(db, swapSQL, "Swapping columns", dryRun, verbose)

	fmt.Println("Migration completed successfully.")
	return nil
}
