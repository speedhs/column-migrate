package main

import (
	"database/sql"
	"fmt"
	"time"
)

func runMigration(db *sql.DB, schema, table, column, newType string, batchSize int) error {
	tempColumn := column + "_new"
	funcName := fmt.Sprintf("sync_%s_%s", table, column)
	triggerName := fmt.Sprintf("trg_sync_%s_%s", table, column)

	// Step 1: Add temp column
	if !columnExists(db, schema, table, tempColumn) {
		query := fmt.Sprintf(
			`ALTER TABLE %s.%s ADD COLUMN %s %s;`,
			quote(schema), quote(table), quote(tempColumn), newType,
		)
		execSQL(db, query, "Adding new column")
	} else {
		fmt.Println("Temp column already exists, skipping add.")
	}

	// Step 2: Create sync trigger
	triggerSQL := fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION %s.%s()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.%s := NEW.%s;
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

	execSQL(db, triggerSQL, "Creating trigger for real-time sync")

	// Step 3: Backfill in batches
	fmt.Println("→ Backfilling data in batches...")
	for {
		query := fmt.Sprintf(`
			UPDATE %s.%s
			SET %s = %s
			WHERE %s IS NOT NULL
			AND %s IS NULL
			LIMIT %d;
		`, quote(schema), quote(table), quote(tempColumn), quote(column),
			quote(column), quote(tempColumn), batchSize)

		res, err := db.Exec(query)
		checkFatal(err, "Batch update")

		rows, _ := res.RowsAffected()
		if rows == 0 {
			break
		}
		fmt.Printf("  ↳ Backfilled %d rows...\n", rows)
		time.Sleep(200 * time.Millisecond) // throttle
	}

	// Step 4: Drop trigger
	triggerCleanup := fmt.Sprintf(`
		DROP TRIGGER IF EXISTS %s ON %s.%s;
		DROP FUNCTION IF EXISTS %s.%s();
	`, quote(triggerName), quote(schema), quote(table),
		quote(schema), quote(funcName))

	execSQL(db, triggerCleanup, "Dropping trigger and function")

	// Step 5: Swap columns
	swapSQL := fmt.Sprintf(`
		ALTER TABLE %s.%s DROP COLUMN %s;
		ALTER TABLE %s.%s RENAME COLUMN %s TO %s;
	`, quote(schema), quote(table), quote(column),
		quote(schema), quote(table), quote(tempColumn), quote(column))

	execSQL(db, swapSQL, "Swapping columns")

	fmt.Println("Migration completed successfully.")
	return nil
}
