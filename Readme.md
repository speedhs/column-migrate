# ColumnMigrate

**ColumnMigrate** is a lightweight Go CLI utility for safely migrating a column's data type in PostgreSQL without locking the entire table. It handles large table migrations by avoiding full-table writes and using a batched, trigger-based approach.

## Features

- Zero-downtime column type migration
- Real-time syncing with database triggers
- Safe, batched backfilling
- Column swap without blocking reads or writes

## How It Works

1. Add a temporary column with the new type.
2. Add a trigger to keep the new column in sync with the original column.
3. Backfill existing rows in batches.
4. Drop the trigger and original column.
5. Rename the new column to the original name.

## Usage

```bash
go run main.go \
    -conn "postgres://user:pass@localhost:5432/dbname?sslmode=disable" \
    -table your_table \
    -column column_to_migrate \
    -type new_data_type \
    -batch 1000
```

### Required Flags

| Flag    | Description                        |
|---------|------------------------------------|
| -conn   | PostgreSQL connection string       |
| -table  | Table containing the column        |
| -column | Column to migrate                  |
| -type   | New data type (e.g. `bigint`)      |

### Optional Flags

| Flag    | Description                        | Default |
|---------|------------------------------------|---------|
| -schema | Schema name                        | public  |
| -batch  | Batch size for backfill updates    | 1000    |

### Example

```bash
go run main.go \
    -conn "postgres://postgres:password@localhost:5432/mydb?sslmode=disable" \
    -table users \
    -column age \
    -type bigint
```

## Notes

- Ensure the new type is compatible with existing data.
- Always run on a test environment before production.
- Supports PostgreSQL only.
