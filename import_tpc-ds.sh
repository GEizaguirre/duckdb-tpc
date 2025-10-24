#!/bin/bash


# Path to your DuckDB database file
DB_FILE="tpc-ds-sf1.duckdb"
# Path to the directory containing the .dat files
DATA_DIR="./data-tpc-ds-sf1"
# Path to the DDL file for creating tables
DDL_FILE="tpc-ds.sql"

if [ -f "$DB_FILE" ]; then
    rm "$DB_FILE"
    echo "Removed existing database file: $DB_FILE"
fi

echo "Creating tables"
duckdb "$DB_FILE" < "$DDL_FILE"
echo -e "\e[34mTables created successfully.\e[0m"

COMMAND=""
for f in "$DATA_DIR"/*.dat; do
  TABLE_NAME=$(basename "$f" .dat)
  COMMAND+="COPY $TABLE_NAME FROM '$f' (DELIMITER '|'); "
  echo "Preparing to load $TABLE_NAME..."
done

echo "Starting data load..."
duckdb "$DB_FILE" -c "$COMMAND"

echo -e "\e[32mAll TPC-DS data has been successfully imported into $DB_FILE!\e[0m"

echo "Verifying row count for 'store_sales'..."
duckdb "$DB_FILE" -c "SELECT COUNT(*) FROM store_sales;"
