#!/bin/bash


# Path to your DuckDB database file
DB_FILE="tpc-h-sf1.duckdb"
# Path to the directory containing the .dat files
DATA_DIR="./data-tpc-h-sf1"
# Path to the DDL file for creating tables
DDL_FILE="tpc-h.sql"

if [ -f "$DB_FILE" ]; then
    rm "$DB_FILE"
    echo "Removed existing database file: $DB_FILE"
fi

echo "Creating tables"
duckdb "$DB_FILE" < "$DDL_FILE"
echo -e "\e[34mTables created successfully.\e[0m"

load_order=(
  "region"
  "nation"
  "part"
  "supplier"
  "partsupp"
  "customer"
  "orders"
  "lineitem"
)

COMMAND=""
for table in "${load_order[@]}"; do
  f="$DATA_DIR/$table.tbl"
  COMMAND+="COPY $table FROM '$f' (DELIMITER '|'); "
  echo "Preparing to load $table..."
done

echo "Starting data load..."
duckdb "$DB_FILE" -c "$COMMAND"

echo -e "\e[32mAll TPC-H data has been successfully imported into $DB_FILE!\e[0m"

echo "Verifying row count for 'lineitem'..."
duckdb "$DB_FILE" -c "SELECT COUNT(*) FROM lineitem;"
