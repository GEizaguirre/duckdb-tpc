#!/bin/bash

if [ "$#" -eq 0 ]; then
  echo "No query number provided. Running all queries..."
  QUERY_FILES=("queries-tpc-h/"*.sql)
elif [ "$#" -eq 1 ]; then
  QUERY_NUMBER=$1
  QUERY_FILE="queries-tpc-h/query${QUERY_NUMBER}.sql"

  if [ ! -f "$QUERY_FILE" ]; then
    echo "Error: Query file '$QUERY_FILE' does not exist."
    exit 1
  fi

  QUERY_FILES=("$QUERY_FILE")
else
  echo "Usage: $0 [<query_number>]"
  exit 1
fi

echo "Preparing to run queries..."

DB_FILE="tpc-h-sf1.duckdb"

echo "Listing tables in the database '$DB_FILE'..."
duckdb "$DB_FILE" -c ".tables"


if [ $? -ne 0 ]; then
  echo "Error: Unable to list tables in the database."
  exit 1
fi


for QUERY_FILE in "${QUERY_FILES[@]}"; do
  echo "--- Running query from $QUERY_FILE ---"

  QUERY_STRING=$(sed 's/`/"/g' "$QUERY_FILE")

  duckdb "$DB_FILE" -c "$QUERY_STRING"


  if [ $? -ne 0 ]; then
    echo "Error executing query from $QUERY_FILE. Halting."
    exit 1
  fi
done

echo "✅ All queries executed successfully."
