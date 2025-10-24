# TPC-DS Benchmark in DuckDB

This repo contains resources and scripts for running TPC-DS benchmarks using DuckDB, directly using *.dat* TPC-DS tables instead of [DuckDB's native TPC-DS extension](https://duckdb.org/docs/stable/core_extensions/tpcds.html).

## ...btw, why don't we use DuckDB's extension?

Good point. DuckDB uses **parquet** as the default format for its tables, and its integrated TPC-DS data
generation tools are *very* fast.

```sql
INSTALL tpcds;
LOAD tpcds;
CALL dsdgen(sf = 1);
```

However, for some reason (haven't identified it yet) some tables don't contain the same data as the ones generated with the [official TPC-DS tool](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) (in **.dat** format). For instance, for scale factor 1:

#### a) In DuckDB

```sql
SELECT COUNT(*) FROM web_returns;
┌──────────────┐
│ count_star() │
│    int64     │
├──────────────┤
│    71654     │
└──────────────┘
```

#### b) From the official *web_returns* table (with one record per line)

```bash
$ wc -l data-sf1/web_returns.dat 
71763 data-sf1/web_returns.dat
```

Note that the TPC-DS has 109 more records than the DuckDB table.


## What do we use this for
Checking the expected result of a TPC-DS query may not be straightforward. That's exactly what this artifact does: returns the results of TPC-DS locally, in seconds.

> ⚠️ **Warning**: This repository is not official and we give no guarantee on the result correctness (don't have blind faith in this)


## Usage

1. **Clone the repo**  
    ```bash
    git clone https://github.com/your-repo/duckdb-tpc-ds.git
    cd duckdb-tpc-ds-dat
    ```

2. **Install DuckDB**  
    Ensure DuckDB is installed.
    ```bash
    curl https://install.duckdb.org | sh
    ```

3. **Generate TPC-DS tables**  
    Use the provided script to generate the TPC-DS tables.
    ```bash
    .generate_data_tpc-ds.sh
    ```

4. **Create the database**  
    Create the DuckDB database.
    ```bash
    .import_tpc-ds.sh
    ```

5. **Run queries**

    You can either run all queries.
    ```bash
    .run_queries_tpc-ds.sh
    ```
    Or run a specific query.
    ```bash
    .run_queries_tpc-ds.sh 94
    ```

## What's more

We also provide equivalent scripts and resources for TPC-H queries!