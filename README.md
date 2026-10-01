# PySpark Retail Sales ETL

A small ETL pipeline built with PySpark. It reads a messy retail sales CSV, cleans it, computes revenue summaries, and writes the results as Parquet.

## What it does
1. **Extract** – reads `sales_raw.csv` into a Spark DataFrame.
2. **Transform**
   - removes duplicate orders
   - drops rows with missing or negative quantity
   - standardises city names (`" guntur "` becomes `Guntur`)
   - adds `revenue = quantity * unit_price` and a `month` column
   - aggregates revenue by month, category and city
3. **Load** – writes `output/cleaned_sales` and `output/monthly_revenue` as Parquet.

## Run it
Requires Python 3.9+ and Java 8/11/17 (Spark needs Java).

```bash
pip install -r requirements.txt
python generate_data.py   # creates sales_raw.csv
python etl.py
```

On Windows you may also need `winutils` set up for Hadoop.

## Concepts used
DataFrames, `dropDuplicates`, `filter`, `withColumn`, `groupBy` + `agg`, Parquet output.

## Ideas to extend
- Replace the CSV with a database source via JDBC
- Add data-quality checks and a rejected-rows output
- Port it to Databricks and a Delta table
