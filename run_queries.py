"""Part 4 (Spark version): run the queries in queries.sql and save each result as a CSV.

    python run_queries.py --bucket nyc-taxi-pipeline-124

The same queries.sql is meant for Snowflake (table taxi_sf.lake.trips_clean). Here the table
name is swapped for the Glue-registered Iceberg table, lake.taxi_db.trips_clean, and nothing
else changes, so the two engines can be compared on identical SQL.

Results go to results/<query name>.csv and are printed to the console.
"""
import argparse
import csv
import re
import sys
from pathlib import Path

from spark_session import CATALOG, get_spark

SNOWFLAKE_TABLE = "taxi_sf.lake.trips_clean"
SPARK_TABLE = f"{CATALOG}.taxi_db.trips_clean"


def load_queries(path: Path) -> dict:
    """Split queries.sql into {name: sql}. A query starts at a '-- name: ...' line and ends
    at its first semicolon. Text before the first marker (the file header) is ignored."""
    text = path.read_text(encoding="utf-8")
    queries = {}
    for block in re.split(r"^-- name:\s*", text, flags=re.MULTILINE)[1:]:
        name, _, body = block.partition("\n")
        queries[name.strip()] = body.split(";")[0].strip()
    return queries


def save_csv(columns, rows, out_file: Path) -> None:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(columns)
        writer.writerows(rows)


def run(spark, queries: dict, table: str, out_dir: Path) -> None:
    for name, sql in queries.items():
        df = spark.sql(sql.replace(SNOWFLAKE_TABLE, table))
        # collect() instead of toPandas(): no pandas needed, and the rows are tiny
        rows = [tuple(r) for r in df.collect()]
        save_csv(df.columns, rows, out_dir / f"{name}.csv")
        print(f"\n== {name} ({len(rows)} rows) -> {out_dir / (name + '.csv')}")
        df.show(30, truncate=False)


def main() -> int:
    p = argparse.ArgumentParser(description="Run queries.sql on the Iceberg table with Spark")
    p.add_argument("--bucket", required=True)
    p.add_argument("--region", default="us-east-2")
    p.add_argument("--sql", default="queries.sql")
    p.add_argument("--out", default="results")
    a = p.parse_args()

    queries = load_queries(Path(a.sql))
    if not queries:
        print(f"no '-- name:' queries found in {a.sql}")
        return 1

    spark = get_spark("glue", bucket=a.bucket, region=a.region)
    run(spark, queries, SPARK_TABLE, Path(a.out))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
