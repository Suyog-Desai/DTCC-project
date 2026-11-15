"""Part 2 + 3a: raw files -> clean Iceberg table (trips_clean) + quarantined rejects.

    python job_clean.py --year 2025 --month 1 --bucket nyc-taxi-pipeline-124

Safe to run again: new rows are added with MERGE INTO, so a second run adds nothing.
The rejects folder for the month is overwritten, so it is also unchanged by a re-run.
"""
import argparse
import sys

from pyspark import StorageLevel
from pyspark.sql import functions as F

from spark_session import CATALOG, get_spark
from transforms import build_clean, build_merge_sql

DATABASE = "taxi_db"
TABLE = f"{CATALOG}.{DATABASE}.trips_clean"


def main() -> int:
    p = argparse.ArgumentParser(description="Clean raw taxi data into an Iceberg table")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--bucket", help="S3 bucket with raw/ (not needed with --catalog local)")
    p.add_argument("--catalog", choices=["glue", "local"], default="glue")
    p.add_argument("--region", default="us-east-2")
    p.add_argument("--trips", help="override the raw trips path (e.g. a local parquet file)")
    p.add_argument("--zones", help="override the raw zone lookup path")
    p.add_argument("--rejects", help="override where rejected rows are written")
    a = p.parse_args()

    ym = f"year={a.year}/month={a.month:02d}"
    base = f"s3a://{a.bucket}" if a.bucket else None
    trips_path = a.trips or f"{base}/raw/yellow_trips/{ym}/yellow_tripdata_{a.year}-{a.month:02d}.parquet"
    zones_path = a.zones or f"{base}/raw/zone_lookup/taxi_zone_lookup.csv"
    rejects_path = a.rejects or f"{base}/rejects/{ym}/"

    spark = get_spark(a.catalog, bucket=a.bucket, region=a.region)

    # 1. Read the raw layer exactly as it landed
    raw = spark.read.parquet(trips_path)
    zones = spark.read.option("header", True).option("inferSchema", True).csv(zones_path)
    raw_n = raw.count()

    # 2. Clean + enrich (all the logic lives in transforms.py, which has the unit tests)
    clean, rejected = build_clean(raw, zones, a.year, a.month)
    # Used several times below (count, load, write): keep the result on disk, not recompute it.
    clean = clean.persist(StorageLevel.DISK_ONLY)
    rejected = rejected.persist(StorageLevel.DISK_ONLY)
    clean_n, rejected_n = clean.count(), rejected.count()
    print(f"raw={raw_n}  clean={clean_n}  rejected={rejected_n}")
    if raw_n != clean_n + rejected_n:
        print("ERROR: raw != clean + rejected, nothing was written", file=sys.stderr)
        return 1

    # 3. Quarantine: every rejected row, with its reject_reason, one folder per month
    rejected.write.mode("overwrite").parquet(rejects_path)
    print("rejects written to", rejects_path)

    # 4. Load trips_clean (Iceberg), partitioned by pickup_date
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DATABASE}")
    if not spark.catalog.tableExists(TABLE):
        clean.limit(0).writeTo(TABLE).using("iceberg").partitionedBy(F.col("pickup_date")).create()
        print("created table", TABLE)

    clean.createOrReplaceTempView("src")
    spark.sql(build_merge_sql(TABLE, clean.columns))

    total = spark.table(TABLE).count()
    snapshots = spark.sql(f"SELECT count(*) AS n FROM {TABLE}.snapshots").first().n
    print(f"{TABLE}: {total} rows, {snapshots} snapshot(s)")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
