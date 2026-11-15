"""Part 3: Iceberg features on trips_clean, plus the daily_zone_metrics table.

    python job_features.py --bucket nyc-taxi-pipeline-124

Run it after job_clean.py. Every step prints its evidence, so keep the output
for the README. Safe to run again.

  1. Schema evolution : ADD COLUMN is_airport_trip (metadata only, no data rewrite)
  2. Backfill         : fill the new column for the existing rows (this one does rewrite)
  3. Time travel      : delete one day, compare row counts across snapshots, roll back
  4. Metrics          : daily_zone_metrics built with MERGE, so a re-run changes nothing
"""
import argparse
import sys

from spark_session import CATALOG, get_spark

DATABASE = "taxi_db"
TRIPS = f"{CATALOG}.{DATABASE}.trips_clean"
METRICS = f"{CATALOG}.{DATABASE}.daily_zone_metrics"


def one(spark, sql):
    return spark.sql(sql).first()[0]


def snapshot_state(spark, table):
    files = one(spark, f"SELECT count(*) FROM {table}.files")
    snaps = one(spark, f"SELECT count(*) FROM {table}.snapshots")
    return files, snaps


def schema_evolution(spark):
    print("\n== 1. Schema evolution: add is_airport_trip ==")
    columns = [f.name for f in spark.table(TRIPS).schema.fields]
    if "is_airport_trip" in columns:
        print("column already exists, nothing to do")
        return
    files_before, snaps_before = snapshot_state(spark, TRIPS)
    spark.sql(f"ALTER TABLE {TRIPS} ADD COLUMN is_airport_trip BOOLEAN")
    files_after, snaps_after = snapshot_state(spark, TRIPS)
    print(f"data files before={files_before} after={files_after}  (same files = nothing rewritten)")
    print(f"snapshots  before={snaps_before} after={snaps_after}  (same = no data commit)")
    nulls = one(spark, f"SELECT count(*) FROM {TRIPS} WHERE is_airport_trip IS NULL")
    print(f"existing rows read the new column as NULL: {nulls}")


def backfill(spark):
    print("\n== 2. Backfill is_airport_trip for existing rows ==")
    spark.sql(f"""
        UPDATE {TRIPS}
        SET is_airport_trip = (lower(coalesce(pickup_zone, '')) LIKE '%airport%'
                            OR lower(coalesce(dropoff_zone, '')) LIKE '%airport%')
        WHERE is_airport_trip IS NULL
    """)
    spark.sql(f"""
        SELECT is_airport_trip, count(*) AS trips FROM {TRIPS}
        GROUP BY is_airport_trip ORDER BY is_airport_trip
    """).show()


def time_travel(spark):
    print("\n== 3. Time travel ==")
    latest = f"SELECT snapshot_id FROM {TRIPS}.snapshots ORDER BY committed_at DESC LIMIT 1"
    before_id = one(spark, latest)
    before_n = one(spark, f"SELECT count(*) FROM {TRIPS}")
    print(f"snapshot {before_id}: {before_n} rows")

    day = one(spark, f"SELECT min(pickup_date) FROM {TRIPS}")
    spark.sql(f"DELETE FROM {TRIPS} WHERE pickup_date = DATE'{day}'")
    after_id = one(spark, latest)
    after_n = one(spark, f"SELECT count(*) FROM {TRIPS}")
    print(f"after deleting {day}, snapshot {after_id}: {after_n} rows")

    old_n = one(spark, f"SELECT count(*) FROM {TRIPS} VERSION AS OF {before_id}")
    print(f"VERSION AS OF {before_id}: {old_n} rows (old snapshot still readable)")

    spark.sql(f"CALL {CATALOG}.system.rollback_to_snapshot('{DATABASE}.trips_clean', {before_id})")
    restored_n = one(spark, f"SELECT count(*) FROM {TRIPS}")
    print(f"after rollback to {before_id}: {restored_n} rows")
    spark.sql(f"""
        SELECT snapshot_id, committed_at, operation FROM {TRIPS}.snapshots
        ORDER BY committed_at
    """).show(truncate=False)


def metrics(spark):
    print("\n== 4. daily_zone_metrics ==")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {METRICS} (
            pickup_date DATE, pickup_zone STRING, trips BIGINT,
            total_revenue DOUBLE, avg_tip_pct DOUBLE, avg_trip_minutes DOUBLE
        ) USING iceberg PARTITIONED BY (pickup_date)
    """)
    spark.sql(f"""
        SELECT pickup_date, pickup_zone,
               count(*)                       AS trips,
               round(sum(total_amount), 2)    AS total_revenue,
               round(avg(tip_pct), 2)         AS avg_tip_pct,
               round(avg(trip_minutes), 2)    AS avg_trip_minutes
        FROM {TRIPS}
        GROUP BY pickup_date, pickup_zone
    """).createOrReplaceTempView("metrics_src")
    spark.sql(f"""
        MERGE INTO {METRICS} t USING metrics_src s
        ON t.pickup_date = s.pickup_date AND t.pickup_zone <=> s.pickup_zone
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    n = one(spark, f"SELECT count(*) FROM {METRICS}")
    print(f"{METRICS}: {n} rows")
    spark.sql(f"SELECT * FROM {METRICS} ORDER BY total_revenue DESC LIMIT 5").show(truncate=False)


def main() -> int:
    p = argparse.ArgumentParser(description="Iceberg features demo + daily_zone_metrics")
    p.add_argument("--bucket", required=True)
    p.add_argument("--region", default="us-east-2")
    p.add_argument("--skip-backfill", action="store_true")
    a = p.parse_args()

    spark = get_spark("glue", bucket=a.bucket, region=a.region)
    schema_evolution(spark)
    if not a.skip_backfill:
        backfill(spark)
    time_travel(spark)
    metrics(spark)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
