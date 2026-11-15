"""Part 5: data quality checks. Prints PASS / FAIL / WARN per check and saves dq_results.csv.

    python dq_checks.py --year 2025 --month 1 --bucket nyc-taxi-pipeline-124

Exit code is 1 if any FAIL, so a scheduler would stop the pipeline.
A WARN is something a person should look at but that does not stop the run.
"""
import argparse
import csv
import sys
from datetime import date

from pyspark.sql import functions as F

from spark_session import CATALOG, get_spark

DATABASE = "taxi_db"
TRIPS = f"{CATALOG}.{DATABASE}.trips_clean"
METRICS = f"{CATALOG}.{DATABASE}.daily_zone_metrics"
REASONS = {"null_timestamp", "dropoff_before_pickup", "non_positive_fare",
           "out_of_month", "exact_duplicate"}


def main() -> int:
    p = argparse.ArgumentParser(description="Data quality checks")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--bucket", required=True)
    p.add_argument("--region", default="us-east-2")
    a = p.parse_args()

    ym = f"year={a.year}/month={a.month:02d}"
    raw_path = f"s3a://{a.bucket}/raw/yellow_trips/{ym}/yellow_tripdata_{a.year}-{a.month:02d}.parquet"
    rejects_path = f"s3a://{a.bucket}/rejects/{ym}/"
    start = date(a.year, a.month, 1)
    end = date(a.year + (a.month == 12), a.month % 12 + 1, 1)

    spark = get_spark("glue", bucket=a.bucket, region=a.region)
    raw = spark.read.parquet(raw_path)
    rejects = spark.read.parquet(rejects_path)
    clean = spark.table(TRIPS)

    results = []

    def record(name, status, detail):
        results.append((name, status, detail))
        print(f"{status:5} {name}: {detail}")

    # 1. Nothing lost: every raw row is either in the clean table or in the rejects
    raw_n, clean_n, rej_n = raw.count(), clean.count(), rejects.count()
    record("raw = clean + rejected",
           "PASS" if raw_n == clean_n + rej_n else "FAIL",
           f"raw={raw_n} clean={clean_n} rejected={rej_n}")

    # 2. Key columns are never null in the clean table
    key_cols = ["trip_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "pickup_date", "fare_amount"]
    nulls = clean.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in key_cols]).first().asDict()
    bad = {c: n for c, n in nulls.items() if n}
    record("no nulls in key columns", "PASS" if not bad else "FAIL", str(bad or "none"))

    # 3. trip_id is unique, so MERGE can rely on it
    dupes = clean.groupBy("trip_id").count().filter("count > 1").count()
    record("trip_id is unique", "PASS" if dupes == 0 else "FAIL", f"{dupes} duplicated ids")

    # 4. The cleaning rules actually hold on what was kept
    broken = clean.filter(
        (F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"))
        | (F.col("fare_amount") <= 0)
        | (F.col("pickup_date") < F.lit(start)) | (F.col("pickup_date") >= F.lit(end))
    ).count()
    record("clean rows obey the reject rules", "PASS" if broken == 0 else "FAIL",
           f"{broken} rows break a rule")

    # 5. Every quarantined row says why it was rejected, and the reason is a known one
    no_reason = rejects.filter(F.col("reject_reason").isNull()).count()
    unknown = rejects.filter(~F.col("reject_reason").isin(*REASONS)).count()
    record("every reject has a valid reject_reason",
           "PASS" if no_reason == 0 and unknown == 0 else "FAIL",
           f"{no_reason} missing, {unknown} unknown")

    # 6. The metrics table adds up to the clean table
    metric_trips = spark.table(METRICS).agg(F.sum("trips")).first()[0]
    record("daily_zone_metrics trips = clean rows",
           "PASS" if metric_trips == clean_n else "FAIL",
           f"metrics={metric_trips} clean={clean_n}")

    # 7. Extreme values: not in the reject rules, so only a warning
    extreme = clean.filter((F.col("fare_amount") > 1000) | (F.col("total_amount") > 1000)
                           | (F.col("trip_distance") > 1000)).count()
    record("no extreme fares or distances (> 1000)", "PASS" if extreme == 0 else "WARN",
           f"{extreme} rows (they distort revenue totals)")

    with open("dq_results.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["check", "status", "detail"])
        w.writerows(results)

    failed = [r for r in results if r[1] == "FAIL"]
    print(f"\n{len(results) - len(failed)} of {len(results)} checks ok, {len(failed)} failed")
    spark.stop()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
