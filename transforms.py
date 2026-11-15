"""Pure transformation functions for the taxi pipeline.

No file, S3 or catalog access in here: every function takes DataFrames and
returns DataFrames. That is what makes them easy to unit test (Part 5) and
lets the Spark job (job_clean.py) stay a thin wrapper around them.

Flow:  normalize_columns -> add_trip_id -> add_reject_reason
       -> split into clean / rejected
       -> clean: add_metrics -> join_zones
"""
from datetime import date

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def normalize_columns(df: DataFrame) -> DataFrame:
    """Lower-case every column name.

    TLC files are inconsistent between months (Airport_fee vs airport_fee,
    PULocationID, ...). One naming style avoids surprises, and Snowflake
    treats unquoted names case-insensitively anyway.
    """
    return df.toDF(*[c.lower() for c in df.columns])


def add_trip_id(df: DataFrame) -> DataFrame:
    """Add trip_id = SHA-256 of every raw column.

    Taxi data has no primary key. Hashing the whole row gives us one:
      * two rows are exact duplicates  <=>  they get the same trip_id
      * it is the key MERGE INTO uses later, so re-runs don't duplicate rows

    Nulls are replaced by a marker first, because concat_ws silently skips
    nulls, which would let different rows hash to the same value.
    Call this on the raw columns only (before any enrichment).
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in df.columns]
    return df.withColumn("trip_id", F.sha2(F.concat_ws("|", *parts), 256))


def add_reject_reason(df: DataFrame, year: int, month: int) -> DataFrame:
    """Add reject_reason: NULL for a good row, otherwise the FIRST rule it breaks.

    Rule order matters because a row can break several rules but gets one
    label. Order: null_timestamp, dropoff_before_pickup, non_positive_fare,
    out_of_month, exact_duplicate.
    """
    month_start = date(year, month, 1)
    next_month_start = date(year + (month == 12), month % 12 + 1, 1)

    pickup = F.col("tpep_pickup_datetime")
    dropoff = F.col("tpep_dropoff_datetime")
    pickup_day = F.to_date(pickup)

    # 1 for the first copy of a trip_id, 2 for the second, ...
    # Which identical copy is "first" doesn't matter: they are identical.
    copy_number = F.row_number().over(Window.partitionBy("trip_id").orderBy(F.lit(1)))

    reason = (
        F.when(pickup.isNull() | dropoff.isNull(), "null_timestamp")
        .when(dropoff < pickup, "dropoff_before_pickup")
        .when(F.col("fare_amount").isNull() | (F.col("fare_amount") <= 0), "non_positive_fare")
        .when((pickup_day < F.lit(month_start)) | (pickup_day >= F.lit(next_month_start)), "out_of_month")
        .when(F.col("_copy_number") > 1, "exact_duplicate")
    )
    return (
        df.withColumn("_copy_number", copy_number)
        .withColumn("reject_reason", reason)
        .drop("_copy_number")
    )


def add_metrics(df: DataFrame) -> DataFrame:
    """Add trip_minutes, avg_mph, tip_pct and pickup_date.

    Only call on rows that passed validation (fare > 0, dropoff >= pickup).
    avg_mph is NULL for zero-length trips instead of dividing by zero.
    """
    seconds = F.col("_seconds")
    return (
        df.withColumn("_seconds", F.expr("timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime)"))
        .withColumn("trip_minutes", F.round(seconds / 60, 2))
        .withColumn("avg_mph", F.when(seconds > 0, F.round(F.col("trip_distance") / (seconds / 3600), 2)))
        .withColumn("tip_pct", F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2))
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .drop("_seconds")
    )


def join_zones(df: DataFrame, zones: DataFrame) -> DataFrame:
    """Attach pickup/dropoff borough and zone names.

    LEFT joins: trips with an unknown zone id (264, 265, or one missing from
    the lookup) must be kept, with null names, not silently dropped.
    The lookup has ~265 rows, so broadcast it to every executor (no shuffle).
    """
    pickup = F.broadcast(
        zones.select(
            F.col("locationid").alias("pulocationid"),
            F.col("borough").alias("pickup_borough"),
            F.col("zone").alias("pickup_zone"),
        )
    )
    dropoff = F.broadcast(
        zones.select(
            F.col("locationid").alias("dolocationid"),
            F.col("borough").alias("dropoff_borough"),
            F.col("zone").alias("dropoff_zone"),
        )
    )
    return df.join(pickup, "pulocationid", "left").join(dropoff, "dolocationid", "left")


def split_clean_rejected(df: DataFrame, year: int, month: int):
    """Return (clean, rejected). Every input row lands in exactly one of them,
    so raw_count == clean_count + rejected_count."""
    flagged = add_reject_reason(add_trip_id(df), year, month)
    rejected = flagged.filter(F.col("reject_reason").isNotNull())
    clean = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    return clean, rejected


def build_clean(raw: DataFrame, zones: DataFrame, year: int, month: int):
    """Whole transformation: raw trips + zone lookup -> (clean, rejected)."""
    clean, rejected = split_clean_rejected(normalize_columns(raw), year, month)
    clean = join_zones(add_metrics(clean), normalize_columns(zones))
    return clean, rejected


def build_merge_sql(table: str, columns: list, source_view: str = "src") -> str:
    """MERGE statement that inserts only the rows whose trip_id is not in the table yet.

    * The ON clause includes pickup_date, so Iceberg only looks at the partitions
      that appear in the new data instead of scanning the whole table.
    * Insert-only: trip_id is a hash of the whole row, so a matching row is by
      definition identical. There is nothing to update, and skipping UPDATE means
      a re-run rewrites no files.
    * Columns are listed explicitly (not INSERT *), so the load still works after
      the table gains a column the source doesn't have (is_airport_trip in Part 3).
    """
    col_list = ", ".join(columns)
    values = ", ".join(f"s.{c}" for c in columns)
    return (
        f"MERGE INTO {table} t USING {source_view} s "
        f"ON t.pickup_date = s.pickup_date AND t.trip_id = s.trip_id "
        f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({values})"
    )
