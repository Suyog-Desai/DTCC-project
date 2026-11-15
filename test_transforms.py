"""Unit tests for transforms.py. Run:  pytest -v"""
import json
import tempfile
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

from transforms import (
    add_metrics, build_merge_sql, add_trip_id, build_clean, join_zones, normalize_columns,
    split_clean_rejected,
)

SCHEMA = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("trip_distance", DoubleType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("Airport_fee", DoubleType()),  # mixed case on purpose
])


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[1]").appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def trip(**overrides):
    """One valid January-2025 trip; override any field to break a rule."""
    row = dict(
        VendorID=1,
        tpep_pickup_datetime=datetime(2025, 1, 5, 8, 0),
        tpep_dropoff_datetime=datetime(2025, 1, 5, 8, 30),
        trip_distance=6.0, PULocationID=132, DOLocationID=161,
        payment_type=1, fare_amount=12.0, tip_amount=3.0,
        total_amount=18.0, Airport_fee=0.0,
    )
    row.update(overrides)
    return tuple(row[f.name] for f in SCHEMA.fields)


def make_df(spark, rows, schema):
    """Build a DataFrame without a Python worker.

    spark.createDataFrame(python_rows) needs Spark's Python worker process,
    which crashes on some Windows setups. Writing JSON lines and reading them
    back with the schema only uses the JVM, so it works everywhere.
    """
    folder = Path(tempfile.mkdtemp(prefix="spark_test_"))
    names = [f.name for f in schema.fields]
    with open(folder / "data.json", "w", encoding="utf-8") as fh:
        for row in rows:
            record = {n: (v.isoformat() if isinstance(v, datetime) else v)
                      for n, v in zip(names, row)}
            fh.write(json.dumps(record) + "\n")
    return spark.read.schema(schema).json(str(folder / "data.json"))


def trips_df(spark, *rows):
    return make_df(spark, list(rows), SCHEMA)


def split(spark, *rows):
    return split_clean_rejected(normalize_columns(trips_df(spark, *rows)), 2025, 1)


@pytest.fixture
def zones(spark):
    schema = StructType([
        StructField("LocationID", LongType()),
        StructField("Borough", StringType()),
        StructField("Zone", StringType()),
        StructField("service_zone", StringType()),
    ])
    return make_df(
        spark,
        [(132, "Queens", "JFK Airport", "Airports"),
         (161, "Manhattan", "Midtown Center", "Yellow Zone")],
        schema,
    )


# ---- validation rules -------------------------------------------------------

def test_valid_row_is_kept(spark):
    clean, rejected = split(spark, trip())
    assert clean.count() == 1 and rejected.count() == 0


@pytest.mark.parametrize("overrides, expected_reason", [
    (dict(tpep_pickup_datetime=None), "null_timestamp"),
    (dict(tpep_dropoff_datetime=None), "null_timestamp"),
    (dict(tpep_dropoff_datetime=datetime(2025, 1, 5, 7, 0)), "dropoff_before_pickup"),
    (dict(fare_amount=0.0), "non_positive_fare"),
    (dict(fare_amount=-5.0), "non_positive_fare"),
    (dict(fare_amount=None), "non_positive_fare"),
    (dict(tpep_pickup_datetime=datetime(2008, 12, 31, 23, 0),
          tpep_dropoff_datetime=datetime(2008, 12, 31, 23, 30)), "out_of_month"),
    (dict(tpep_pickup_datetime=datetime(2025, 2, 1, 0, 0),
          tpep_dropoff_datetime=datetime(2025, 2, 1, 0, 20)), "out_of_month"),
])
def test_bad_rows_are_rejected_with_reason(spark, overrides, expected_reason):
    clean, rejected = split(spark, trip(**overrides))
    assert clean.count() == 0
    assert [r.reject_reason for r in rejected.collect()] == [expected_reason]


def test_month_boundaries(spark):
    first = trip(tpep_pickup_datetime=datetime(2025, 1, 1, 0, 0),
                 tpep_dropoff_datetime=datetime(2025, 1, 1, 0, 10))
    last = trip(tpep_pickup_datetime=datetime(2025, 1, 31, 23, 59),
                tpep_dropoff_datetime=datetime(2025, 2, 1, 0, 20))
    clean, rejected = split(spark, first, last)
    assert clean.count() == 2 and rejected.count() == 0


def test_exact_duplicate_keeps_one_copy(spark):
    clean, rejected = split(spark, trip(), trip())
    assert clean.count() == 1
    assert [r.reject_reason for r in rejected.collect()] == ["exact_duplicate"]


def test_near_duplicate_is_not_a_duplicate(spark):
    clean, rejected = split(spark, trip(), trip(fare_amount=12.5))
    assert clean.count() == 2 and rejected.count() == 0


def test_rows_reconcile(spark):
    rows = [trip(), trip(), trip(fare_amount=0.0), trip(tpep_pickup_datetime=None),
            trip(fare_amount=20.0)]
    clean, rejected = split(spark, *rows)
    assert clean.count() + rejected.count() == len(rows)


# ---- trip_id ----------------------------------------------------------------

def test_trip_id_same_for_identical_rows_different_otherwise(spark):
    df = add_trip_id(normalize_columns(trips_df(spark, trip(), trip(), trip(fare_amount=99.0))))
    ids = [r.trip_id for r in df.collect()]
    assert ids[0] == ids[1] and ids[0] != ids[2]


def test_trip_id_distinguishes_null_positions(spark):
    # concat_ws would skip nulls and hash these the same without the marker
    a = trip(tip_amount=None, Airport_fee=1.0)
    b = trip(tip_amount=1.0, Airport_fee=None)
    ids = [r.trip_id for r in add_trip_id(normalize_columns(trips_df(spark, a, b))).collect()]
    assert ids[0] != ids[1]


# ---- enrichment -------------------------------------------------------------

def test_metrics_values(spark):
    row = add_metrics(normalize_columns(trips_df(spark, trip()))).collect()[0]
    assert row.trip_minutes == 30.0
    assert row.avg_mph == 12.0          # 6 miles in half an hour
    assert row.tip_pct == 25.0          # 3 / 12
    assert str(row.pickup_date) == "2025-01-05"


def test_zero_minute_trip_has_null_mph(spark):
    same = datetime(2025, 1, 5, 8, 0)
    df = trips_df(spark, trip(tpep_pickup_datetime=same, tpep_dropoff_datetime=same))
    row = add_metrics(normalize_columns(df)).collect()[0]
    assert row.trip_minutes == 0.0 and row.avg_mph is None


def test_zone_join_adds_names(spark, zones):
    df = normalize_columns(trips_df(spark, trip()))
    row = join_zones(df, normalize_columns(zones)).collect()[0]
    assert (row.pickup_zone, row.pickup_borough) == ("JFK Airport", "Queens")
    assert (row.dropoff_zone, row.dropoff_borough) == ("Midtown Center", "Manhattan")


def test_unknown_zone_is_kept_with_null_name(spark, zones):
    df = normalize_columns(trips_df(spark, trip(DOLocationID=264)))
    out = join_zones(df, normalize_columns(zones))
    assert out.count() == 1 and out.collect()[0].dropoff_zone is None


# ---- plumbing ---------------------------------------------------------------

def test_normalize_columns_lowercases(spark):
    cols = normalize_columns(trips_df(spark, trip())).columns
    assert "airport_fee" in cols and "pulocationid" in cols
    assert all(c == c.lower() for c in cols)


def test_build_clean_end_to_end(spark, zones):
    raw = trips_df(spark, trip(), trip(), trip(fare_amount=0.0))
    clean, rejected = build_clean(raw, zones, 2025, 1)
    assert clean.count() == 1 and rejected.count() == 2
    assert {"trip_id", "trip_minutes", "avg_mph", "tip_pct", "pickup_date",
            "pickup_zone", "dropoff_zone"} <= set(clean.columns)
    assert "reject_reason" in rejected.columns


def test_merge_sql_is_insert_only_and_prunes_by_partition():
    sql = build_merge_sql("lake.taxi_db.trips_clean", ["trip_id", "pickup_date", "fare_amount"])
    assert "WHEN NOT MATCHED THEN INSERT (trip_id, pickup_date, fare_amount)" in sql
    assert "VALUES (s.trip_id, s.pickup_date, s.fare_amount)" in sql
    assert "t.pickup_date = s.pickup_date AND t.trip_id = s.trip_id" in sql
    assert "WHEN MATCHED" not in sql
