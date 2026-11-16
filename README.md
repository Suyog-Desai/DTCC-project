# NYC Yellow Taxi pipeline (Jan 2025)

A small batch pipeline for the NYC TLC Yellow Taxi data. It lands the raw files in S3, cleans them with PySpark into an Iceberg table, adds a daily metrics table, checks the data, and lets Snowflake query the same Iceberg tables.

Pipeline recording: (https://drive.google.com/file/d/1znl3bXMWHRiRXywUH8kAnAORSVQl3M4B/view?usp=sharing)

```
TLC website
   |  ingestion.py       download, validate, upload to S3 (skips if already there)
   v
S3 raw/                  original files, never modified
   |  job_clean.py       clean, enrich, split good rows from bad rows
   v
Iceberg trips_clean      good rows, partitioned by pickup_date
S3 rejects/              bad rows with a reject_reason
   |  job_features.py    schema change, time travel, daily metrics
   v
Iceberg daily_zone_metrics
   |  dq_checks.py       seven checks, saved to dq_results.csv
   v
Snowflake                reads the Iceberg tables from S3, runs queries.sql
```

## What is in the repo

| File | What it does |
|---|---|
| `ingestion.py` | Part 1. Downloads the trips Parquet and the zone lookup CSV and lands them under `raw/` in S3. |
| `transforms.py` | Part 2. All cleaning logic as small functions: column names, `trip_id`, reject rules, zone join, derived columns, MERGE statement. |
| `job_clean.py` | Part 2. Reads raw, calls `transforms.py`, writes rejects and merges good rows into `trips_clean`. |
| `job_features.py` | Part 3. Schema evolution, time travel, and the `daily_zone_metrics` table. |
| `queries.sql` | Part 4. The three Snowflake queries. |
| `run_queries.py` | Runs the same `queries.sql` in Spark. Used as a cross-check against Snowflake. |
| `dq_checks.py` | Part 5. Data quality checks, writes `dq_results.csv`. |
| `test_transforms.py`, `conftest.py` | Part 5. Unit tests for `transforms.py`. |
| `run_pipeline.py` | Runs ingest, clean, features and checks in order with one command. |
| `spark_session.py` | One place that builds the Spark session and Iceberg catalog settings. |
| `notebooks/` | Exploration of the raw data that led to the reject rules. |
| `results/` | Query results from Snowflake (CSV). |

## How to run it

You need Python 3.12, Java 17, and an AWS profile with access to your own S3 bucket and Glue. I ran it on Windows. Credentials come from the AWS profile, never from code.

```
python -m venv .venv312
.venv312\Scripts\activate
pip install -r requirements.txt

set AWS_PROFILE=taxi
python run_pipeline.py --bucket <your-bucket>
```

Or one step at a time:

```
python ingestion.py --year 2025 --month 1 --bucket <your-bucket>
python job_clean.py --year 2025 --month 1 --bucket <your-bucket>
python job_features.py --bucket <your-bucket>
python dq_checks.py --year 2025 --month 1 --bucket <your-bucket>
pytest -v
python run_queries.py --bucket <your-bucket>
```

Spark prints red `ShutdownHookManager` errors at the end of each job on Windows. Spark is trying to delete its temp folder while a jar is still locked. It happens after the job has finished and does not affect the results.

## Part by part

### Part 1, ingestion

The two source files go to `raw/yellow_trips/year=2025/month=01/` and `raw/zone_lookup/`. Before uploading it checks the file is not empty and is a real Parquet or CSV file, and that the size matches what the server reported, so a broken download cannot overwrite a good file. Running it twice prints `SKIP` for both files. The bucket is private, and the script uses an IAM user rather than the root account.

### Part 2, cleaning

`trips_clean` has the raw columns (names lowercased) plus `trip_id`, `trip_minutes`, `avg_mph`, `tip_pct`, `pickup_date`, pickup and dropoff borough and zone, and later `is_airport_trip`. Every raw row ends up in exactly one place, either the clean table or `rejects/`. A row is rejected for the first of these reasons it breaks, in this order:

1. `null_timestamp`: pickup or dropoff time is missing
2. `dropoff_before_pickup`
3. `non_positive_fare`: fare is missing, zero or negative
4. `out_of_month`: pickup date is outside the month being loaded
5. `exact_duplicate`: every column matches an earlier row

`trip_id` is a SHA-256 hash of all raw columns, so two identical rows get the same id. The job stops without writing anything if raw is not equal to clean plus rejected.

For January 2025: raw 3,475,226, clean 3,329,565, rejected 145,661.

### Part 3, Iceberg features

- Loading uses `MERGE INTO` on `trip_id` and inserts only rows that are not there yet. A re-run adds nothing. The join includes `pickup_date` so Iceberg only looks at the partitions in the new data.
- Adding `is_airport_trip` changed only metadata. Data files stayed at 31 and snapshots at 3, and the 3,329,565 existing rows read the new column as NULL until the backfill filled it (281,469 airport trips, 3,048,096 others).
- Time travel: 3,329,565 rows, then 3,247,178 after deleting 2025-01-01, `VERSION AS OF` the old snapshot still gives 3,329,565, and after `rollback_to_snapshot` the table is back to 3,329,565.
- `daily_zone_metrics` (7,243 rows) is built per day and pickup zone with a MERGE that updates matching rows and inserts new ones, so it is also safe to re-run.

### Part 4, Snowflake

Snowflake reads the Iceberg tables straight from S3 through an external volume (S3 access) and a catalog integration (the Glue catalog), so there is no copy or load step. `queries.sql` answers three questions: top 10 pickup zones by revenue, trips by hour of day, and average tip percent by payment type. Results are in `results/`.

### Part 5, data quality

`dq_checks.py` runs seven checks and writes `dq_results.csv`:

| Check | Result |
|---|---|
| raw = clean + rejected | PASS |
| no nulls in key columns | PASS |
| `trip_id` is unique | PASS |
| clean rows obey the reject rules | PASS |
| every reject has a known `reject_reason` | PASS |
| `daily_zone_metrics` trips = clean rows | PASS |
| no fare, total or distance above 1000 | WARN, 98 rows |

A FAIL makes the script exit with code 1. A WARN does not. `pytest -v` runs the unit tests for the cleaning functions, which build small DataFrames with known bad rows.

## Assumptions and choices

- Glue instead of Polaris. The brief prefers Polaris and accepts Glue. I used the AWS Glue Data Catalog because I could get it working in the time available, and Snowflake can read it directly. With more time I would try Polaris.
- Spark runs locally (`local[*]`), writing to S3. No cluster.
- Revenue is `total_amount`. That is what the passenger paid, so it includes tips, taxes and surcharges. It is not driver or company income.
- Extreme values are kept and flagged. 98 clean rows have a fare, total or distance above 1000, which are almost certainly meter errors. They do not break a reject rule, so the pipeline keeps them and `dq_checks.py` reports a WARN. The revenue totals in `queries.sql` include them. The 1000 threshold is my judgement, not a standard.
- Zones 'N/A' and 'Outside of NYC' are not ranked in the top-zones query because they are not real places.
- Cash tips are recorded as 0 by the taxi meter, since only card tips are captured. The low cash tip percentage means "not recorded", not "tips less".
- Airport trips are matched by "airport" appearing in the pickup or dropoff zone name.
- One month (January 2025) is what I ran. The scripts take `--year` and `--month`, but I have not tested other months.
- The Iceberg demo in `job_features.py` deletes a day and rolls it back on every run, so each run adds a few snapshots. In production this would run once, not on every load.

## Snowflake setup

Setup notes without any account identifiers. Do not commit those.

1. Create a read-only IAM role for Snowflake with `s3:GetObject`, `s3:GetObjectVersion`, `s3:ListBucket` and `s3:GetBucketLocation` on the `warehouse/` prefix only.
2. Create the external volume in Snowflake with `ALLOW_WRITES = FALSE`, `DESC` it, and put the IAM user ARN and external ID it shows into the role's trust policy.
3. Create a second role for the Glue catalog integration with `glue:GetDatabase(s)` and `glue:GetTable(s)` on the `taxi_db` database, and do the same trust policy step with that integration's own external ID.
4. Create the Iceberg tables with `CATALOG = '<integration>'` and `CATALOG_TABLE_NAME = 'trips_clean'` (and `daily_zone_metrics`). A `COUNT(*)` on `trips_clean` should return 3,329,565.

An AWS Organizations policy blocked the first attempts. The AWS account sat under an organization with an AWS-managed resource control policy that denies `sts:*` and `s3:*` to any principal outside the organization. Snowflake runs in a different AWS account, so every `AssumeRole` call failed with "not authorized to perform sts:AssumeRole" even though the role and trust policy were correct. AWS gives the same error whether the role is missing, the trust policy does not match, or an organization policy denies it, which made this hard to find. The fix was an exception for Snowflake's IAM user in that policy, applied in the organization's management account. I will remove that exception when I delete the resources.

## Production notes

- Run the pipeline from a scheduler such as Airflow, and use the exit codes to alert. Today the quality checks run last, so they report a problem after the data is written. A safer design is write-audit-publish: write to a staging table or Iceberg branch, run the checks there, and only publish if they pass.
- Expire old Iceberg snapshots and compact small files on a schedule.
- Pass the month from the scheduler and load one month at a time.
- Run Spark on a managed service (EMR, Glue jobs) instead of a laptop.

## Security

No credentials or keys are in this repo. The AWS profile and any key files stay outside it, and `.gitignore` excludes them along with virtual environments, data files and local Spark warehouses. Cloud resources are deleted after review.
