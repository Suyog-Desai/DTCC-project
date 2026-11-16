"""Download NYC TLC data and land it, untouched, in S3 under raw/.

Usage:
    export AWS_PROFILE=taxi
    export TAXI_BUCKET=your-bucket-name
    python ingestion.py --year 2025 --month 1

"""
import argparse
import os
import sys
import tempfile
import boto3
import requests
from botocore.exceptions import ClientError

#sources locations

BASE = "https://d37ci6vzurychx.cloudfront.net"

def trips_source(year: int, month: int) -> str:
    return f"{BASE}/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

ZONES_SOURCE = f"{BASE}/misc/taxi_zone_lookup.csv"

#S3 destination locations

def trips_key(year: int, month: int) -> str:
    # Hive-style partition folders: year=2025/month=01
    return (
        f"raw/yellow_trips/year={year}/month={month:02d}/yellow_tripdata_{year}-{month:02d}.parquet"
    )

ZONES_KEY = "raw/zone_lookup/taxi_zone_lookup.csv"

#Validation Checks

def source_size(url: str) -> int | None:
    """Size in bytes reported by the source server (HEAD request), or None."""
    r = requests.head(url, allow_redirects=True, timeout=30)
    r.raise_for_status()
    length = r.headers.get("Content-Length")
    return int(length) if length else None

def s3_size(s3, bucket: str, key: str) -> int | None:
    """Size of the object already in S3, or None if it does not exist."""
    try:
        return s3.head_object(Bucket=bucket, Key=key)["ContentLength"]
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise  

def validate(path: str, key: str) -> None:
    """Raise if the downloaded file is not plausibly what we expect.

    Runs BEFORE upload, so a bad download can never overwrite a good object.
    """
    size = os.path.getsize(path)
    if size == 0:
        raise RuntimeError("Downloaded file is empty")
    with open(path, "rb") as f:
        head = f.read(4)
        if key.endswith(".parquet"):
            # Every valid Parquet file starts and ends with the magic bytes PAR1.
            f.seek(-4, os.SEEK_END)
            tail = f.read(4)
            if head != b"PAR1" or tail != b"PAR1":
                raise RuntimeError("Not a valid Parquet file (bad magic bytes)")
        elif key.endswith(".csv"):
            f.seek(0)
            if b"LocationID" not in f.readline():
                raise RuntimeError("Zone lookup CSV is missing its expected header")

#Landing Procedure

def land(s3, bucket: str, url: str, key: str) -> str:
    """Download url and upload to s3://bucket/key unless it is already there."""
    remote = source_size(url)
    existing = s3_size(s3, bucket, key)
    if existing and remote and existing == remote:
        return f"SKIP : s3://{bucket}/{key} (already present, {existing:,} bytes)"

    # Stream to a temp file so a 50 MB parquet never has to sit fully in memory.
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, os.path.basename(key))
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(path,"wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
        size = os.path.getsize(path)
        # Guard against truncated downloads before they land in "raw".
        if remote is not None and size != remote:
            raise RuntimeError(f"Truncated download: got {size}, expected {remote}")
        validate(path, key)  # raises -> nothing is uploaded, old object survives
        if existing and size == existing:
            return f"SKIP   s3://{bucket}/{key} (same size after download, {size:,} bytes)"
        s3.upload_file(path, bucket, key)
    return f"UPLOAD s3://{bucket}/{key} ({size:,} bytes)"

def main() -> int:
    p = argparse.ArgumentParser(description="Download NYC TLC data and land it, untouched, in S3 under raw/")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True, choices=range(1, 13))
    p.add_argument("--bucket", default=os.environ.get("TAXI_BUCKET"))
    args = p.parse_args()

    if not args.bucket:
        print("Set TAXI_BUCKET or pass --bucket", file=sys.stderr)
        return 2

    s3 = boto3.client("s3")  # credentials come from AWS_PROFILE / env, never code
    print(land(s3, args.bucket, trips_source(args.year, args.month),
               trips_key(args.year, args.month)))
    print(land(s3, args.bucket, ZONES_SOURCE, ZONES_KEY))
    return 0


if __name__ == "__main__":
    sys.exit(main())