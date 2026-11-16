"""Run the whole pipeline in order with one command.

    python run_pipeline.py --bucket nyc-taxi-pipeline-124

Each step is one of the scripts you can also run by hand. If a step fails,
the pipeline stops there, so later steps never run on bad input.
"""
import argparse
import subprocess
import sys


def main() -> int:
    p = argparse.ArgumentParser(description="Run ingestion, clean, features and DQ checks")
    p.add_argument("--bucket", required=True)
    p.add_argument("--year", type=int, default=2025)
    p.add_argument("--month", type=int, default=1)
    p.add_argument("--region", default="us-east-2")
    a = p.parse_args()

    ym = ["--year", str(a.year), "--month", str(a.month)]
    common = ["--bucket", a.bucket, "--region", a.region]

    # NOTE: check the flags of ingestion.py on your machine and edit this line if they differ.
    steps = [
        ("1 ingest", ["ingestion.py", "--bucket", a.bucket] + ym),
        ("2 clean", ["job_clean.py"] + common + ym),
        ("3 features", ["job_features.py"] + common),
        ("4 dq checks", ["dq_checks.py"] + common + ym),
    ]

    for name, args in steps:
        print(f"\n=== step {name} ===", flush=True)
        code = subprocess.call([sys.executable] + args)
        if code != 0:
            print(f"step '{name}' failed with exit code {code}, stopping.")
            return code

    print("\npipeline finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
