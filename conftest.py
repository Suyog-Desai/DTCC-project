"""Loaded by pytest before any test runs.

Spark starts separate Python worker processes. On Windows the plain command
`python` can point at the Microsoft Store shortcut instead of the venv, so the
workers never start. Pointing Spark at the interpreter that is running the
tests avoids that.
"""
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
