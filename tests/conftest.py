"""Shared pytest fixtures for PySpark tests."""
import os
import sys

import pytest
from pyspark.sql import SparkSession

# Pin PySpark's Python workers to the same interpreter running pytest. Without
# this, Spark spawns workers via `python3` on PATH — which on many systems is a
# different Python than the venv, breaking imports (`typing.io`, etc.).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    """
    One SparkSession per test session — starting Spark is expensive (~5s),
    so tests share the same session.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("sports-injury-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
