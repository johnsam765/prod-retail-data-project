from datetime import datetime
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from src.ingestion_ledger import (
    LEDGER_SCHEMA,
    find_successful_ingestion,
)

@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("IngestionLedgerTests")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.ui.enabled", "false")
    )

    session = configure_spark_with_delta_pip(
        builder
    ).getOrCreate()

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def test_missing_ledger_returns_none(spark, tmp_path):
    ledger_path = tmp_path / "missing_ledger"

    result = find_successful_ingestion(
        spark=spark,
        dataset_name="Sales",
        source_path="/data/raw/sales.csv",
        source_sha256="ABC123",
        ledger_path=ledger_path,
    )

    assert result is None
    assert not DeltaTable.isDeltaTable(
        spark, str(ledger_path)
    )


def test_successful_ingestion_lookup(spark, tmp_path):
    ledger_path = tmp_path / "test_ledger"

    records = [
        (
            "Sales",
            "/data/raw/sales.csv",
            "ABC123",
            "BATCH050",
            "RUN100",
            "FAILED",
            19,
            None,
            None,
            datetime(2026, 11, 5, 10, 0),
            datetime(2026, 11, 5, 10, 5),
            "Simulated failure",
        ),
        (
            "Sales",
            "/data/raw/sales.csv",
            "ABC123",
            "BATCH050",
            "RUN101",
            "SUCCESS",
            19,
            19,
            15,
            datetime(2026, 11, 5, 11, 0),
            datetime(2026, 11, 5, 11, 5),
            None,
        ),
    ]

    ledger_df = spark.createDataFrame(
        records,
        schema=LEDGER_SCHEMA,
    )

    (
        ledger_df.write
        .format("delta")
        .mode("errorifexists")
        .save(str(ledger_path))
    )

    # Case 1: Existing successful ingestion
    result = find_successful_ingestion(
        spark,
        "Sales",
        "/data/raw/sales.csv",
        "ABC123",
        ledger_path,
    )

    assert result is not None
    assert result["pipeline_run_id"] == "RUN101"
    assert result["status"] == "SUCCESS"
    assert result["persisted_count"] == 19
    assert result["bronze_delta_version"] == 15

    # Case 2: Modified source fingerprint
    changed_source = find_successful_ingestion(
        spark,
        "Sales",
        "/data/raw/sales.csv",
        "XYZ999",
        ledger_path,
    )

    assert changed_source is None

    # Case 3: Different dataset
    other_dataset = find_successful_ingestion(
        spark,
        "Customers",
        "/data/raw/sales.csv",
        "ABC123",
        ledger_path,
    )

    assert other_dataset is None

    # Case 4: Failed ingestion has no SUCCESS match
    failed_only = find_successful_ingestion(
        spark,
        "Sales",
        "/data/raw/sales.csv",
        "ABC123",
        ledger_path,
    )

    # RUN100 is FAILED, but RUN101 is SUCCESS.
    # Therefore, the successful RUN101 must be returned.
    assert failed_only["pipeline_run_id"] == "RUN101"
