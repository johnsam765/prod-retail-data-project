from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql.types import (
StructType, StructField, StringType, LongType, TimestampType)
from delta.tables import DeltaTable
from datetime import datetime

ROOT=Path(__file__).resolve().parent.parent
LEDGER_PATH=ROOT/"data"/"audit"/"ingestion_ledger"

LEDGER_SCHEMA=(StructType([
StructField("dataset_name",StringType(),False),
StructField("source_path",StringType(),False),
StructField("source_sha256",StringType(),False),
StructField("source_batch_id",StringType(),False),
StructField("pipeline_run_id",StringType(),False),
StructField("status",StringType(),False),
StructField("expected_count",LongType(),True),
StructField("persisted_count",LongType(),True),
StructField("bronze_delta_version",LongType(),True),
StructField("started_at",TimestampType(),False),
StructField("completed_at",TimestampType(),True),
StructField("error_message",StringType(),True)]))



def find_successful_ingestion(spark,dataset_name,source_path,source_sha256,ledger_path=LEDGER_PATH):
    ledger_path=Path(ledger_path)
    if not DeltaTable.isDeltaTable(spark,str(ledger_path)):
        return None
    ledger_df=(spark.read.format("delta").load(str(ledger_path))
    .filter((F.col("dataset_name")== F.lit(dataset_name))&
    (F.col("source_path") == F.lit(source_path)) &
    (F.col("source_sha256") == F.lit(source_sha256)) &
    (F.col("status") == F.lit("SUCCESS"))).orderBy(F.col("completed_at").desc_nulls_last(),F.col("pipeline_run_id"))
    .limit(1))
    rows=ledger_df.collect()
    return rows[0].asDict(recursive=True) if rows else None



def write_ingestion_record(
    spark,
    record: dict,
    ledger_path=LEDGER_PATH
):
    ledger_path = Path(ledger_path)

    # 1. Validate required identity fields
    required_fields = [
        "dataset_name",
        "source_path",
        "source_sha256",
        "source_batch_id",
        "pipeline_run_id",
    ]

    missing_fields = [
        field for field in required_fields
        if not isinstance(record.get(field), str)
        or not record[field].strip()
    ]

    if missing_fields:
        raise ValueError(
            f"Missing or invalid required fields: {missing_fields}"
        )

    # 2. Validate ingestion status
    if record.get("status") not in {"SUCCESS", "FAILED"}:
        raise ValueError(
            "status must be SUCCESS or FAILED"
        )

    # 3. Validate start timestamp
    if not isinstance(record.get("started_at"), datetime):
        raise ValueError(
            "started_at must be a datetime"
        )

    # 4. Check whether Delta ledger exists
    ledger_exists = DeltaTable.isDeltaTable(
        spark,
        str(ledger_path)
    )

    # 5. Prevent duplicate records for the same attempt
    if ledger_exists:
        existing = (
            spark.read
            .format("delta")
            .load(str(ledger_path))
            .filter(
                (F.col("pipeline_run_id") ==
                 F.lit(record["pipeline_run_id"])) &
                (F.col("dataset_name") ==
                 F.lit(record["dataset_name"])) &
                (F.col("source_path") ==
                 F.lit(record["source_path"]))
            )
            .limit(1)
            .collect()
        )

        if existing:
            raise ValueError(
                "Duplicate ingestion attempt already exists"
            )

    # 6. Create DataFrame using explicit schema
    ledger_df = spark.createDataFrame(
        [record],
        schema=LEDGER_SCHEMA
    )

    # 7. Persist ingestion record
    write_mode = (
        "append" if ledger_exists else "errorifexists"
    )

    (
        ledger_df.write
        .format("delta")
        .mode(write_mode)
        .save(str(ledger_path))
    )
