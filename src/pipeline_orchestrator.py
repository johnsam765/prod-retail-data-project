from spark_session import create_spark_session
from datetime import datetime, timezone
from bronze_pipeline import run_bronze
from silver_pipeline import run_silver
from gold_pipeline import run_gold
from pipeline_audit import write_pipeline_audit
from pathlib import Path
from run_identity import get_source_batch_id,generate_pipeline_run_id

ROOT=Path(__file__).resolve().parent.parent
SALES_BRONZE_PATH=ROOT/"data"/"bronze"/"sales"


def run_pipeline():
    pipeline_run_id=generate_pipeline_run_id()
    started_at=datetime.now(timezone.utc)
    current_stage="INITIALIZATION"
    spark=None
    silver_result=None
    gold_result=None
    audit_record={
    "pipeline_name":"Retail Prod Pipeline",
    "pipeline_run_id":pipeline_run_id,
    "source_batch_id":None,
    "status":"RUNNING",
    "failed_stage":None,
    "bronze_count":None,
    "silver_count":None,
    "quarantine_count":None,
    "gold_count":None,
    "reconciliation_passed":None,
    "error_message":None,
    "started_at":started_at,
    "completed_at":None
    }
    try:
        spark=create_spark_session()
        current_stage="BATCH_VALIDATION(BRONZE)"
        sales_bronze_count,customers_bronze_count=run_bronze(spark,pipeline_run_id)
        source_batch_id=get_source_batch_id(spark,SALES_BRONZE_PATH,pipeline_run_id)
        audit_record["bronze_count"]=sales_bronze_count
        print("Bronze layer ran successfully")
        current_stage="DATA QUALITY(SILVER)"
        silver_result=run_silver(spark,pipeline_run_id)
        if silver_result["source_batch_id"]!=source_batch_id:
            raise RuntimeError("Source batch ID mismatch")
        audit_record["silver_count"]=silver_result["silver_count"]
        audit_record["quarantine_count"]=silver_result["quarantine_count"]
        audit_record["reconciliation_passed"]=silver_result["reconciliation_status"]
        current_stage="BUSINESS REQUIREMENT(GOLD)"
        gold_result=run_gold(spark,source_batch_id)
        audit_record["gold_count"]=gold_result["gold_count"]
        audit_record["status"] = "SUCCESS"

    except Exception as error:
        audit_record["status"]="Failed"
        audit_record["failed_stage"]=current_stage
        audit_record["error_message"]=str(error)
        audit_record["completed_at"]=datetime.now(timezone.utc)
        print(f"PIPELINE FAILED AT: {current_stage}")
        print(f"ERROR: {error}")
        if spark is not None:
            try:
                write_pipeline_audit(spark,audit_record)
            except Exception as audit_error:
                print(f"CRITICAL AUDIT FAILURE: {audit_error}")
        else:
            print("AUDIT UNAVAILABLE: Spark initialization failed")
        raise

    else:
        audit_record["completed_at"] = datetime.now(timezone.utc)
        try:
            write_pipeline_audit(spark, audit_record)
        except Exception as audit_error:
            print(f"AUDIT FAILURE AFTER PROCESSING: {audit_error}")
            raise
        print("PIPELINE SUCCESS — AUDIT PERSISTED")

    finally:
        if spark is not None:
            spark.stop()

if __name__=="__main__":
    run_pipeline()














