from pyspark.sql import functions as F
from spark_session import create_spark_session
from pathlib import Path
from run_identity import get_source_batch_id
from pyspark.sql.window import Window
from run_identity import get_source_batch_id

ROOT=Path(__file__).resolve().parent.parent
SALES_BRONZE_PATH=ROOT/"data"/"bronze"/"sales"
CUSTOMER_BRONZE_PATH=ROOT/"data"/"bronze"/"customers"
SILVER_PATH=ROOT/"data"/"silver"/"sales"
QUARANTINE_PATH=ROOT/"data"/"quarantine"/"sales"



def run_silver(spark,pipeline_run_id):
    source_batch_id=get_source_batch_id(spark,SALES_BRONZE_PATH,pipeline_run_id)
    customer_df=(spark.read.format("delta").load(str(CUSTOMER_BRONZE_PATH)).filter(F.col("source_batch_id") == source_batch_id)
    .filter(F.col("pipeline_run_id") == pipeline_run_id)
    .dropDuplicates(["customer_id"])
    .select("customer_id","customer_name","region","email")
    .withColumn("_customer_found",F.lit(1)))
    sales_df=(spark.read.format("delta").load(str(SALES_BRONZE_PATH)).filter(F.col("source_batch_id") == source_batch_id)
    .filter(F.col("pipeline_run_id") == pipeline_run_id)
    .withColumn("_parsed_date",F.expr("try_cast(order_date as date)"))
    .withColumn("_parsed_amount",F.expr("try_cast(amount as decimal(12,2))"))
    .withColumn("_row_id",F.monotonically_increasing_id()))
    window_function=Window.partitionBy("order_id").orderBy(F.col("ingestedTime").desc(),F.col("_row_id").desc())
    sales_rn_df=sales_df.withColumn("_row_order",F.row_number().over(window_function))
    joined_sales_records=sales_rn_df.join(customer_df,on="customer_id",how="left")
    validating_df = (
    joined_sales_records.withColumn(
        "rejection_reason",
        F.when(
            F.col("_row_order") > 1,
            F.lit("DUPLICATE_ORDER_ID")
        )
        .when(
            F.col("order_id").isNull() |
            (F.trim(F.col("order_id")) == ""),
            F.lit("MISSING_ORDER_ID")
        )
        .when(
            F.col("customer_id").isNull() |
            (F.trim(F.col("customer_id")) == ""),
            F.lit("MISSING_CUSTOMER_ID")
        )
        .when(
            F.col("_parsed_date").isNull(),
            F.lit("INVALID_ORDER_DATE")
        )
        .when(
            F.col("_parsed_amount").isNull(),
            F.lit("INVALID_AMOUNT")
        )
        .when(
            F.col("_parsed_amount") <= 0,
            F.lit("NON_POSITIVE_AMOUNT")
        )
        .when(
            F.col("_customer_found").isNull(),
            F.lit("CUSTOMER_NOT_FOUND")
        )
        .when(
            F.col("region").isNull() |
            (F.trim(F.col("region")) == ""),
            F.lit("CUSTOMER_REGION_MISSING")
        )
    )
)
    valid_df=validating_df.filter(F.col("rejection_reason").isNull())
    invalid_df=validating_df.filter(F.col("rejection_reason").isNotNull())
    silver_final_df = valid_df.select(
    "order_id",
    "customer_id",
    "customer_name",
    "region",
    "email",
    F.col("_parsed_date").alias("order_date"),
    "status",
    F.col("_parsed_amount").alias("amount"),
    "ingestedTime",
    "source_path",
    "source_batch_id",
    "pipeline_run_id"
    )
    quarantine_final_df = invalid_df.select(
    "order_id",
    "customer_id",
    "customer_name",
    "email",
    "region",
    "order_date",
    "amount",
    "_parsed_date",
    "_parsed_amount",
    "rejection_reason",
    "ingestedTime",
    "source_path",
    "source_batch_id",
    "pipeline_run_id"
    )
    #Reconciliation check
    bronze_df_count=sales_df.count()
    silver_final_df_count=silver_final_df.count()
    quarantine_final_df_count=quarantine_final_df.count()
    process_count=silver_final_df_count+quarantine_final_df_count
    reconciliation_status=(process_count==bronze_df_count)
    if not reconciliation_status:
        raise RuntimeError(f"Reconciliation failed: Bronze={bronze_df_count}, "
        f"Silver={silver_final_df_count}, "
        f"Quarantine={quarantine_final_df_count}")
    print("RECONCILIATION PASSED")
    print("Writing silver and quarantine delta")
    silver_final_df.write.format("delta").mode("overwrite").save(str(SILVER_PATH))
    quarantine_final_df.write.format("delta").mode("overwrite").save(str(QUARANTINE_PATH))
    print("Silver pipeline executed successfully")
    print({
    "source_batch_id":source_batch_id,
    "bronze_count":bronze_df_count,
    "silver_count":silver_final_df_count,
    "quarantine_count":quarantine_final_df_count,
    "reconciliation_status":reconciliation_status}
    )
    return {
    "source_batch_id":source_batch_id,
    "bronze_count":bronze_df_count,
    "silver_count":silver_final_df_count,
    "quarantine_count":quarantine_final_df_count,
    "reconciliation_status":reconciliation_status
    }

if __name__=="__main__":
    spark=create_spark_session()
    source_batch_id="20261023_154324_e644c670"
    run_silver(spark,source_batch_id)
    if spark is not None:
        spark.stop()
