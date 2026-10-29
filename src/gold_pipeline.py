from spark_session import create_spark_session
from pyspark.sql import functions as F
from pathlib import Path

ROOT=Path(__file__).resolve().parent.parent
SILVER_SALES_PATH=ROOT/"data"/"silver"/"sales"
GOLD_SALES_PATH=ROOT/"data"/"gold"/"sales"


def run_gold(spark,source_batch_id,source_version=None,output_path=None):
    if source_version is not None and output_path is None:
        raise RuntimeError("Historical gold recovery requires explicit output path")
    target_path = (
        Path(output_path)
        if output_path is not None
        else GOLD_SALES_PATH
    )
    sales_reader=spark.read.format("delta")
    if source_version is not None:
        sales_reader=sales_reader.option("versionAsOf",source_version)

    sales_df = (
        sales_reader
        .load(str(SILVER_SALES_PATH))
        .filter(F.col("source_batch_id") == source_batch_id)
    )

    if sales_df.limit(1).count()==0:
        raise RuntimeError(f"No Silver records found for batch "
            f"{source_batch_id} at version {source_version}")

    gold_df = (
        sales_df
        .filter(F.col("status") == "Completed")
        .groupBy("region")
        .agg(
            F.count("*").alias("completed_sales_count"),
            F.sum("amount").alias("total_sales_amount"),
            F.avg("amount")
                .cast("decimal(12,2)")
                .alias("average_sales_amount")
        )
        .withColumn(
            "source_batch_id",
            F.lit(source_batch_id)
        )
    )

    gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(str(target_path))

    persisted_gold_df = (
        spark.read.format("delta")
        .load(str(target_path))
    )

    gold_count = persisted_gold_df.count()

    print("\nGOLD REGIONAL SALES:")
    persisted_gold_df.show(truncate=False)

    return {
        "source_batch_id": source_batch_id,
        "gold_count": gold_count
    }

if __name__=="__main__":
    spark=create_spark_session()
    source_batch_id="20261024_042204_ab58a267"
    run_gold(spark,source_batch_id,8,ROOT/"data"/"gold"/"recovery_test")
    if spark is not None:
        spark.stop()
