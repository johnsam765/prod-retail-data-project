from spark_session import create_spark_session
from pyspark.sql import functions as F
from pathlib import Path

ROOT=Path(__file__).resolve().parent.parent
SILVER_SALES_PATH=ROOT/"data"/"silver"/"sales"
GOLD_SALES_PATH=ROOT/"data"/"gold"/"sales"


def run_gold(spark,source_batch_id):
    sales_df = (
        spark.read.format("delta")
        .load(str(SILVER_SALES_PATH))
        .filter(F.col("source_batch_id") == source_batch_id)
    )

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
    .save(str(GOLD_SALES_PATH))

    persisted_gold_df = (
        spark.read.format("delta")
        .load(str(GOLD_SALES_PATH))
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
    source_batch_id="20261023_154324_e644c670"
    run_gold(spark,source_batch_id)
    if spark is not None:
        spark.stop()
