from pathlib import Path
from spark_session import create_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import StructField,StructType,StringType
from run_identity import set_source_batch_id

ROOT_PATH=Path(__file__).resolve().parent.parent
SALES_RAW_PATH=ROOT_PATH/"data"/"raw"/"sales.csv"
CUSTOMER_RAW_PATH=ROOT_PATH/"data"/"raw"/"customers.csv"
SALES_BRONZE_PATH=ROOT_PATH/"data"/"bronze"/"sales"
CUSTOMER_BRONZE_PATH=ROOT_PATH/"data"/"bronze"/"customers"

def load_csv_to_spark(spark,source,destination,schema,source_batch_id,pipeline_run_id):
    bronze_df=spark.read.option("header",True).schema(schema).csv(str(source))
    bronze_final_df=(bronze_df.withColumn("source_batch_id",F.lit(source_batch_id)).withColumn("pipeline_run_id",F.lit(pipeline_run_id))
    .withColumn("ingestedTime",F.current_timestamp())
    .withColumn("source_path",F.lit(str(source))))
    bronze_final_df.write.format("delta").mode("append").save(str(destination))
    return bronze_final_df.count()


def run_bronze(spark,pipeline_run_id):
    source_batch_id=set_source_batch_id()
    sales_schema=(StructType([
    StructField("order_id",StringType(),True),
    StructField("customer_id",StringType(),True),
    StructField("order_date",StringType(),True),
    StructField("status",StringType(),True),
    StructField("amount",StringType(),True)]))

    customer_schema=(StructType([
    StructField("customer_id",StringType(),True),
    StructField("customer_name",StringType(),True),
    StructField("region",StringType(),True),
    StructField("email",StringType(),True)]))

    print(f"Loading sales csv into spark as df for the batch:{source_batch_id}")
    sales_bronze_count=load_csv_to_spark(spark,SALES_RAW_PATH,SALES_BRONZE_PATH,sales_schema,source_batch_id,pipeline_run_id)

    print(f"Loading customer csv into spark as df for the batch:{source_batch_id}")
    customer_bronze_count=load_csv_to_spark(spark,CUSTOMER_RAW_PATH,CUSTOMER_BRONZE_PATH,customer_schema,source_batch_id,pipeline_run_id)

    print(f"Sales count:{sales_bronze_count},Customer count:{customer_bronze_count}")
    print("Bronze pipeline completed")
    return sales_bronze_count,customer_bronze_count

if __name__=="__main__":
    spark=create_spark_session()
    print(run_bronze(spark))
    if spark is not None:
        spark.stop() 
