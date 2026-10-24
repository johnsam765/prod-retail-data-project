from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def create_spark_session():
    sparkBuilder=(SparkSession.builder
    .master("local[*]")
    .appName("prod-retail")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    spark=configure_spark_with_delta_pip(sparkBuilder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
