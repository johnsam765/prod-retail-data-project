from uuid import uuid4
from datetime import datetime, timezone
from pyspark.sql import functions as F

def generate_pipeline_run_id():
    return str(uuid4())

def get_source_batch_id(spark,path,pipeline_run_id):
    batch_ids=(spark.read.format("delta").load(str(path))
    .filter(F.col("pipeline_run_id")==pipeline_run_id)
    .select("source_batch_id").distinct().limit(2).collect())
    if len(batch_ids)!=1:
        raise RuntimeException("Multiple Batch ids found")
    source_batch_id=batch_ids[0]["source_batch_id"]
    if source_batch_id is None:
        raise RuntimeError("Source batch ID cannot be NULL")
    return source_batch_id


def set_source_batch_id():
   return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")+"_"+uuid4().hex[:8]
