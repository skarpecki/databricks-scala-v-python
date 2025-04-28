# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Tests will be defined in a single module attached as wheel package. Then there will be a single notebook orchestrating all the code, where test will be passed as a parameter. UDFs can be a little tricky as they will need first to be registered but worst case it will be a single cell in beginning of a notebook that will execute for all, but will not be tracked with listener

# COMMAND ----------

def rw_test():
    n = 7
    table = "samples.tpch.orders"

    df = spark.read.table(table)
    for _ in range(n):
        df = df.union(
            spark.read.table(table)
        )

    (
        df.write
        .mode("overwrite")
        .saveAsTable("bronze.default.orders_py")
    )

# COMMAND ----------

# MAGIC %scala
# MAGIC import utils.JobMetricsListener
# MAGIC
# MAGIC val listener = new JobMetricsListener()
# MAGIC spark.sparkContext.addSparkListener(listener)

# COMMAND ----------

from datetime import datetime
from collections.abc import Callable
from delta.tables import *

def time_method(spark, test_name: str, job_id: str, metrics_table_name: str, test_func: Callable[[], None]):
    language = "python"
    tstart = datetime.now()
    test_func()
    tend = datetime.now()
    run_time_ms = (int)((tend - tstart).total_seconds() * 1000)
    df = spark.createDataFrame([{
        "job_id": job_id,
        "test_name": test_name,
        "language": language,
        "run_time_ms": run_time_ms}])

    dt_metrics = DeltaTable.forName(spark, metrics_table_name)
    (
        dt_metrics.alias("tgt").merge(
            df.alias("src"),
            "tgt.job_id = src.job_id AND tgt.test_name = src.test_name",
        )
        .whenMatchedUpdate(set = {
            "test_name": "src.test_name",
            "language": "src.language",
            "run_time_ms": "src.run_time_ms"
        })
        .whenNotMatchedInsert(values = {
            "job_id": "src.job_id",
            "test_name": "src.test_name",
            "language": "src.language",
            "run_time_ms": "src.run_time_ms"
        })
        .execute()
    )

# COMMAND ----------

time_method(spark, "test123", "123", "logging.metrics.tests_metrics", rw_test)

# COMMAND ----------

# MAGIC %scala
# MAGIC import io.delta.tables._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC
# MAGIC val metrics_table_name = "logging.metrics.tests_metrics"
# MAGIC val job_id = "123"
# MAGIC val test_name = "test123"
# MAGIC val language = "scala"
# MAGIC
# MAGIC var df = listener.stageMetrics.toDF
# MAGIC
# MAGIC df = df.select(
# MAGIC   sum("executor_cpu_time_ms").as("executor_cpu_time_ms"),
# MAGIC   sum("executor_deserialize_cpu_time_ms").as("executor_deserialize_cpu_time_ms"),
# MAGIC   sum("executor_deserialize_time_ms").as("executor_deserialize_time_ms"),
# MAGIC   sum("executor_run_time_ms").as("executor_run_time_ms"),
# MAGIC   lit(job_id).as("job_id"),
# MAGIC   lit(test_name).as("test_name"),
# MAGIC   lit(language).as("language")
# MAGIC )
# MAGIC
# MAGIC val dt_metrics = DeltaTable.forName(spark, metrics_table_name)
# MAGIC
# MAGIC
# MAGIC dt_metrics.as("tgt").merge(
# MAGIC     df.as("src"),
# MAGIC     "tgt.job_id = src.job_id AND tgt.test_name = src.test_name",
# MAGIC )
# MAGIC .whenMatched
# MAGIC .updateExpr(
# MAGIC     Map(
# MAGIC         "language" -> "src.language",
# MAGIC         "executor_cpu_time_ms" -> "src.executor_cpu_time_ms",
# MAGIC         "executor_deserialize_cpu_time_ms" -> "src.executor_deserialize_cpu_time_ms",
# MAGIC         "executor_deserialize_time_ms" -> "src.executor_deserialize_time_ms",
# MAGIC         "executor_run_time_ms" -> "src.executor_run_time_ms"
# MAGIC     ))
# MAGIC .whenNotMatched
# MAGIC .insertExpr(Map(
# MAGIC     "job_id" -> "src.job_id",
# MAGIC     "test_name" -> "src.test_name",
# MAGIC     "language" -> "src.language",
# MAGIC     "executor_cpu_time_ms" -> "src.executor_cpu_time_ms",
# MAGIC     "executor_deserialize_cpu_time_ms" -> "src.executor_deserialize_cpu_time_ms",
# MAGIC     "executor_deserialize_time_ms" -> "src.executor_deserialize_time_ms",
# MAGIC     "executor_run_time_ms" -> "src.executor_run_time_ms"
# MAGIC ))
# MAGIC .execute()
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT * FROM logging.metrics.tests_metrics

# COMMAND ----------

# MAGIC %scala
# MAGIC import utils.JobMetricsListener
# MAGIC
# MAGIC val listener = new JobMetricsListener()
# MAGIC spark.sparkContext.addSparkListener(listener)

# COMMAND ----------

# MAGIC %scala 
# MAGIC
# MAGIC listener.stageMetrics.toDF.show()
