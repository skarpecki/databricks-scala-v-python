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
# MAGIC import utils.MetricsLogger
# MAGIC
# MAGIC val listener = new JobMetricsListener()
# MAGIC val metricsLogger = new MetricsLogger(spark, "123", "test123", "python", "logging.metrics.tests_metrics")
# MAGIC
# MAGIC spark.sparkContext.addSparkListener(listener)

# COMMAND ----------

from databricks_libs.utils import time_method_log_metrics
time_method_log_metrics(spark, "123", "test123", "python", "logging.metrics.tests_metrics", rw_test)

# COMMAND ----------

# MAGIC %scala
# MAGIC metricsLogger.writeStageMetricsToTable(listener.stageMetrics)
