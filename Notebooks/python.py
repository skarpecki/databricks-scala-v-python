# Databricks notebook source
# Here Python UDFs will be registered so they can be used in a test method 

# COMMAND ----------

# Here Scala UDFs will be registered so they can be used in a test method

# COMMAND ----------

# MAGIC %scala
# MAGIC import utils.JobMetricsListener
# MAGIC import utils.MetricsLogger
# MAGIC
# MAGIC val listener = new JobMetricsListener()
# MAGIC val metricsLogger = new MetricsLogger(spark, "123", "join_group_avg", "python", "logging.metrics.tests_metrics")
# MAGIC
# MAGIC spark.sparkContext.addSparkListener(listener)

# COMMAND ----------

from databricks_libs.utils import time_method_log_metrics
from databricks_libs.tests import TestsFactory

time_method_log_metrics(spark, "123", "join_group_avg", "python", "logging.metrics.tests_metrics", TestsFactory().get_test_func("join_group_avg"))

# COMMAND ----------

# MAGIC %scala
# MAGIC metricsLogger.writeStageMetricsToTable(listener.stageMetrics)

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM logging.metrics.tests_metrics
