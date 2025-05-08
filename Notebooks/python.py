# Databricks notebook source
dbutils.widgets.text("test_code", "")
dbutils.widgets.text("job_id", "{{job.id}}")
dbutils.widgets.text("run_id", "{{job.run_id}}")
dbutils.widgets.text("task_id", "task_id")
dbutils.widgets.dropdown("language", "Python", ["Python", "Scala"])
dbutils.widgets.text("metrics_table", "logging.metrics.tests_metrics")

test_code = dbutils.widgets.get("test_code")
job_id = dbutils.widgets.get("job_id")
run_id = dbutils.widgets.get("run_id")
task_id = dbutils.widgets.get("task_id")
language = dbutils.widgets.get("language")
metrics_table = dbutils.widgets.get("metrics_table")

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.widgets.text("test_code", "")
# MAGIC dbutils.widgets.text("job_id", "{{job.id}}")
# MAGIC dbutils.widgets.text("run_id", "{{job.run_id}}")
# MAGIC dbutils.widgets.text("task_id", "task_id")
# MAGIC dbutils.widgets.text("language", "Python")
# MAGIC dbutils.widgets.text("metrics_table", "logging.metrics.tests_metrics")
# MAGIC
# MAGIC val testCode = dbutils.widgets.get("test_code")
# MAGIC val jobId = dbutils.widgets.get("job_id")
# MAGIC val runId = dbutils.widgets.get("run_id")
# MAGIC val taskId = dbutils.widgets.get("task_id")
# MAGIC val language = dbutils.widgets.get("language")
# MAGIC val metricsTable = dbutils.widgets.get("metrics_table")

# COMMAND ----------

# Here Python UDFs will be registered so they can be used in a test method 
# Maybe do it in another notebook

# COMMAND ----------

# Here Python UDFs will be registered so they can be used in a test method 
# Maybe do it in another notebook

# COMMAND ----------

# MAGIC %scala
# MAGIC import utils.JobMetricsListener
# MAGIC import utils.MetricsLogger
# MAGIC
# MAGIC val listener = new JobMetricsListener()
# MAGIC val metricsLogger = new MetricsLogger(spark, jobId, runId, taskId, testCode, language, metricsTable)
# MAGIC
# MAGIC spark.sparkContext.addSparkListener(listener)

# COMMAND ----------

from databricks_libs.utils import time_method_log_metrics
from databricks_libs.tests import TestsFactory

time_method_log_metrics(spark, job_id, run_id, task_id, test_code, language, metrics_table, TestsFactory().get_test_func(test_code))

# COMMAND ----------

# MAGIC %scala
# MAGIC metricsLogger.writeStageMetricsToTable(listener.stageMetrics)
