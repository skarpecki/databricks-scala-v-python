// Databricks notebook source
dbutils.widgets.text("test_code", "")
dbutils.widgets.text("job_id", "{{job.id}}")
dbutils.widgets.text("run_id", "{{job.run_id}}")
dbutils.widgets.text("task_id", "task_id")
dbutils.widgets.text("language", "Scala")
dbutils.widgets.text("metrics_table", "logging.metrics.tests_metrics")

val testCode = dbutils.widgets.get("test_code")
val jobId = dbutils.widgets.get("job_id")
val runId = dbutils.widgets.get("run_id")
val taskId = dbutils.widgets.get("task_id")
val language = dbutils.widgets.get("language")
val metricsTable = dbutils.widgets.get("metrics_table")

// COMMAND ----------

// MAGIC %python
// MAGIC from databricks_libs.udf_registry import UdfRegistry
// MAGIC
// MAGIC UdfRegistry().register_udf(spark)

// COMMAND ----------

import udfs.UdfRegistry

UdfRegistry.registerUdfs(spark)

// COMMAND ----------

import utils.{ JobMetricsListener, MetricsLogger }
import tests.TestsFactory

val listener = new JobMetricsListener()
val metricsLogger = new MetricsLogger(spark, jobId, runId, taskId, testCode, language, metricsTable)

spark.sparkContext.addSparkListener(listener)
metricsLogger.timeMethodAndLogMetrics(TestsFactory.getTestObject(testCode))

metricsLogger.writeStageMetricsToTable(listener.stageMetrics)
