// Databricks notebook source
import utils.{ JobMetricsListener, MetricsLogger }

val listener = new JobMetricsListener()
val metricsLogger = new MetricsLogger(spark, "123", "test", "scala", "logging.metrics.tests_metrics")

spark.sparkContext.addSparkListener(listener)

// COMMAND ----------

metricsLogger.timeMethodAndLogMetrics() {
  () => {
    val n = 7
    val table = "samples.tpch.orders"

    var df = spark.table(table)
    for (_ <- 1 to n) {
      df = df.union(spark.table(table))
    }

    df.write
      .mode("overwrite")
      .saveAsTable("bronze.default.orders_scala")
  }
}

// COMMAND ----------

metricsLogger.writeStageMetricsToTable(listener.stageMetrics)
