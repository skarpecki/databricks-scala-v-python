// Databricks notebook source
import utils.{ JobMetricsListener, MetricsLogger }
import tests.TestsFactory

val listener = new JobMetricsListener()
val metricsLogger = new MetricsLogger(spark, "123", "join_group_avg", "scala", "logging.metrics.tests_metrics")

spark.sparkContext.addSparkListener(listener)
metricsLogger.timeMethodAndLogMetrics(TestsFactory.getTestFunc("join_group_avg"))

metricsLogger.writeStageMetricsToTable(listener.stageMetrics)
