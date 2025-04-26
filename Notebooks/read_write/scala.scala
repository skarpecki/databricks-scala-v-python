// Databricks notebook source
import utils.JobMetricsListener

val listener = new JobMetricsListener()
spark.sparkContext.addSparkListener(listener)

// COMMAND ----------

val start = System.currentTimeMillis

val n = 7
val table = "samples.tpch.orders"

var df = spark.table(table)
for (_ <- 1 to n) {
  df = df.union(spark.table(table))
}

df.write
  .mode("overwrite")
  .saveAsTable("bronze.default.orders_scala")


val end = System.currentTimeMillis

// COMMAND ----------

listener.stageMetrics.toDF.show()

// COMMAND ----------

println((end - start) / 1000)
