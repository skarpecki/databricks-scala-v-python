# Databricks notebook source
from time import time

# COMMAND ----------

# MAGIC %run "../utils"

# COMMAND ----------

start = time()

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

end = time()

# COMMAND ----------

# MAGIC %scala
# MAGIC listener.stageMetrics.toDF.show()

# COMMAND ----------

print(end - start)
