# Databricks notebook source
dbutils.widgets.text("test_code", "")

# COMMAND ----------

test_code = dbutils.widgets.get("test_code")

# COMMAND ----------

from databricks_libs.tests import TestsFactory

test_object = TestsFactory().get_test_object(test_code)
shift = 28 # dafult for tests
df = test_object.prepare_dataframe(spark, shift, True)
df.count()
