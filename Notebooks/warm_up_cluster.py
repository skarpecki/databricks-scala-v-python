# Databricks notebook source
from databricks_libs.tests import Test

shift = 28 # as in tests
df = Test.prepare_dataframe(spark, shift, True)

df.unpersist()
