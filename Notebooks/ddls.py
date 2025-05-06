# Databricks notebook source
# MAGIC %md
# MAGIC ### Catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- LOGGING
# MAGIC CREATE CATALOG IF NOT EXISTS `logging`
# MAGIC MANAGED LOCATION 'abfss://data@skarpeckiadb.dfs.core.windows.net/';
# MAGIC
# MAGIC -- DATA
# MAGIC CREATE CATALOG IF NOT EXISTS `data`
# MAGIC MANAGED LOCATION 'abfss://data@skarpeckiadb.dfs.core.windows.net/';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC --logging.metrics
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS `logging`.`metrics`
# MAGIC MANAGED LOCATION 'abfss://data@skarpeckiadb.dfs.core.windows.net/logging/metrics';
# MAGIC
# MAGIC -- DATA
# MAGIC CREATE SCHEMA IF NOT EXISTS `data`.`tpch`
# MAGIC MANAGED LOCATION 'abfss://data@skarpeckiadb.dfs.core.windows.net/data/tpch';

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Tables
# MAGIC
# MAGIC Managed tables to simplify deletion and managing tables just via commands

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE `logging`.`metrics`.`tests_metrics` (
# MAGIC   `job_id`  STRING,
# MAGIC   `test_name` STRING,
# MAGIC   `language` STRING,
# MAGIC   `executor_cpu_time_ms` INT,
# MAGIC   `executor_deserialize_cpu_time_ms` INT,
# MAGIC   `executor_deserialize_time_ms` INT,
# MAGIC   `executor_run_time_ms` INT,
# MAGIC   `run_time_ms` INT,
# MAGIC   `extended_plan` STRING,
# MAGIC   `cost_plan` STRING,
# MAGIC   `formatted_plan` STRING
# MAGIC )

# COMMAND ----------

dbutils.fs.ls("abfss://data@skarpeckiadb.dfs.core.windows.net/data")
