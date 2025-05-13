from datetime import datetime
from collections.abc import Callable
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp
from .tests import Test

def time_method_log_metrics(
    spark,
    job_id: str,
    run_id: str,
    task_id: str,
    test_name: str,
    language: str,
    metrics_table_name: str,
    test_object: Test
):
    """
    Measures the execution time of a test function and logs the result into a Delta table.

    Args:
        spark: 
            A SparkSession object used to interact with the Delta table.
        job_id (str): 
            A unique identifier for the job.
        run_id (str): 
            A unique identifier for the job run.
        run_id (str): 
            A unique identifier for the task in a job run combination.  
        test_name (str): 
            The name of the test or function being measured.
        language (str): 
            Language for which test is performed.    
        metrics_table_name (str): 
            The name of the Delta table where metrics will be stored.
        test_func (Callable[[], None]): 
            A no-argument function to be executed and timed.
    """
    shift = 28 # 1 << 28 = 2^28 = 268,435,456 = 2019.6 MiB as per logical plan
    df_test = test_object.prepare_dataframe(spark, shift, False)
    t_start = datetime.now()
    df_test = test_object.test_func(spark, df_test)
    # Action to force execution - write to nowhere
    df_test.write.format("noop").mode("overwrite").save()
    t_end = datetime.now()
    run_time_ms = (int)((t_end - t_start).total_seconds() * 1000)
    
    extended_plan = df_test._sc._jvm.PythonSQLUtils.explainString(df_test._jdf.queryExecution(), "extended")
    cost_plan = df_test._sc._jvm.PythonSQLUtils.explainString(df_test._jdf.queryExecution(), "cost")
    formatted_plan = df_test._sc._jvm.PythonSQLUtils.explainString(df_test._jdf.queryExecution(), "formatted")

    df_test.unpersist()

    df = spark.createDataFrame([{
        "job_id": job_id,
        "run_id": run_id,
        "task_id": task_id,
        "test_name": test_name,
        "language": language,
        "run_time_ms": run_time_ms,
        "extended_plan": extended_plan,
        "cost_plan": cost_plan,
        "formatted_plan": formatted_plan}]).withColumn(
            "current_timestamp", current_timestamp()
    )

    dt_metrics = DeltaTable.forName(spark, metrics_table_name)
    (
        dt_metrics.alias("tgt").merge(
            df.alias("src"),
            """tgt.job_id = src.job_id
            AND tgt.run_id = src.run_id
            AND tgt.task_id = src.task_id
            AND tgt.test_name = src.test_name
            AND tgt.language = src.language""",
        )
        .whenMatchedUpdate(set = {
            "test_name": "src.test_name",
            "language": "src.language",
            "run_time_ms": "src.run_time_ms",
            "extended_plan": "src.extended_plan",
            "cost_plan": "src.cost_plan",
            "formatted_plan": "src.formatted_plan",
            "updated_at": "src.current_timestamp"
        })
        .whenNotMatchedInsert(values = {
            "job_id": "src.job_id",
            "run_id": "src.run_id",
            "task_id": "src.task_id",
            "test_name": "src.test_name",
            "language": "src.language",
            "run_time_ms": "src.run_time_ms",
            "extended_plan": "src.extended_plan",
            "cost_plan": "src.cost_plan",
            "formatted_plan": "src.formatted_plan",
            "inserted_at": "src.current_timestamp",
            "updated_at": "src.current_timestamp"
        })
        .execute()
    )