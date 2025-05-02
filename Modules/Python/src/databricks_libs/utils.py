from datetime import datetime
from collections.abc import Callable
from delta.tables import *

def time_method_log_metrics(
    spark,
    job_id: str,
    test_name: str,
    language: str,
    metrics_table_name: str,
    test_func: Callable[[], None]
):
    """
    Measures the execution time of a test function and logs the result into a Delta table.

    Args:
        spark: 
            A SparkSession object used to interact with the Delta table.
        job_id (str): 
            A unique identifier for the job execution.
        test_name (str): 
            The name of the test or function being measured.
        metrics_table_name (str): 
            The name of the Delta table where metrics will be stored.
        language (str): 
            Language for which test is performed.
        test_func (Callable[[], None]): 
            A no-argument function to be executed and timed.

    Functionality:
        - Executes `test_func` and measures its runtime in milliseconds.
        - Creates a DataFrame containing job_id, test_name, language (hardcoded to "python"), and run time.
        - Merges the result into the specified Delta table:
            - If a matching job_id and test_name exist, updates the record.
            - If no match is found, inserts a new record.
    """
    tstart = datetime.now()
    df = test_func()
    extended_plan = df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "extended")
    cost_plan = df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "cost")
    formatted_plan = df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")
    tend = datetime.now()
    run_time_ms = (int)((tend - tstart).total_seconds() * 1000)
    df = spark.createDataFrame([{
        "job_id": job_id,
        "test_name": test_name,
        "language": language,
        "run_time_ms": run_time_ms,
        "extended_plan": extended_plan,
        "cost_plan": cost_plan,
        "formatted_plan": formatted_plan}]) 

    dt_metrics = DeltaTable.forName(spark, metrics_table_name)
    (
        dt_metrics.alias("tgt").merge(
            df.alias("src"),
            "tgt.job_id = src.job_id AND tgt.test_name = src.test_name AND tgt.language = src.language",
        )
        .whenMatchedUpdate(set = {
            "test_name": "src.test_name",
            "language": "src.language",
            "run_time_ms": "src.run_time_ms",
            "extended_plan": "src.extended_plan",
            "cost_plan": "src.cost_plan",
            "formatted_plan": "src.formatted_plan"
        })
        .whenNotMatchedInsert(values = {
            "job_id": "src.job_id",
            "test_name": "src.test_name",
            "language": "src.language",
            "run_time_ms": "src.run_time_ms",
            "extended_plan": "src.extended_plan",
            "cost_plan": "src.cost_plan",
            "formatted_plan": "src.formatted_plan"
        })
        .execute()
    )