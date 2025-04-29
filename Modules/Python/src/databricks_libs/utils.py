from datetime import datetime
from collections.abc import Callable
from delta.tables import *

def time_method_log_metrics(
    spark,
    test_name: str,
    job_id: str,
    metrics_table_name: str,
    test_func: Callable[[], None]
):
    """
    Measures the execution time of a test function and logs the result into a Delta table.

    Args:
        spark: 
            A SparkSession object used to interact with the Delta table.
        test_name (str): 
            The name of the test or function being measured.
        job_id (str): 
            A unique identifier for the job execution.
        metrics_table_name (str): 
            The name of the Delta table where metrics will be stored.
        test_func (Callable[[], None]): 
            A no-argument function to be executed and timed.

    Functionality:
        - Executes `test_func` and measures its runtime in milliseconds.
        - Creates a DataFrame containing job_id, test_name, language (hardcoded to "python"), and run time.
        - Merges the result into the specified Delta table:
            - If a matching job_id and test_name exist, updates the record.
            - If no match is found, inserts a new record.
    """
    language = "python"
    tstart = datetime.now()
    test_func()
    tend = datetime.now()
    run_time_ms = (int)((tend - tstart).total_seconds() * 1000)
    df = spark.createDataFrame([{
        "job_id": job_id,
        "test_name": test_name,
        "language": language,
        "run_time_ms": run_time_ms}])

    dt_metrics = DeltaTable.forName(spark, metrics_table_name)
    (
        dt_metrics.alias("tgt").merge(
            df.alias("src"),
            "tgt.job_id = src.job_id AND tgt.test_name = src.test_name",
        )
        .whenMatchedUpdate(set = {
            "test_name": "src.test_name",
            "language": "src.language",
            "run_time_ms": "src.run_time_ms"
        })
        .whenNotMatchedInsert(values = {
            "job_id": "src.job_id",
            "test_name": "src.test_name",
            "language": "src.language",
            "run_time_ms": "src.run_time_ms"
        })
        .execute()
    )