package utils

import org.apache.spark.sql.{SparkSession, DataFrame}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.{ExtendedMode, CostMode, FormattedMode}
import tests.TestCase

class MetricsLogger(    
    spark: SparkSession,
    jobId: String,
    runId: String,
    taskId: String,
    testObject: TestCase,
    language: String,
    metricsTableName: String) {

  def timeMethodAndLogMetrics(testObject: TestCase): Unit = {
    val tstart = System.currentTimeMillis()

    // Create test dataframe and run test
    val shift = 28 // 1 << 28 = 2^28 = 268,435,456 = 2019.6 MiB as per logical plan
    var df_test_arg = testObject.prepareDataFrame(spark, shift)
    val df_test = testObject.testFunc(this.spark, df_test_arg)
    val tend = System.currentTimeMillis()
    val runTimeMs = (tend - tstart).toInt
    
    val extendedPlan = df_test.queryExecution.explainString(ExtendedMode).toString()
    val costPlan = df_test.queryExecution.explainString(CostMode).toString()
    val formattedPlan = df_test.queryExecution.explainString(FormattedMode).toString()

    df_test.unpersist()

    import spark.implicits._

    val df = Seq(
      (
        jobId,
        runId,
        taskId,
        testObject.name,
        language,
        runTimeMs,
        extendedPlan,
        costPlan,
        formattedPlan
      )
    ).toDF(
      "job_id",
      "run_id",
      "task_id",
      "test_name",
      "language",
      "run_time_ms",
      "extended_plan",
      "cost_plan",
      "formatted_plan"
    ).withColumn(
      "current_timestamp", current_timestamp()
    )

    val deltaTable = DeltaTable.forName(spark, metricsTableName)

    deltaTable.as("tgt")
      .merge(
        df.as("src"),
        """tgt.job_id = src.job_id 
        AND tgt.run_id = src.run_id
        AND tgt.task_id = src.task_id
        AND tgt.test_name = src.test_name
        AND tgt.language = src.language"""
      )
      .whenMatched()
      .updateExpr(Map(
        "test_name" -> "src.test_name",
        "language" -> "src.language",
        "run_time_ms" -> "src.run_time_ms",
        "extended_plan" -> "src.extended_plan",
        "cost_plan" -> "src.cost_plan",
        "formatted_plan" -> "src.formatted_plan",
        "updated_at" -> "src.current_timestamp"
      ))
      .whenNotMatched()
      .insertExpr(Map(
        "job_id" -> "src.job_id",
        "run_id" -> "src.run_id",
        "task_id" -> "src.task_id",
        "test_name" -> "src.test_name",
        "language" -> "src.language",
        "run_time_ms" -> "src.run_time_ms",
        "extended_plan" -> "src.extended_plan",
        "cost_plan" -> "src.cost_plan",
        "formatted_plan" -> "src.formatted_plan",
        "inserted_at" -> "src.current_timestamp",
        "updated_at" -> "src.current_timestamp"
      ))
      .execute()
  }

  def writeStageMetricsToTable(stageMetrics: List[StageMetrics]) : Unit = {
      import spark.implicits._

      val df = stageMetrics.toDF.select(
        sum("executor_cpu_time_ms").as("executor_cpu_time_ms"),
        sum("executor_deserialize_cpu_time_ms").as("executor_deserialize_cpu_time_ms"),
        sum("executor_deserialize_time_ms").as("executor_deserialize_time_ms"),
        sum("executor_run_time_ms").as("executor_run_time_ms"),
        lit(jobId).as("job_id"),
        lit(runId).as("run_id"),
        lit(taskId).as("task_id"),
        lit(testObject.name).as("test_name"),
        lit(language).as("language"),
        current_timestamp().as("current_timestamp")
      )

      val dt_metrics = DeltaTable.forName(spark, metricsTableName)
      dt_metrics.as("tgt").merge(
          df.as("src"),
          """tgt.job_id = src.job_id
          AND tgt.run_id = src.run_id
          AND tgt.task_id = src.task_id
          AND tgt.test_name = src.test_name
          AND tgt.language = src.language""",
      )
      .whenMatched
      .updateExpr(
          Map(
              "language" -> "src.language",
              "executor_cpu_time_ms" -> "src.executor_cpu_time_ms",
              "executor_deserialize_cpu_time_ms" -> "src.executor_deserialize_cpu_time_ms",
              "executor_deserialize_time_ms" -> "src.executor_deserialize_time_ms",
              "executor_run_time_ms" -> "src.executor_run_time_ms",
              "updated_at" -> "src.current_timestamp"
          ))
      .whenNotMatched
      .insertExpr(Map(
          "job_id" -> "src.job_id",
          "run_id" -> "src.run_id",
          "task_id" -> "src.task_id",
          "test_name" -> "src.test_name",
          "language" -> "src.language",
          "executor_cpu_time_ms" -> "src.executor_cpu_time_ms",
          "executor_deserialize_cpu_time_ms" -> "src.executor_deserialize_cpu_time_ms",
          "executor_deserialize_time_ms" -> "src.executor_deserialize_time_ms",
          "executor_run_time_ms" -> "src.executor_run_time_ms",
          "inserted_at" -> "src.current_timestamp",
          "updated_at" -> "src.current_timestamp"
      ))
      .execute()
    }
}