package utils

import org.apache.spark.sql.{SparkSession, DataFrame}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.{ExtendedMode, CostMode, FormattedMode}

class MetricsLogger(    
    spark: SparkSession,
    jobId: String,
    testName: String,
    language: String,
    metricsTableName: String) {

  def timeMethodAndLogMetrics(testFunc: SparkSession => DataFrame): Unit = {
    val tstart = System.currentTimeMillis()

    // Run the test function
    val df_test = testFunc(this.spark)
    // Force evaluation by calling an action
    df_test.show(100)
    val tend = System.currentTimeMillis()
    val runTimeMs = (tend - tstart).toInt
    
    val extendedPlan = df_test.queryExecution.explainString(ExtendedMode).toString()
    val costPlan = df_test.queryExecution.explainString(CostMode).toString()
    val formattedPlan = df_test.queryExecution.explainString(FormattedMode).toString()

    import spark.implicits._

    val df = Seq(
      (
        jobId,
        testName,
        language,
        runTimeMs,
        extendedPlan,
        costPlan,
        formattedPlan
      )
    ).toDF(
      "job_id",
      "test_name",
      "language",
      "run_time_ms",
      "extended_plan",
      "cost_plan",
      "formatted_plan"
    )

    val deltaTable = DeltaTable.forName(spark, metricsTableName)

    deltaTable.as("tgt")
      .merge(
        df.as("src"),
        "tgt.job_id = src.job_id AND tgt.test_name = src.test_name AND tgt.language = src.language"
      )
      .whenMatched()
      .updateExpr(Map(
        "test_name" -> "src.test_name",
        "language" -> "src.language",
        "run_time_ms" -> "src.run_time_ms",
        "extended_plan" -> "src.extended_plan",
        "cost_plan" -> "src.cost_plan",
        "formatted_plan" -> "src.formatted_plan"
      ))
      .whenNotMatched()
      .insertExpr(Map(
        "job_id" -> "src.job_id",
        "test_name" -> "src.test_name",
        "language" -> "src.language",
        "run_time_ms" -> "src.run_time_ms",
        "extended_plan" -> "src.extended_plan",
        "cost_plan" -> "src.cost_plan",
        "formatted_plan" -> "src.formatted_plan"
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
        lit(testName).as("test_name"),
        lit(language).as("language")
      )

      val dt_metrics = DeltaTable.forName(spark, metricsTableName)
      dt_metrics.as("tgt").merge(
          df.as("src"),
          "tgt.job_id = src.job_id AND tgt.test_name = src.test_name AND tgt.language = src.language",
      )
      .whenMatched
      .updateExpr(
          Map(
              "language" -> "src.language",
              "executor_cpu_time_ms" -> "src.executor_cpu_time_ms",
              "executor_deserialize_cpu_time_ms" -> "src.executor_deserialize_cpu_time_ms",
              "executor_deserialize_time_ms" -> "src.executor_deserialize_time_ms",
              "executor_run_time_ms" -> "src.executor_run_time_ms"
          ))
      .whenNotMatched
      .insertExpr(Map(
          "job_id" -> "src.job_id",
          "test_name" -> "src.test_name",
          "language" -> "src.language",
          "executor_cpu_time_ms" -> "src.executor_cpu_time_ms",
          "executor_deserialize_cpu_time_ms" -> "src.executor_deserialize_cpu_time_ms",
          "executor_deserialize_time_ms" -> "src.executor_deserialize_time_ms",
          "executor_run_time_ms" -> "src.executor_run_time_ms"
      ))
      .execute()
    }
}