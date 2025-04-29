package utils

import org.apache.spark.sql.{SparkSession, DataFrame}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._

class MetricsLogger(    
    spark: SparkSession,
    jobId: String,
    testName: String,
    language: String,
    metricsTableName: String) {

  def timeMethodAndLogMetrics(testFunc: () => Unit): Unit = {
    val tstart = System.currentTimeMillis()

    // Run the test function
    testFunc()

    val tend = System.currentTimeMillis()
    val runTimeMs = (tend - tstart).toInt

    import spark.implicits._

    val df = Seq(
      (jobId, testName, language, runTimeMs)
    ).toDF("job_id", "test_name", "language", "run_time_ms")

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
        "run_time_ms" -> "src.run_time_ms"
      ))
      .whenNotMatched()
      .insertExpr(Map(
        "job_id" -> "src.job_id",
        "test_name" -> "src.test_name",
        "language" -> "src.language",
        "run_time_ms" -> "src.run_time_ms"
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