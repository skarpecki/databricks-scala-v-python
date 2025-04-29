package utils

import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerApplicationStart,
  SparkListenerStageCompleted,
  SparkListenerTaskEnd
}
import scala.collection.mutable.ListBuffer
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

// snake_case to simplify writing to log table

case class StageMetrics(
  stage_id: Int,
  executor_cpu_time_ms: Long,
  executor_deserialize_cpu_time_ms: Long,
  executor_deserialize_time_ms: Long,
  executor_run_time_ms: Long
)

case class TaskMetrics(
  stage_id: Int,
  task_id: Long,
  executor_cpu_time_ms: Long,
  executor_deserialize_cpu_time_ms: Long,
  executor_deserialize_time_ms: Long,
  executor_run_time_ms: Long
)

class JobMetricsListener extends SparkListener {

  private var appIdOpt: Option[String] = None
  private val stageMetricsBuffer = ListBuffer[StageMetrics]()
  private val taskMetricsBuffer = ListBuffer[TaskMetrics]()

  def appId: Option[String] = appIdOpt
  def stageMetrics: List[StageMetrics] = stageMetricsBuffer.toList
  def taskMetrics: List[TaskMetrics] = taskMetricsBuffer.toList

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    appIdOpt = applicationStart.appId
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    Option(stageCompleted.stageInfo.taskMetrics).foreach { metrics =>
      val stage = StageMetrics(
        stage_id = stageCompleted.stageInfo.stageId,
        executor_cpu_time_ms = metrics.executorCpuTime / 1000000,
        executor_deserialize_cpu_time_ms = metrics.executorDeserializeCpuTime / 1000000,
        executor_deserialize_time_ms = metrics.executorDeserializeTime,
        executor_run_time_ms = metrics.executorRunTime
      )
      stageMetricsBuffer += stage
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskInfo = taskEnd.taskInfo
    Option(taskEnd.taskMetrics).foreach { metrics =>
      val task = TaskMetrics(
        stage_id = taskEnd.stageId,
        task_id = taskInfo.taskId,
        executor_cpu_time_ms = metrics.executorCpuTime / 1000000,
        executor_deserialize_cpu_time_ms = metrics.executorDeserializeCpuTime / 1000000,
        executor_deserialize_time_ms = metrics.executorDeserializeTime,
        executor_run_time_ms = metrics.executorRunTime
      )
      taskMetricsBuffer += task
    }
  }

  def writeAverageMetricsToTable(
    spark: SparkSession,
    jobId: String,
    testName: String,
    language: String,
    metricsTableName: String) : Unit = {
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
