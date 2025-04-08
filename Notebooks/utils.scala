// Databricks notebook source
import org.apache.spark.scheduler.{
  SparkListener,
  SparkListenerApplicationStart,
  SparkListenerStageCompleted,
  SparkListenerTaskEnd
}
import scala.collection.mutable.ListBuffer

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
}


// COMMAND ----------

val listener = new JobMetricsListener()
spark.sparkContext.addSparkListener(listener)
