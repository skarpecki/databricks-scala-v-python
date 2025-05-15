package tests

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

trait TestCase {
  val COLUMN = "rand_val"

  def name: String
  
  def testFunc(spark: SparkSession, df_arg: DataFrame): DataFrame
  
  def prepareDataFrame(spark: SparkSession, shift: Int, printPlan: Boolean = false): DataFrame = {
    val df = spark.range(0, 1L << shift)
      .toDF("id")
      .withColumn(this.COLUMN, rand() * 10000)
      .select(format_string("%d", col(this.COLUMN).cast("int")).alias(this.COLUMN))

    df.persist(StorageLevel.MEMORY_AND_DISK)
    df.count()
    if (printPlan) {
      df.explain("cost")
    }

    df
  }

}