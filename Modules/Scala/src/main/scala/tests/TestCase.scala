package tests

import org.apache.spark.sql.{SparkSession, DataFrame}

trait TestCase {
  def testFunc(spark: SparkSession): DataFrame
}