package tests

import org.apache.spark.sql.{SparkSession, DataFrame}

object TestsFactory {
  def getTestFunc(code: String): (SparkSession) => DataFrame = {
    code match {
      case JoinGroupAverageTest.name => JoinGroupAverageTest.testFunc
    }
  }
}