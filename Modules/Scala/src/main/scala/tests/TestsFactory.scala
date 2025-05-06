package tests

import org.apache.spark.sql.{SparkSession, DataFrame}

object TestsFactory {
  def getTestFunc(code: String): (SparkSession) => DataFrame = {
    code match {
      case "join_group_avg" => JoinGroupAverageTest.testFunc
    }
  }
}