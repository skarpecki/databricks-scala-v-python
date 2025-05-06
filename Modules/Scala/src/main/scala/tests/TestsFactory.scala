package tests

object TestsFactory {
  def getTestFunc(code: String): (SparkSession) => DataFrame = {
    code match {
      case "join_group_avg" => JoinGroupAverageTest.testFunc
    }
  }
}