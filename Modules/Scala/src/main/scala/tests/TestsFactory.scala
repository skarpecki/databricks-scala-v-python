package tests

import org.apache.spark.sql.{SparkSession, DataFrame}

object TestsFactory {
  val tests: Seq[TestCase] = Seq(
    JoinGroupAverageTest,
    LeftPandasUdfTest,
    LeftPythonArrowUdfTest,
    LeftPythonPickledUdfTest,
    LeftScalaUdfTest,
    LeftSparkTest
  )

  val testsMap: Map[String, TestCase] = tests.map(test => test.code -> test).toMap

  def getTestObject(code: String): TestCase = {
    testsMap.getOrElse(code, {
      val validCodes = testsMap.keys.mkString(" ")
      throw new IllegalArgumentException(s"Unknown test code: $code. Accepted codes are $validCodes.")
    })
  }
}