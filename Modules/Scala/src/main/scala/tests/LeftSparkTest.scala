package tests

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object LeftSparkTest extends TestCase {
  val code = "left_spark"

  override def testFunc(spark: SparkSession, df: DataFrame): DataFrame = {
    val dfTest = df.selectExpr(s"left($COLUMN, 3) as $COLUMN")
    dfTest.write.format("noop").mode("overwrite").save()
    dfTest
  }
}