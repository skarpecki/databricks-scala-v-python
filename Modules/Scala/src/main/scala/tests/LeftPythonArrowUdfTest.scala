package tests

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import udfs.UdfRegistry

object LeftPythonArrowUdfTest extends TestCase {
  val code = "left_python_arrow_udf"

  override def testFunc(spark: SparkSession, df: DataFrame): DataFrame = {
    val dfTest = df.selectExpr(s"${UdfRegistry.LEFT_PYTHON_ARROW_UDF}($COLUMN, 3) as $COLUMN")
    dfTest.write.format("noop").mode("overwrite").save()
    dfTest
  }
}