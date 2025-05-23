package tests

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import udfs.UdfRegistry

object LeftPythonPickledUdfTest extends TestCase {
  val code = "left_python_pickled_udf"

  override def testFunc(spark: SparkSession, df: DataFrame): DataFrame = {
    val dfTest = df.selectExpr(s"${UdfRegistry.LEFT_PYTHON_PICKLED_UDF}($COLUMN, 3) as $COLUMN")
    dfTest.write.format("noop").mode("overwrite").save()
    dfTest
  }
}