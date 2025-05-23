package tests

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import udfs.UdfRegistry

object LeftPandasUdfTest extends TestCase {
  val code = "left_pandas_udf"

  override def testFunc(spark: SparkSession, df: DataFrame): DataFrame = {
    val dfTest = df.selectExpr(s"${UdfRegistry.LEFT_PANDAS_UDF}($COLUMN) as $COLUMN")
    dfTest.write.format("noop").mode("overwrite").save()
    dfTest
  }
}