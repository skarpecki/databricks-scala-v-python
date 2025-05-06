package tests

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object JoinGroupAverageTest extends TestCase {
    def testFunc(spark: SparkSession) : DataFrame = {
      val df_o = spark.table("bronze.default.orders")
      val df_c = spark.table("bronze.default.customer")
      val df_n = spark.table("bronze.default.nation")

      val df = 
        df_o
        .join(
          df_c,
          df_o("o_custkey") === df_c("c_custkey"),
          "inner")
        .join(
          df_n,
          df_c("c_nationkey") === df_n("n_nationkey"),
          "inner")
        .groupBy("n_name")
        .agg(avg("o_totalprice").alias("average_totalprice"))
        .orderBy("n_name")

      return df
    }
}