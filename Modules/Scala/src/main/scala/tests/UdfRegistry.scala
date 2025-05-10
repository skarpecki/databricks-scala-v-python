package tests

import org.apache.spark.sql.SparkSession

object UdfRegistry {
    val LEFT_SCALA_UDF: String = LeftScalaUdf.code
    val LEFT_PYTHON_UDF = "left_python_udf"
 
    def registerUdfs(spark: SparkSession): Unit = {
        spark.udf.register(LEFT_SCALA_UDF, LeftScalaUdf.leftScalaUdf)
    }
}