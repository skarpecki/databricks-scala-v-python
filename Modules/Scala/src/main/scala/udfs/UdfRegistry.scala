package udfs

import org.apache.spark.sql.SparkSession

object UdfRegistry {
    val LEFT_PYTHON_ARROW_UDF = "left_python_arrow_udf"
    val LEFT_PYTHON_PICKLED_UDF = "left_python_pickled_udf"
    val LEFT_SCALA_UDF = "left_scala_udf"
    val LEFT_PANDAS_UDF = "left_pandas_udf"
 
    def registerUdfs(spark: SparkSession): Unit = {
        spark.udf.register(LEFT_SCALA_UDF, LeftScalaUdf.leftScalaUdf)
    }
}