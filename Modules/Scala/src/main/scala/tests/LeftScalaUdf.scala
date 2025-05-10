package tests

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StringType

object LeftScalaUdf {
    val code: String = "left_scala_udf"
    val leftScalaUdf: UserDefinedFunction = udf((strVal: String, len: Int) => {
        strVal.take(len)
    })
}