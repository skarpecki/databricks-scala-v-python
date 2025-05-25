# Databricks notebook source
# MAGIC %scala
# MAGIC
# MAGIC // Necessary imports
# MAGIC import org.apache.spark.sql.expressions.UserDefinedFunction
# MAGIC import org.apache.spark.sql.functions.udf
# MAGIC import org.apache.spark.sql.types.StringType
# MAGIC
# MAGIC // Define UDF (in object to make it more Scala idiomatic)
# MAGIC object LeftScalaUdf {
# MAGIC     val code: String = "left_scala_udf"
# MAGIC     val leftScalaUdf: UserDefinedFunction = udf((strVal: String, len: Int) => {
# MAGIC         strVal.take(len)
# MAGIC     })
# MAGIC }
# MAGIC
# MAGIC // Register UDF
# MAGIC spark.udf.register(LeftScalaUdf.code, LeftScalaUdf.leftScalaUdf)

# COMMAND ----------

# Necessary imports
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define Python function
def left(str_val, len):
    return str_val[:len]

# Make it UDF
left_udf = udf(left, StringType(), useArrow=True)

# Register UDF
spark.udf.register("left_python_udf", left_udf)

# COMMAND ----------

# Prepare sample DataFramea
data = [("example1", "example1"), ("example2", "example1"), ("example3", "example1")]
columns = ["col1", "col2"]
df = spark.createDataFrame(data, columns)

# Display result
display(
    df.selectExpr(
        "left_scala_udf(col1, 3) as col1_left",
        "left_python_udf(col2, 3) as col2_left")
)
