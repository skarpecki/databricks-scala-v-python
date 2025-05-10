import pyspark.sql.functions as F
from pyspark.sql.types import StringType

class UdfRegistry:
    LEFT_PYTHON_UDF = "left_python_udf"
    LEFT_SCALA_UDF = "left_scala_udf"
    
    def __init__(self, spark):
        self.spark = spark

    @staticmethod
    def _left_udf(str_val, len):
        return str_val[:len]
    
    def register_udf(self):
        # Left python udf
        left_python_udf = F.udf(UdfRegistry._left_udf, StringType())
        self.spark.udf.register(UdfRegistry.LEFT_PYTHON_UDF, left_python_udf)