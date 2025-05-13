import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import pandas as pd

class UdfRegistry:
    LEFT_PYTHON_UDF = "left_python_udf"
    LEFT_PANDAS_UDF = "left_pandas_udf"
    LEFT_SCALA_UDF = "left_scala_udf"

    def _left_udf(self, str_val, len):
        return str_val[:len]
    
    def _left_pandas_udf_factory(self, n):
        def left_pandas_udf(s: pd.Series) -> pd.Series:
            return s.str.slice(0, 3) 
        return F.pandas_udf(left_pandas_udf, StringType())
    
    def register_udf(self, spark):
        # Left python udf
        spark.udf.register(UdfRegistry.LEFT_PYTHON_UDF, F.udf(self._left_udf, StringType()))
        spark.udf.register(UdfRegistry.LEFT_PANDAS_UDF, F.udf(self._left_pandas_udf_factory(3), StringType()))