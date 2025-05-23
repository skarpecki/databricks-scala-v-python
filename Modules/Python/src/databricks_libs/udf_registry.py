import pyspark.sql.functions as F
from pyspark.sql.types import StringType
import pandas as pd

class UdfRegistry:
    LEFT_PYTHON_ARROW_UDF = "left_python_arrow_udf"
    LEFT_PYTHON_PICKLED_UDF = "left_python_pickled_udf"
    LEFT_PANDAS_UDF = "left_pandas_udf"
    LEFT_SCALA_UDF = "left_scala_udf"

    def _left_udf_factory(self, useArrow: bool):
        def _left_udf(str_val, len):
            return str_val[:len]
        return F.udf(_left_udf, StringType(), useArrow=useArrow)
    
    def _left_pandas_udf_factory(self, n):
        def _left_pandas_udf(s: pd.Series) -> pd.Series:
            return s.str.slice(0, n) 
        return F.pandas_udf(_left_pandas_udf, StringType())
    
    def register_udf(self, spark):
        # Left python udf
        spark.udf.register(UdfRegistry.LEFT_PYTHON_ARROW_UDF, self._left_udf_factory(useArrow=True))
        spark.udf.register(UdfRegistry.LEFT_PYTHON_PICKLED_UDF, self._left_udf_factory(useArrow=False))
        spark.udf.register(UdfRegistry.LEFT_PANDAS_UDF, self._left_pandas_udf_factory(3))