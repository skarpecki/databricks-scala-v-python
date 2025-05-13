from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, format_string, rand, col
from .udf_registry import UdfRegistry


class Test:
    COLUMN = "rand_val"

    def test_func(spark: SparkSession) -> DataFrame:
        raise NotImplementedError
    
    @staticmethod
    def prepare_dataframe(spark: SparkSession, shift: int, print_plan: bool = False) -> DataFrame:
        df = ( 
            spark.range(0, 1 << shift )
                .toDF("id")
                .withColumn(Test.COLUMN, rand() * 10000)
                .select(format_string("%d", col(Test.COLUMN).cast("int")).alias(Test.COLUMN))
        )

        df.persist()
        df.count()
        if print_plan:
            df.explain(mode="cost") # to get size
        return df

class JoinGroupAverageTest(Test):
    code = "join_group_avg"
    def test_func(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        df_o = spark.read.table("bronze.default.orders")
        df_c = spark.read.table("bronze.default.customer")
        df_n = spark.read.table("bronze.default.nation")

        df = (
            df_o
            .join(
                df_c,
                on=df_o["o_custkey"] == df_c["c_custkey"],
                how="inner")
            .join(
                df_n,
                on=df_c["c_nationkey"] == df_n["n_nationkey"],
                how="inner")
            .groupBy("n_name")
            .agg(avg("o_totalprice").alias("average_totalprice"))
            .orderBy("n_name")
        )
        return df


class LeftSparkTest(Test):
    code = "left_spark"
    def test_func(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        return df.selectExpr(f"left({Test.COLUMN}, 3) AS {Test.COLUMN}") 


class LeftPythonArrowUdf(Test):
    """ 
    Remember to register UDFs before running
    """
    code = "left_python_arrow_udf"
    def test_func(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        return df.selectExpr(f"{UdfRegistry.LEFT_PYTHON_ARROW_UDF}({Test.COLUMN}, 3) AS {Test.COLUMN}")
    
class LeftPythonNonArrowUdf(Test):
    """ 
    Remember to register UDFs before running
    """
    code = "left_python_pickled_udf"
    def test_func(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        return df.selectExpr(f"{UdfRegistry.LEFT_PYTHON_PICKLED_UDF}({Test.COLUMN}, 3) AS {Test.COLUMN}")

class LeftScalaUdf(Test):
    """ 
    Remember to register UDFs before running
    """
    code = "left_scala_udf"
    def test_func(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        return df.selectExpr(f"{UdfRegistry.LEFT_SCALA_UDF}({Test.COLUMN}, 3) AS {Test.COLUMN}")

class LeftPandasUdf(Test):
    """ 
    Remember to register UDFs before running
    """
    code = "left_pandas_udf"
    def test_func(self, spark: SparkSession, df: DataFrame) -> DataFrame:
        return df.selectExpr(f"{UdfRegistry.LEFT_PANDAS_UDF}({Test.COLUMN}) AS {Test.COLUMN}")

class TestsFactory:
    def __init__(self):
        self.tests = [ 
            JoinGroupAverageTest(),
            LeftSparkTest(),
            LeftPythonArrowUdf(),
            LeftPythonNonArrowUdf(),
            LeftScalaUdf(),
            LeftPandasUdf()
        ]
        self.tests_dict = { test.code: test for test in self.tests }

    def get_test_func(self, code: str) -> Test:
        try:
            return self.tests_dict[code].test_func
        except KeyError:
            raise ValueError(f"Unknown test code: {code}. Accepted " +
                f"codes are {' '.join(self.tests_dict.keys())} .")