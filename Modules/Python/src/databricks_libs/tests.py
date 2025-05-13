from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg
from .udf_registry import UdfRegistry


class Test:
    def test_func(spark: SparkSession) -> DataFrame:
        raise NotImplementedError

class JoinGroupAverageTest(Test):
    code = "join_group_avg"
    def test_func(self, spark) -> DataFrame:
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
    def test_func(self, spark) -> DataFrame:
        df = (
            spark.read.table("bronze.default.orders")
                .selectExpr("left(o_comment, 3) AS left_o_comment")
                .orderBy("left_o_comment") # To force executing for all rows
        )
        return df


class LeftPythonUdf(Test):
    """ 
    Remember to register UDFs before running
    """
    code = "left_python_udf"
    def test_func(self, spark) -> DataFrame:
        df = (
            spark.read.table("bronze.default.orders")
                .selectExpr(f"{UdfRegistry.LEFT_PYTHON_UDF}(o_comment, 3) AS left_o_comment")
                .orderBy("left_o_comment") # To force executing for all rows
        )

        return df

class LeftScalaUdf(Test):
    """ 
    Remember to register UDFs before running
    """
    code = "left_scala_udf"
    def test_func(self, spark) -> DataFrame:
        df = (
            spark.read.table("bronze.default.orders")
                .selectExpr(f"{UdfRegistry.LEFT_SCALA_UDF}(o_comment, 3) AS left_o_comment")
                .orderBy("left_o_comment") # To force executing for all rows
        )

        return df

class LeftPandasUdf(Test):
    """ 
    Remember to register UDFs before running
    """
    code = "left_pandas_udf"
    def test_func(self, spark) -> DataFrame:
        df = (
            spark.read.table("bronze.default.orders")
                .selectExpr(f"{UdfRegistry.LEFT_PANDAS_UDF}(o_comment) AS left_o_comment")
                .orderBy("left_o_comment") # To force executing for all rows
        )

        return df

class TestsFactory:
    def __init__(self):
        self.tests = [ 
            JoinGroupAverageTest(),
            LeftSparkTest(),
            LeftPythonUdf(),
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