from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg


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

class TestsFactory:
    def __init__(self):
        self.tests = [ 
            JoinGroupAverageTest(),
        ]

        self.tests_dict = { test.code: test for test in self.tests }
    def get_test_func(self, code: str) -> Test:
        try:
            return self.tests_dict[code].test_func
        except KeyError:
            raise ValueError(f"Unknown test code: {code}. Accepted " +
                f"codes are {' '.join(self.tests_dict.keys())} .")