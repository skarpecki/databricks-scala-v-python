from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg


class Test:
    def test_func(spark: SparkSession) -> DataFrame:
        raise NotImplementedError


class JoinGroupAverageTest(Test):
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
    @staticmethod
    def get_test_func(code: str) -> Test:
        if code == 'join_group_avg':
            return JoinGroupAverageTest().test_func