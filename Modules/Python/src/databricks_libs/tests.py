class Test:
    def test_func():
        raise NotImplementedError


class ReadWriteTest(Test):
    def __init__(self, spark):
        self.spark = spark
    def test_func(self):
        n = 7
        table = "samples.tpch.orders"

        df = self.spark.read.table(table)
        for _ in range(n):
            df = df.union(
                self.spark.read.table(table)
            )

        (
            df.write
            .mode("overwrite")
            .saveAsTable("bronze.default.orders")
        )

        return df

class TestsFactory:
    @staticmethod
    def get_test_func(code: str, spark) -> Test:
        if code == 'rw_test':
            return ReadWriteTest(spark).test_func