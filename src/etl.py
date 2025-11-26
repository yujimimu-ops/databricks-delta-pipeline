from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class DeltaETL:
    """ETL 用の共通クラス"""

    def __init__(self, spark):
        self.spark = spark

    def load_csv(self, path: str) -> DataFrame:
        """CSV を DataFrame としてロード"""
        return (
            self.spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(path)
        )

    def transform_sales(self, df: DataFrame) -> DataFrame:
        """売上データの簡易変換処理"""

        df_clean = (
            df.dropna(subset=["amount"])
              .withColumn("amount", col("amount").cast("double"))
        )

        return df_clean

    def write_delta(self, df: DataFrame, path: str, mode: str = "overwrite"):
        """Delta テーブルとして書き出し"""
        (
            df.write.format("delta")
            .mode(mode)
            .save(path)
        )
