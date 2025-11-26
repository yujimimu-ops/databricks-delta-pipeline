# デモ用 PySpark + Delta ETL Notebook
# src/etl.py の DeltaETL を呼び出して ETL を実行する

from pyspark.sql import SparkSession
from src.etl import DeltaETL

# Spark セッション作成
spark = SparkSession.builder.appName("demo-etl").getOrCreate()

# DeltaETL クラスを初期化
etl = DeltaETL(spark)

# === ① データ読み込み ===
input_path = "./data/sample_sales.csv"
df = etl.load_csv(input_path)
print("▼ Raw Data")
df.show()

# === ② 変換処理 ===
df_transformed = etl.transform_sales(df)
print("▼ Transformed Data")
df_transformed.show()

# === ③ Delta形式で保存 ===
output_path = "./data/delta/sales"
etl.write_delta(df_transformed, output_path)
print(f"▼ Delta書き込み完了: {output_path}")

# Spark停止
spark.stop()
