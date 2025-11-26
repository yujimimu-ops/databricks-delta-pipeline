# PySpark + Delta Lake ETL サンプル

このリポジトリは、PySpark と Delta Lake を用いた ETL パイプラインの最小サンプルです。  
実務の Databricks × ETL 構成（ETL クラス化 / Notebook 実行 / Delta 書き込み）に近い形で作成しています。

---

##  プロジェクト構成

your-repo/
├── notebooks/
│ └── etl_delta_demo.py # Notebook デモ（ETL の実行）
├── src/
│ └── etl.py # ETL ロジック（Load / Transform / Write）
├── data/
│ ├── sample_sales.csv # デモ用CSV（任意）
│ └── delta/ # Delta書き込み先
└── README.md

yaml
コードをコピーする

---

## 実行方法（PySpark / Databricks 共通）

1. SparkSession を生成  
2. CSV を読み込み  
3. 変換処理を実行  
4. Delta テーブルとして書き込み  

Notebook では以下のコードで ETL を実行しています：

```python
from pyspark.sql import SparkSession
from src.etl import DeltaETL

spark = SparkSession.builder.appName("demo-etl").getOrCreate()

etl = DeltaETL(spark)

df = etl.load_csv("./data/sample_sales.csv")
df_transformed = etl.transform_sales(df)
etl.write_delta(df_transformed, "./data/delta/sales")

spark.stop()
 ETL クラスの設計方針（src/etl.py）
Load / Transform / Write を明確に分離

Notebook にロジックを書かず、クラスに集約

Delta Lake を利用し、ACID 特性を保持

データ品質（NULL除去 / 型変換）を最低限実装

実務の Databricks プロジェクトで使われる構成に寄せている

使用技術
Python 3.x

PySpark

Delta Lake


 補足
このリポジトリは小規模でも

クラスタ構成

ETL パターン

Delta 書き込み

などを理解していることを示す目的で作成しています。
