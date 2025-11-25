# databricks-delta-pipeline
Databricks × Delta Lake ETL sample project
Databricks Delta Lake ETL Pipeline Sample
Databricks Delta Lake ETL Pipeline Sample
概要
このリポジトリは Azure Databricks と Delta Lake を用いた ETL パイプラインの基本構成を再現したサンプルです。
実務で得たクラウド移行やデータパイプライン運用の知見を、再現可能な形でまとめています。
処理フロー:
生データ → 前処理 → 集約 → Delta テーブル公開
を Notebook 形式で整理しています。

目的

Databricks を用いたデータ処理の理解を深める
Delta Lake の ACID トランザクション / MERGE を使った更新処理の実例を提示
実務レベルの ETL 設計を再現
技術理解を外部から確認可能な形で共有（技術選考用）


使用技術

Azure Databricks
PySpark
Delta Lake
Databricks Workflow（想定）
Python 3.x


ディレクトリ構成
/notebooks
   ├── 01_load_raw_data.py
   ├── 02_cleaning_processing.py
   ├── 03_aggregate.py
   └── 04_publish_delta_tables.py

/data_sample
   └── sample_data.csv

README.md


実行方法

Databricks Workspace に notebooks ディレクトリをインポート
サンプルデータを DBFS にアップロード
Notebook を上から順に実行
Delta テーブルが作成され、MERGE による更新が可能になります


工夫したポイント

Delta Lake による SCD Type2 風の MERGE 処理を実装
運用で想定される「部分差分更新」を再現
実運用で行っていた ログ出力やデータ品質チェックを簡易的に導入
Notebook の章構成を、実務パイプライン構成と一致させて設計


今後の改善

ジョブ化（Databricks Workflow）のサンプル追加
エラーハンドリングの強化
pytest による ETL 単体テストの追加
Lakehouse 全体アーキテクチャ図の追加
