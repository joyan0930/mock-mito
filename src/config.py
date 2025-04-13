import os
import logging
from dotenv import load_dotenv

# .envファイルから環境変数を読み込む
load_dotenv()

# ロガーの設定
logger = logging.getLogger(__name__)

# Google CloudプロジェクトID
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
# BigQueryデータセットID
BIGQUERY_DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")
# BigQueryスキーマ定義テーブルID
BIGQUERY_SCHEMA_TABLE_ID = os.getenv("BIGQUERY_SCHEMA_TABLE_ID")

# DLP APIリージョン
DLP_API_LOCATION = os.getenv("DLP_API_LOCATION", "global") # デフォルトはglobal

# LLM APIエンドポイント (Vertex AI)
LLM_API_ENDPOINT = os.getenv("LLM_API_ENDPOINT")
LLM_MODEL_NAME = os.getenv("LLM_MODEL_NAME")

# 設定値の確認 (ロギング)
if not all([GOOGLE_CLOUD_PROJECT, BIGQUERY_DATASET_ID, BIGQUERY_SCHEMA_TABLE_ID, LLM_API_ENDPOINT, LLM_MODEL_NAME]):
    logger.warning("環境変数が一部設定されていません。動作に影響する可能性があります。")

logger.info(f"設定読み込み完了: Project={GOOGLE_CLOUD_PROJECT}, Dataset={BIGQUERY_DATASET_ID}, SchemaTable={BIGQUERY_SCHEMA_TABLE_ID}") 
