import json
import logging
import os
from . import config, bigquery_client
from typing import Dict, List, Any, Optional

# ロガーの設定
logger = logging.getLogger(__name__)

# スキーマ定義を格納する辞書 (メモリキャッシュ)
_schemas: Dict[str, Dict] = {}
_initialized = False

def _initialize_schemas():
    """アプリケーション起動時にBigQueryからスキーマ定義を読み込む"""
    global _schemas, _initialized
    if not _initialized:
        logger.info("スキーマ定義の初期化を開始します...")
        try:
            _schemas = bigquery_client.load_all_schema_definitions()
            _initialized = True
            logger.info(f"スキーマ定義の初期化完了。{len(_schemas)} 件のマスターをロードしました。")
        except Exception as e:
            logger.error(f"スキーマ定義の初期化中にエラーが発生しました: {e}", exc_info=True)
            # エラーが発生しても空の辞書で続行する（あるいはアプリを停止させる）
            _schemas = {}
            _initialized = True # 再試行を防ぐために True にする

# モジュール読み込み時に初期化を実行
_initialize_schemas()

def get_all_master_names() -> List[str]:
    """登録されているすべてのマスター名を取得する (メモリキャッシュから)"""
    # _initialize_schemas() # 既に初期化済みのはずだが、念のため呼ぶことも可能
    return list(_schemas.keys())

def get_schema(master_name: str) -> Dict[str, Any] | None:
    """指定されたマスターのスキーマ定義を取得する (メモリキャッシュから)"""
    return _schemas.get(master_name)

def add_master(master_name: str, columns: List[Dict]):
    """新しいマスターとそのスキーマを登録する"""
    if master_name in _schemas:
        raise ValueError(f"マスター '{master_name}' は既に存在します。")
    # TODO: スキーマのバリデーション (カラム名、型、セキュリティレベルなど)
    new_schema = {"columns": columns}
    try:
        bigquery_client.save_schema_definition(master_name, new_schema)
        _schemas[master_name] = new_schema # メモリキャッシュも更新
        print(f"新規マスター '{master_name}' を登録し、BigQueryに保存しました。")
    except Exception as e:
        print(f"マスター '{master_name}' の登録・保存中にエラー: {e}")
        raise # エラーを呼び出し元に伝える

def update_schema(master_name: str, columns: List[Dict]):
    """既存マスターのスキーマを更新する"""
    if master_name not in _schemas:
        raise ValueError(f"マスター '{master_name}' が存在しません。")
    # TODO: スキーマのバリデーション
    updated_schema = {"columns": columns}
    try:
        bigquery_client.save_schema_definition(master_name, updated_schema)
        _schemas[master_name] = updated_schema # メモリキャッシュも更新
        print(f"マスター '{master_name}' のスキーマを更新し、BigQueryに保存しました。")
    except Exception as e:
        print(f"マスター '{master_name}' のスキーマ更新・保存中にエラー: {e}")
        raise

def delete_master(master_name: str):
    """マスター定義を削除する"""
    if master_name not in _schemas:
        raise ValueError(f"マスター '{master_name}' が存在しません。")
    try:
        bigquery_client.delete_schema_definition(master_name)
        del _schemas[master_name] # メモリキャッシュから削除
        print(f"マスター '{master_name}' を削除し、BigQueryからも削除しました。")
    except Exception as e:
        print(f"マスター '{master_name}' の削除中にエラー: {e}")
        raise

# --- 初期化処理 ---
# _initialize_schemas() はモジュール読み込み時に実行される 
