import logging
import pandas as pd
from . import schema_manager, bigquery_client, inspection_service

# ロガーの設定
logger = logging.getLogger(__name__)

def load_master_data(master_name: str) -> pd.DataFrame:
    """マスターデータをBigQueryから読み込む"""
    # TODO: 必要に応じてキャッシュ処理などを追加
    return bigquery_client.load_data_from_bq(master_name)

def save_master_data(master_name: str, df: pd.DataFrame):
    """
    マスターデータを検査し、問題なければBigQueryに保存する。
    要件定義 3.4 および 4.2 に関連。
    """
    logger.info(f"マスター '{master_name}' の保存処理を開始します...")
    schema = schema_manager.get_schema(master_name)
    if not schema:
        logger.error(f"マスター '{master_name}' のスキーマ定義が見つかりません。保存処理を中止します。")
        raise ValueError(f"Schema not found for master: {master_name}")

    # inspection_serviceを使用してデータを検査
    # InspectionServiceのインスタンス化が必要
    inspector = inspection_service.InspectionService()
    violations = inspector.inspect_data(df, schema)

    if not violations:
        logger.info(f"データ検査の結果、問題ありませんでした。BigQueryへの保存を開始します。")
        try:
            bigquery_client.save_data_to_bigquery(master_name, df)
            logger.info(f"マスター '{master_name}' の保存が成功しました。")
        except Exception as e:
            logger.error(f"BigQueryへの保存中にエラーが発生しました: {e}", exc_info=True)
            # 保存失敗時のハンドリング (例: リトライ、エラー通知など)
            raise # エラーを再スローして呼び出し元に通知
    else:
        logger.warning(f"データ検査の結果、{len(violations)} 件の違反が検出されました。保存は行いません。")
        logger.warning("違反の詳細:")
        for viol in violations:
            logger.warning(f"  - {viol}")
        # 違反があった場合の処理 (例: ユーザーへの通知、修正の要求など)
        # ここでは例外を発生させて処理を中断させる
        raise ValueError(f"Data inspection failed for master '{master_name}'. Violations found: {violations}")

# --- マスター定義関連の処理 (app.pyから呼び出す用) ---
def get_master_list():
    return schema_manager.get_all_master_names()

def get_master_schema(master_name: str):
    return schema_manager.get_schema(master_name)

def create_new_master(master_name: str, schema_definition: dict):
    """
    新しいマスター定義を登録し、対応するBigQueryテーブルを作成し、ダミーデータを挿入する。
    要件定義 4.1 に関連。
    """
    # BigQueryClientのインスタンス化が必要
    bq_client = bigquery_client.BigQueryClient()

    try:
        # 1. スキーママネージャーに登録 & BigQueryにスキーマ保存
        schema_manager.register_schema(bq_client, master_name, schema_definition)
        logger.info(f"スキーマ定義 '{master_name}' を保存しました。")

        # 2. BigQueryにデータテーブルを作成
        try:
            bq_client.create_data_table(master_name, schema_definition)
            logger.info(f"データテーブル '{master_name}' を作成しました。")
        except Exception as e:
            logger.error(f"データテーブル '{master_name}' の作成に失敗しました: {e}", exc_info=True)
            # ロールバック処理: 登録したスキーマ定義を削除
            try:
                schema_manager.delete_schema(bq_client, master_name)
                logger.info(f"ロールバック: スキーマ定義 '{master_name}' を削除しました。")
            except Exception as rollback_e:
                logger.error(f"ロールバック中にエラーが発生しました: {rollback_e}", exc_info=True)
            raise # 元のエラーを再スロー

        # 3. ダミーデータを挿入 (任意、エラーでも処理は止めないことが多い)
        try:
            bq_client.insert_dummy_data(master_name, schema_definition)
            logger.info(f"ダミーデータをテーブル '{master_name}' に挿入しました。")
        except Exception as e:
            # ダミーデータ挿入失敗は警告に留めることが多い
            logger.warning(f"警告: ダミーデータの挿入に失敗しました ({master_name}): {e}", exc_info=True)

        logger.info(f"新規マスター '{master_name}' の初期セットアップが完了しました。")

    except Exception as e:
        logger.error(f"新規マスター '{master_name}' の作成プロセス中にエラーが発生しました: {e}", exc_info=True)
        raise # エラーを呼び出し元に通知

def update_master_schema(master_name: str, new_schema_definition: dict):
    """マスターのスキーマ定義を更新する"""
    bq_client = bigquery_client.BigQueryClient()
    try:
        schema_manager.update_schema(bq_client, master_name, new_schema_definition)
        logger.info(f"マスター '{master_name}' のスキーマ定義を更新しました。BigQuery側のデータテーブルスキーマ変更は別途必要になる場合があります。")
        # 注意: この関数はスキーマ定義の *メタデータ* を更新するだけ。
        # BigQueryのテーブルスキーマ自体の変更 (ALTER TABLE) はここでは行わない。
        # 必要であれば、別途マイグレーション処理などを実装する必要がある。
    except Exception as e:
        logger.error(f"マスター '{master_name}' のスキーマ定義更新中にエラー: {e}", exc_info=True)
        raise

def delete_master_definition(master_name: str):
    """マスターのスキーマ定義を削除する"""
    bq_client = bigquery_client.BigQueryClient()
    try:
        schema_manager.delete_schema(bq_client, master_name)
        logger.info(f"マスター '{master_name}' の定義を削除しました。BigQuery側のデータテーブルは手動で削除してください。")
        # 注意: この関数はスキーマ定義の *メタデータ* を削除するだけ。
        # BigQueryのテーブル自体は削除しない。
    except Exception as e:
        logger.error(f"マスター '{master_name}' の定義削除中にエラー: {e}", exc_info=True)
        raise 
