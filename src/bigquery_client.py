from google.cloud import bigquery
import pandas as pd
from . import config, schema_manager
import json
from typing import Dict
import logging
import random
import string
from datetime import datetime
from google.cloud.exceptions import NotFound, Conflict
from google.api_core.exceptions import GoogleAPICallError, BadRequest

# ロガーの設定
logger = logging.getLogger(__name__)

# BigQueryクライアントの初期化 (ADCを使用)
# 環境変数 GOOGLE_APPLICATION_CREDENTIALS が設定されているか、
# gcloud auth application-default login が実行されていれば自動で認証されます。
client = bigquery.Client(project=config.GOOGLE_CLOUD_PROJECT)
dataset_ref = client.dataset(config.BIGQUERY_DATASET_ID)

schema_table_id = f"{config.GOOGLE_CLOUD_PROJECT}.{config.BIGQUERY_DATASET_ID}.{config.BIGQUERY_SCHEMA_TABLE_ID}"

def load_data_from_bq(master_name: str) -> pd.DataFrame:
    """指定されたマスターのデータをBigQueryから読み込む"""
    table_id = f"{config.GOOGLE_CLOUD_PROJECT}.{config.BIGQUERY_DATASET_ID}.{master_name}"
    logger.info(f"[BQ Client] BigQueryからデータを読み込み開始: {table_id}")

    try:
        logger.debug(f"[BQ Client] テーブル存在確認: {table_id}")
        table = client.get_table(table_id) # テーブルオブジェクトを取得
        logger.info(f"[BQ Client] テーブル確認完了. テーブル行数: {table.num_rows}")
    except Exception as e:
        if "Not found" in str(e):
            logger.info(f"[BQ Client] テーブル {table_id} が存在しません。空のデータを返します。")
            # スキーマに基づいて空のDataFrameを作成
            schema_def = schema_manager.get_schema(master_name)
            if schema_def:
                columns = [col['name'] for col in schema_def.get('columns', [])]
                logger.debug(f"[BQ Client] スキーマから空のDataFrameを作成: Columns={columns}")
                return pd.DataFrame(columns=columns)
            else:
                logger.warning("[BQ Client] スキーマ定義が見つからないため、完全に空のDataFrameを返します。")
                return pd.DataFrame()
        else:
            logger.error(f"[BQ Client] テーブル情報の取得中にエラーが発生しました: {e}")
            raise # 予期せぬエラーは再raise

    # テーブルが存在する場合のデータ読み込み
    try:
        query = f"SELECT * FROM `{table_id}`"
        logger.debug(f"[BQ Client] クエリ実行: {query}")
        # .to_dataframe() は内部でイテレータを処理する
        df = client.query(query).to_dataframe()
        logger.info(f"[BQ Client] クエリ完了、DataFrame変換後: Shape={df.shape}")
        if not df.empty:
            logger.debug(f"[BQ Client] DataFrameの内容 (先頭5件):\n{df.head().to_string()}")
        else:
            logger.info("[BQ Client] DataFrameは空です。")
        return df
    except Exception as e:
        logger.error(f"[BQ Client] クエリ実行またはDataFrame変換中にエラーが発生しました: {e}")
        logger.warning("[BQ Client] エラーのため、空のDataFrameを返します。")
        return pd.DataFrame()

def save_data_to_bq(master_name: str, df: pd.DataFrame):
    """指定されたマスターのデータをBigQueryに上書き保存する (WRITE_TRUNCATE)"""
    table_id = f"{config.GOOGLE_CLOUD_PROJECT}.{config.BIGQUERY_DATASET_ID}.{master_name}"
    logger.info(f"BigQueryへデータを保存中: {table_id} ({len(df)} 件)")

    # スキーマ定義を取得
    schema_def = schema_manager.get_schema(master_name)
    if not schema_def:
        raise ValueError(f"マスター '{master_name}' のスキーマ定義が見つかりません。")

    # BigQueryのスキーマを定義 (schema_managerの定義から変換)
    bq_schema = []
    column_map = {col['name']: col['type'] for col in schema_def.get("columns", [])}
    for col_name in df.columns:
        bq_type = column_map.get(col_name, "STRING") # 不明な場合はSTRINGとする
        # TODO: より厳密な型マッピング (例: INTEGER, FLOAT, DATE, TIMESTAMP)
        bq_schema.append(bigquery.SchemaField(col_name, bq_type))

    # DataFrameのデータ型をBigQueryの型に合わせる (必要に応じて)
    # 例: df['price'] = df['price'].astype(float)

    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # テーブルを上書き
        # 必要に応じてパーティションやクラスタリングの設定を追加
    )

    try:
        load_job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        logger.info(f"  Load job {load_job.job_id} を開始しました。")

        load_job.result() # ジョブの完了を待つ

        destination_table = client.get_table(table_id)
        logger.info(f"BigQueryへのデータ保存完了: {table_id}, Total rows: {destination_table.num_rows}")

    except Exception as e:
        logger.error(f"BigQueryへのデータ保存中にエラーが発生しました: {e}")
        # エラーの詳細を取得 (ジョブのエラーなど)
        if hasattr(load_job, 'errors') and load_job.errors:
            logger.error("  Job errors:")
            for error in load_job.errors:
                logger.error(f"    Reason: {error['reason']}, Message: {error['message']}")
        raise

# --- スキーマ定義テーブル操作関数 ---

def _get_schema_table() -> bigquery.Table:
    """スキーマ定義テーブルを取得または作成する"""
    try:
        table = client.get_table(schema_table_id)
        logger.info(f"スキーマ定義テーブル {schema_table_id} が存在します。")
        return table
    except Exception as e:
        if "Not found" in str(e):
            logger.info(f"スキーマ定義テーブル {schema_table_id} が存在しません。新規作成します。")
            schema = [
                bigquery.SchemaField("master_name", "STRING", mode="REQUIRED"),
                # JSON型でスキーマ定義全体を保存 (カラム定義などを含む)
                bigquery.SchemaField("schema_definition", "JSON", mode="REQUIRED"),
                # 最終更新日時などを追加しても良い
                # bigquery.SchemaField("last_updated", "TIMESTAMP", mode="REQUIRED"),
            ]
            table = bigquery.Table(schema_table_id, schema=schema)
            try:
                table = client.create_table(table)
                logger.info(f"スキーマ定義テーブル {schema_table_id} を作成しました。")
                return table
            except Exception as create_error:
                logger.error(f"スキーマ定義テーブルの作成に失敗しました: {create_error}")
                raise
        else:
            logger.error(f"スキーマ定義テーブルの取得中にエラーが発生しました: {e}")
            raise

def load_all_schema_definitions() -> Dict[str, Dict]:
    """BigQueryから全てのスキーマ定義を読み込む"""
    _get_schema_table() # テーブルが存在しなければ作成
    logger.info(f"BigQueryからスキーマ定義を読み込み中: {schema_table_id}")
    schemas = {}
    try:
        query = f"SELECT master_name, schema_definition FROM `{schema_table_id}`"
        query_job = client.query(query)

        for row in query_job:
            master_name = row["master_name"]
            # JSON型のカラムは文字列として取得されるため、パースする
            try:
                schema_def_str = row["schema_definition"]
                if schema_def_str: # Nullや空文字列でないことを確認
                     schemas[master_name] = json.loads(schema_def_str)
                # else:
                     # print(f"警告: master_name '{master_name}' の schema_definition が空です。") # 警告表示をコメントアウト
            except json.JSONDecodeError as json_err:
                logger.error(f"エラー: master_name '{master_name}' のスキーマ定義JSONのパースに失敗しました: {json_err}")
            except Exception as parse_err:
                 logger.error(f"エラー: master_name '{master_name}' のスキーマ定義処理中に予期せぬエラー: {parse_err}")

        logger.info(f"BigQueryからのスキーマ定義読み込み完了: {len(schemas)} 件")
        return schemas
    except Exception as e:
        logger.error(f"BigQueryからのスキーマ定義読み込み中にエラーが発生しました: {e}")
        return {} # エラー時は空辞書を返す

def save_schema_definition(master_name: str, schema_definition: Dict):
    """スキーマ定義をBigQueryに保存（上書き）する"""
    _get_schema_table() # テーブルが存在しなければ作成
    logger.info(f"スキーマ定義をBigQueryに保存中: {master_name}")
    try:
        # スキーマ定義をJSON文字列に変換
        schema_def_json = json.dumps(schema_definition, ensure_ascii=False)

        # MERGE文を使用してUPSERT（存在すればUPDATE、存在しなければINSERT）を行う
        merge_sql = f"""
        MERGE `{schema_table_id}` T
        USING (SELECT @master_name AS master_name, @schema_def AS schema_definition) S
        ON T.master_name = S.master_name
        WHEN MATCHED THEN
          UPDATE SET schema_definition = S.schema_definition
        WHEN NOT MATCHED THEN
          INSERT (master_name, schema_definition) VALUES (S.master_name, S.schema_definition)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("master_name", "STRING", master_name),
                bigquery.ScalarQueryParameter("schema_def", "JSON", schema_def_json),
            ]
        )

        query_job = client.query(merge_sql, job_config=job_config)
        query_job.result() # クエリの完了を待つ
        logger.info(f"スキーマ定義の保存完了: {master_name}")
    except Exception as e:
        logger.error(f"スキーマ定義の保存中にエラーが発生しました ({master_name}): {e}")
        raise

def delete_schema_definition(master_name: str):
    """スキーマ定義をBigQueryから削除する"""
    _get_schema_table() # テーブルが存在しなければ作成 (エラー防止)
    logger.info(f"スキーマ定義をBigQueryから削除中: {master_name}")
    try:
        delete_sql = f"DELETE FROM `{schema_table_id}` WHERE master_name = @master_name"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("master_name", "STRING", master_name),
            ]
        )
        query_job = client.query(delete_sql, job_config=job_config)
        query_job.result() # クエリの完了を待つ
        logger.info(f"スキーマ定義の削除完了: {master_name}")
    except Exception as e:
        logger.error(f"スキーマ定義の削除中にエラーが発生しました ({master_name}): {e}")
        raise

# --- データテーブル操作関数 ---

def _bq_type_mapper(app_type: str) -> str:
    """アプリケーション内の型名をBigQueryの型名に変換する"""
    type_map = {
        "STRING": "STRING",
        "INTEGER": "INT64", # または INTEGER
        "FLOAT": "FLOAT64", # または FLOAT
        "BOOLEAN": "BOOL", # または BOOLEAN
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
        "JSON": "JSON",
        # 必要に応じて他の型を追加
    }
    return type_map.get(app_type.upper(), "STRING") # 不明な場合はSTRING

def create_data_table(master_name: str, schema_definition: Dict):
    """スキーマ定義に基づいて新しいデータテーブルをBigQueryに作成する"""
    table_id = f"{config.GOOGLE_CLOUD_PROJECT}.{config.BIGQUERY_DATASET_ID}.{master_name}"
    logger.info(f"データテーブルを作成中: {table_id}")

    try:
        # 既存チェック (存在する場合はエラーにするか、何もしないか)
        client.get_table(table_id)
        logger.warning(f"警告: データテーブル {table_id} は既に存在します。作成をスキップします。")
        return # 既に存在する場合は何もしない
    except Exception as e:
        if "Not found" not in str(e):
            logger.error(f"データテーブルの存在確認中にエラーが発生しました: {e}")
            raise # 予期せぬエラーは再raise
        # Not found の場合は処理を続行

    # BigQueryのスキーマを構築
    bq_schema = []
    columns = schema_definition.get("columns", [])
    if not columns:
         logger.warning(f"スキーマ定義 '{master_name}' にカラムが含まれていないため、空のテーブルを作成します。")
         # 空でもテーブルは作成できる
    else:
        for col_def in columns:
            col_name = col_def.get("name")
            app_type = col_def.get("type", "STRING")
            constraints = col_def.get("constraints", [])

            if not col_name:
                raise ValueError("カラム定義に名前が指定されていません。")

            bq_type = _bq_type_mapper(app_type)
            # NOT NULL制約があれば REQUIRED に設定
            mode = "REQUIRED" if "NOT NULL" in [c.upper() for c in constraints] else "NULLABLE"

            bq_schema.append(bigquery.SchemaField(col_name, bq_type, mode=mode))

    # テーブルオブジェクト作成
    table = bigquery.Table(table_id, schema=bq_schema)

    # テーブル作成実行
    try:
        created_table = client.create_table(table)
        logger.info(f"データテーブル {created_table.table_id} を作成しました。")
    except Exception as create_error:
        logger.error(f"データテーブル {table_id} の作成に失敗しました: {create_error}")
        raise

def insert_dummy_data(master_name: str, schema_definition: Dict):
    """指定されたデータテーブルに型に基づいたダミーデータを1行挿入する"""
    table_id = f"{config.GOOGLE_CLOUD_PROJECT}.{config.BIGQUERY_DATASET_ID}.{master_name}"
    logger.info(f"ダミーデータを挿入中: {table_id}")
    columns = schema_definition.get("columns", [])
    if not columns:
        logger.warning(f"警告: マスター '{master_name}' のカラム定義が空のため、ダミーデータを挿入できません。")
        return

    # ダミーデータ行を作成
    dummy_row = {}
    from datetime import datetime, date
    import json

    for col_def in columns:
        col_name = col_def.get("name")
        app_type = col_def.get("type", "STRING").upper()
        constraints = col_def.get("constraints", [])
        is_required = "NOT NULL" in [c.upper() for c in constraints]

        if not col_name:
            continue # 名前がないカラムはスキップ

        # 型に応じたダミー値を生成
        dummy_value = None
        if app_type == "STRING":
            dummy_value = f"dummy_{col_name}"
        elif app_type == "INTEGER":
            dummy_value = 0
        elif app_type == "FLOAT":
            dummy_value = 0.0
        elif app_type == "BOOLEAN":
            dummy_value = False
        elif app_type == "DATE":
            # BigQueryは 'YYYY-MM-DD' 形式の文字列を受け付ける
            dummy_value = date.today().isoformat()
        elif app_type == "TIMESTAMP":
            # BigQueryは 'YYYY-MM-DD HH:MM:SS[.ffffff][+HH:MM]' 形式。UTC推奨
            dummy_value = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f UTC')
        elif app_type == "JSON":
            # BigQueryはJSON文字列またはdictを受け付ける (insert_rows_jsonの場合)
            dummy_value = {"dummy_key": f"value_for_{col_name}"}
        else:
            dummy_value = "unknown_type_dummy"

        # REQUIRED なのに値が None の場合 (通常は発生しないはずだが念のため)
        if is_required and dummy_value is None:
             logger.warning(f"警告: REQUIREDカラム '{col_name}' のダミー値が生成できませんでした。デフォルト値を使用します。")
             if app_type == "STRING": dummy_value = ""
             elif app_type == "INTEGER": dummy_value = 0
             # ... 他の型も必要に応じてデフォルト値設定

        dummy_row[col_name] = dummy_value

    # データを挿入
    try:
        errors = client.insert_rows_json(table_id, [dummy_row])
        if errors == []:
            logger.info(f"ダミーデータの挿入成功: {table_id}")
        else:
            logger.error(f"ダミーデータの挿入中にエラーが発生しました: {table_id}")
            for error in errors:
                logger.error(f"  Row data: {error.get('row')}")
                logger.error(f"  Errors: {error.get('errors')}")
    except Exception as e:
        logger.error(f"ダミーデータの挿入中に予期せぬエラーが発生しました: {e}")
        # テーブル作成直後でスキーマが不一致になるケースは少ないはずだが、エラーはログに残す

# --- 必要に応じて他の関数を追加 ---
# 例: テーブル作成、スキーマ更新など 
