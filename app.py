import streamlit as st
import pandas as pd
from mitosheet.streamlit.v1 import spreadsheet
from streamlit_option_menu import option_menu
from src import data_handler, config, schema_manager

# --- 仮のデータと設定 ---
# TODO: src/schema_manager.py から読み込むように変更
DUMMY_SCHEMAS = {
    "product_master": {
        "columns": [
            {"name": "product_id", "type": "STRING", "constraints": ["NOT NULL", "UNIQUE"], "security_level": "C"},
            {"name": "product_name", "type": "STRING", "constraints": ["NOT NULL"], "security_level": "C"},
            {"name": "price", "type": "INTEGER", "constraints": [], "security_level": "C"},
            {"name": "description", "type": "STRING", "constraints": [], "security_level": "C"}
        ]
    },
    "customer_master": {
        "columns": [
            {"name": "customer_id", "type": "STRING", "constraints": ["NOT NULL", "UNIQUE"], "security_level": "C"},
            {"name": "name", "type": "STRING", "constraints": ["NOT NULL"], "security_level": "B"}, # 注意情報
            {"name": "email", "type": "STRING", "constraints": [], "security_level": "B"}, # 注意情報
            {"name": "address", "type": "STRING", "constraints": [], "security_level": "A"}, # 機密情報
            {"name": "phone_number", "type": "STRING", "constraints": [], "security_level": "B"} # 注意情報
        ]
    }
}

# TODO: src/bigquery_client.py から読み込むように変更
DUMMY_DATA = {
    "product_master": pd.DataFrame({
        "product_id": ["P001", "P002"],
        "product_name": ["Laptop", "Keyboard"],
        "price": [120000, 8000],
        "description": ["High-performance laptop", "Mechanical keyboard"]
    }),
    "customer_master": pd.DataFrame({
        "customer_id": ["C001", "C002"],
        "name": ["Taro Yamada", "Hanako Sato"],
        "email": ["taro@example.com", "hanako@example.com"],
        "address": ["Tokyo", "Osaka"],
        "phone_number": ["090-1234-5678", "080-9876-5432"]
    })
}

# -------------------------

st.set_page_config(layout="wide")
st.title("（仮称）マスターデータ管理・検査アプリケーション")

# --- サイドバー --- 
with st.sidebar:
    # ナビゲーションメニュー
    selected_menu = option_menu(
        menu_title="メインメニュー",
        options=["既存マスター編集", "新規マスター登録"],
        icons=['pencil-square', 'plus-circle'], # アイコン (Bootstrap Icons)
        menu_icon="cast",
        default_index=0, # デフォルトで「既存マスター編集」を選択
        orientation="vertical",
    )

# --- メインエリア --- 

if selected_menu == "新規マスター登録":
    st.header("新規マスター登録")
    st.info("新しいマスターテーブルの名前とスキーマ（カラム定義）を定義してください。")

    with st.form("new_master_form", clear_on_submit=True):
        new_master_name = st.text_input("新規マスター名", key="new_master_name_input")

        st.caption("カラム定義:")
        # スキーマ定義用データエディタ (session_state で管理)
        if 'new_schema_df' not in st.session_state:
            st.session_state.new_schema_df = pd.DataFrame([
                {"name": "id", "type": "STRING", "security_level": "C", "constraints": ["NOT NULL", "UNIQUE"]},
                {"name": "created_at", "type": "TIMESTAMP", "security_level": "C", "constraints": ["NOT NULL"]}
            ])

        edited_schema_df = st.data_editor(
            st.session_state.new_schema_df,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "name": st.column_config.TextColumn("カラム名", required=True),
                "type": st.column_config.SelectboxColumn(
                    "データ型",
                    options=["STRING", "INTEGER", "FLOAT", "BOOLEAN", "DATE", "TIMESTAMP", "JSON"],
                    required=True
                ),
                "security_level": st.column_config.SelectboxColumn(
                    "セキュリティレベル",
                    options=config.SECURITY_LEVELS,
                    required=True
                ),
                 "constraints": st.column_config.ListColumn(
                    "制約 (例: NOT NULL, UNIQUE)",
                    help="BigQueryでサポートされる制約を文字列リストで入力",
                )
            },
            key="schema_editor"
        )
        st.session_state.new_schema_df = edited_schema_df # 編集結果を保持

        submitted = st.form_submit_button("登録実行")
        if submitted:
            if new_master_name and not edited_schema_df.empty:
                # バリデーション
                if edited_schema_df['name'].duplicated().any():
                    st.error("エラー: カラム名が重複しています。")
                elif edited_schema_df['name'].isnull().any() or (edited_schema_df['name'] == "").any():
                    st.error("エラー: カラム名が空の行があります。")
                elif edited_schema_df['type'].isnull().any():
                    st.error("エラー: データ型が選択されていない行があります。")
                elif edited_schema_df['security_level'].isnull().any():
                    st.error("エラー: セキュリティレベルが選択されていない行があります。")
                else:
                    try:
                        # DataFrameをリスト形式に変換 (constraints も含める)
                        columns_to_save = edited_schema_df.to_dict('records')
                        # constraints が None や NaN の場合に空リストに変換
                        for col in columns_to_save:
                            if not isinstance(col.get('constraints'), list):
                                col['constraints'] = []

                        data_handler.create_new_master(new_master_name, columns_to_save)
                        st.success(f"マスター「{new_master_name}」を登録しました。")
                        # 成功したらフォームをリセットするための状態をクリア
                        st.session_state.new_schema_df = pd.DataFrame([
                            {"name": "id", "type": "STRING", "security_level": "C", "constraints": ["NOT NULL", "UNIQUE"]},
                            {"name": "created_at", "type": "TIMESTAMP", "security_level": "C", "constraints": ["NOT NULL"]}
                         ])
                        # new_master_name は clear_on_submit=True でクリアされるはず
                    except ValueError as ve:
                        st.error(f"登録エラー: {ve}")
                    except Exception as e:
                        st.error(f"予期せぬエラーが発生しました: {e}")
            elif not new_master_name:
                st.warning("マスター名を入力してください。")
            else:
                st.warning("カラム定義を1つ以上入力してください。")

elif selected_menu == "既存マスター編集":
    st.header("既存マスター編集")

    # --- マスター選択 --- 
    try:
        master_names = data_handler.get_master_list()
        if not master_names:
            st.warning("編集可能なマスターデータが登録されていません。まずは「新規マスター登録」からマスターを作成してください。")
            st.stop()
    except Exception as e:
        st.error(f"マスターリストの読み込みに失敗しました: {e}")
        st.stop()

    selected_master = st.selectbox(
        "編集するマスターを選択",
        options=master_names,
        key="selected_master_selector",
        index=0 # デフォルトはリストの最初
    )

    if selected_master:
        # --- スキーマ表示 (Expander内) ---
        with st.expander("選択中マスターのスキーマ定義", expanded=False):
            try:
                schema_info = data_handler.get_master_schema(selected_master)
                if schema_info:
                    schema_df = pd.DataFrame(schema_info.get("columns", []))
                    st.dataframe(schema_df, use_container_width=True, hide_index=True)
                    # TODO: スキーマ編集機能への導線
                    if st.button("スキーマを編集する (未実装)", key="edit_schema_button"):
                         st.warning("スキーマ編集機能は現在実装中です。")
                else:
                    st.warning("スキーマ情報が見つかりません。")
            except Exception as e:
                st.error(f"スキーマ情報の取得エラー: {e}")

        # --- データ編集 (Mito) ---
        st.subheader(f"「{selected_master}」のデータ編集")

        # 既存データ読み込み
        # @st.cache_data # キャッシュを一旦無効化して常に最新を読み込む
        def load_data(master):
             return data_handler.load_master_data(master)

        initial_df = None
        load_error = None

        with st.spinner(f"データをBigQueryから読み込み中..."):
            try:
                initial_df = load_data(selected_master)
                if not isinstance(initial_df, pd.DataFrame):
                    initial_df = None
                    raise TypeError("データ読み込み結果が DataFrameではありませんでした。")
            except Exception as e:
                load_error = e
                initial_df = None

        # --- 読み込み完了後の処理 ---
        if load_error:
            st.error(f"データの読み込みに失敗しました: {load_error}")
        elif initial_df is not None:
            st.success(f"データ読み込み完了 ({len(initial_df)} 件)")
            if initial_df.empty:
                st.info("現在データはありません。Mitoシート上で新規データを入力できます。")

            # デバッグ: Mitoに渡すデータを確認
            st.caption("Mitoに渡されるデータ (プレビュー):")
            st.dataframe(initial_df)

            # Mitoスプレッドシート表示
            st.info("Mitoを使用してデータを編集してください。編集後、「保存」ボタンを押してください。")
            final_dfs, code = spreadsheet(initial_df, key=f"mito_sheet_{selected_master}")

            if final_dfs and isinstance(final_dfs, list):
                edited_df = final_dfs[0]
                if initial_df.shape != edited_df.shape or not initial_df.equals(edited_df):
                    st.subheader("編集後のデータ (プレビュー)")
                    st.dataframe(edited_df)

                    if st.button("保存", key=f"save_button_{selected_master}"):
                        with st.spinner("保存処理を実行中..."):
                            try:
                                success, result = data_handler.save_master_data(selected_master, edited_df)
                                if success:
                                    st.success(f"マスター「{selected_master}」をBigQueryに正常に保存しました。")
                                    # load_data.clear() # キャッシュ削除に伴い不要
                                    st.rerun()
                                else:
                                    # エラー処理 (変更なし)
                                    if result and result.get("type") == "inspection_violation":
                                        st.error("データ検査の結果、セキュリティレベル違反が検出されたため、保存できませんでした。")
                                        st.warning("以下の違反が検出されました:")
                                        st.json(result.get("details", []))
                                    elif result and result.get("type") == "save_error":
                                        st.error(f"BigQueryへの保存中にエラーが発生しました: {result.get('message')}")
                                    else:
                                        st.error("不明なエラーにより保存に失敗しました。")
                            except Exception as e:
                                st.error(f"保存処理中に予期せぬエラーが発生しました: {e}")
                else:
                    st.caption("データに変更はありません。")
        # else: initial_df is None かつ load_error is None の場合 (通常は発生しない)
        #    st.warning("データの読み込み処理で問題が発生しました。")

        # --- マスター定義削除 --- 
        with st.expander("マスター定義の削除", expanded=False):
             st.warning("注意: この操作はマスターの定義情報のみを削除します。BigQuery上のテーブルとデータは削除されません。")
             if st.button("「{selected_master}」の定義を削除する", key=f"delete_button_{selected_master}"):
                 confirm_delete = st.checkbox("本当に削除しますか？", key=f"delete_confirm_{selected_master}")
                 if confirm_delete:
                    try:
                        data_handler.delete_master_definition(selected_master)
                        st.success(f"マスター「{selected_master}」の定義を削除しました。")
                        # キャッシュクリアとリロード
                        # load_data.clear() # 関連キャッシュもクリア
                        st.rerun()
                    except Exception as e:
                        st.error(f"マスター定義削除エラー: {e}")
    else:
         # selected_master が None の場合 (リストが空だった場合など)
         pass # 上の警告メッセージで処理は停止しているはず
