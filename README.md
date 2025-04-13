# （仮称）マスターデータ管理・検査アプリケーション

## 概要

ユーザーが自由にマスターデータのスキーマを定義し、データを登録・編集できるStreamlitアプリケーションです。
登録・編集されたデータは、保存時に機密情報が含まれていないか検査され、設定されたセキュリティレベルに基づいてBigQueryへの保存可否を判断します。

**主要機能:**

*   マスターテーブルの定義（スキーマ、セキュリティレベル設定） - *一部未実装*
*   Sheet形式（Mitoライブラリ利用）でのデータ作成、編集、管理
*   保存時のデータ検査（DLP、LLMによる個人情報等の検出） - *ダミー実装*
*   セキュリティレベルに基づいたBigQueryへのデータ保存（上書き）
*   BigQueryからの既存データの読み込みと編集

## 技術スタック

*   **フロントエンド:** Streamlit
*   **データ編集UI:** Mito
*   **バックエンド:** Python
*   **パッケージ管理:** uv
*   **コンテナ管理:** Docker Compose
*   **データベース:** Google BigQuery
*   **データ検査:** Google Cloud DLP, LLM (Vertex AI等) - *要実装*

## セットアップ

### 前提条件

*   Docker Desktop がインストールされていること
*   Google Cloud SDK (`gcloud`) がインストール・設定済みであること
*   `uv` がインストールされていること (`pip install uv`)
*   Google Cloud プロジェクトが作成済みで、課金が有効になっていること
*   BigQuery API, DLP API, Vertex AI API (または利用するLLMサービスのAPI) が有効になっていること
*   アプリケーションが利用する BigQuery データセットが作成済みであること
*   ローカル環境で Google Cloud にログインし、Application Default Credentials (ADC) を取得していること:
    ```bash
    gcloud auth application-default login
    ```

### 手順 (Docker Compose を利用)

1.  **リポジトリをクローン:**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **環境変数を設定:**
    *   `.env.example` をコピーして `.env` ファイルを作成します。
        ```bash
        cp .env.example .env
        ```
    *   `.env` ファイルを開き、ご自身の環境に合わせて以下の値を設定します。
        *   `GOOGLE_CLOUD_PROJECT`: ご自身のGCPプロジェクトID
        *   `BIGQUERY_DATASET_ID`: アプリケーションが使用するBigQueryデータセットID
        *   `BIGQUERY_SCHEMA_TABLE_ID`: (任意) スキーマ定義を保存するBigQueryテーブル名。デフォルトは `master_schema_definitions`。
        *   `SCHEMA_DEFINITION_PATH`: (任意) スキーマ定義ファイルのパス。デフォルトは `config/schemas.json`。
        *   `MITO_LICENSE_KEY`: (任意) Mitoの商用機能を利用する場合。
        *   `DLP_API_ENDPOINT`, `LLM_API_ENDPOINT`, `LLM_API_KEY`: データ検査サービスを実装後に設定。

3.  **Docker Compose で起動:**
    ```bash
    docker-compose up --build
    ```
    *   初回起動時または `Dockerfile` や依存関係に変更があった場合は `--build` オプションを付けます。
    *   バックグラウンドで実行する場合は `-d` オプションを追加します。

4.  **アプリケーションにアクセス:**
    Webブラウザで `http://localhost:8501` を開きます。

### 手順 (ローカル環境で直接実行 - 非推奨)

1.  **リポジトリをクローンして移動。**
2.  **仮想環境を作成して有効化:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate # Linux/macOS
    # .venv\Scripts\activate # Windows
    ```
3.  **依存関係をインストール:**
    ```bash
    uv pip install -r requirements.txt
    ```
4.  **環境変数を設定:**
    `.env` ファイルを作成し、上記 Docker Compose の手順と同様に設定します。
5.  **Streamlit アプリケーションを実行:**
    ```bash
    streamlit run app.py
    ```

## 使い方

1.  サイドバーで編集したいマスターを選択します。
2.  メインエリアに表示されるMitoスプレッドシートでデータを編集します。
3.  編集後、「保存」ボタンを押すと、データ検査が実行され、問題がなければBigQueryにデータが上書き保存されます。
4.  （未実装）サイドバーで新規マスターの登録やスキーマの編集ができます。

## 注意事項・TODO

*   **データ検査:** 現在のデータ検査 (`src/inspection_service.py`) はダミー実装です。実際のDLP/LLM API連携を実装する必要があります。
*   **スキーマ定義UI:** 新規マスター登録時のスキーマ定義入力UI、既存スキーマの編集UIは未実装です。
*   **エラーハンドリング:** 各所のエラーハンドリングは基本的なもののみです。より詳細なエラー表示やリカバリ処理が必要です。
*   **BigQueryスキーマ同期:** アプリケーション側でスキーマ定義を変更した場合、BigQuery側のテーブルスキーマとの同期は自動で行われません。必要に応じて手動での対応または同期機能の実装が必要です。
*   **BigQueryスキーマ定義テーブル:** アプリケーションはスキーマ定義を `BIGQUERY_SCHEMA_TABLE_ID` で指定されたテーブルに保存します。初回起動時にテーブルが存在しない場合は自動で作成されますが、必要な権限（`bigquery.tables.create`, `bigquery.tables.getData`, `bigquery.tables.updateData`, `bigquery.jobs.create`等）がサービスアカウントまたはADCに付与されている必要があります。
*   **セキュリティ:** Streamlitアプリケーション自体への認証・認可機能は実装されていません。必要に応じてStreamlitの機能や外部認証サービスとの連携を検討してください。
*   **Mito:** 大量データ (>100万セル程度) を扱う場合、Mitoのパフォーマンスに注意が必要です。
*   **ADCマウントパス:** `docker-compose.yml` 内のADC認証情報ボリューム (`~/.config/gcloud:/root/.config/gcloud:ro`) のパスは、環境によって異なる場合があります。適切に調整してください。

## ライセンス

(ライセンス情報を記載) 
