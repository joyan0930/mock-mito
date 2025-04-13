FROM python:3.13-slim

WORKDIR /app

# UVをインストールに必要なライブラリをインストール
RUN apt-get update -y \
    && apt-get install -y curl ca-certificates poppler-utils \
    && apt-get -y clean all

# UVのインストール
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh

# UVを使用するに必要なパスを通す
ENV PATH="/root/.local/bin/:$PATH"

# UV用環境変数の設定
ENV UV_SYSTEM_PYTHON=true \ 
    UV_COMPILE_BYTCODE=1 \
    UV_CAHE_DIR=/root/.cache/uv \
    UV_LINK_MODE=copy \
    PYTHONPATH=/app

# 環境変数を適切に設定
ENV PORT=8080

# アプリケーションのコピー
COPY . .

# 依存関係ファイルのみをコピーしてキャッシュ効率を向上
COPY pyproject.toml uv.lock ./

# uv sync で依存関係を仮想環境にインストール (プロジェクト自体は含めない)
# キャッシュマウントを修正 (uv 0.1.17+)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# アプリが使うパッケージをインストール
RUN uv export --frozen --no-dev --format requirements-txt > requirements.txt \ 
    && uv pip install -r requirements.txt

# Streamlitの設定をオーバーライドするための.streamlitディレクトリを作成
RUN if [ "$SERVICE_TYPE" = "ui" ]; then \
    mkdir -p /root/.streamlit && \
    echo "[server]" > /root/.streamlit/config.toml && \
    echo "port = 8080" >> /root/.streamlit/config.toml && \
    echo "address = \"0.0.0.0\"" >> /root/.streamlit/config.toml; \
fi

# 8080ポートのみを公開
EXPOSE 8080

# 起動コマンド
CMD ["streamlit", "run", "app.py"]
