# んょしtube

YouTube の検索・視聴・コメント閲覧に特化した高速ビューア。
Invidious / Piped / Sia / Sennin / RapidAPI を同時レースし、最初に返った有効応答を採用する。

## Render デプロイ

1. このリポジトリを GitHub に push
2. Render → New + → Web Service → リポジトリ選択
3. Runtime: **Docker** / Region: **Singapore** / Health Check Path: `/healthz`
4. 環境変数 `PYTHONUNBUFFERED=1`
5. Create Web Service

無料枠はスリープするため、`RENDER_EXTERNAL_URL` に発行URLを入れると自動キープアライブが動く。
