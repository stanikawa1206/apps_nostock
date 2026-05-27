# apps/etc/message_viewer.py

from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

import sys
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from flask import Flask, render_template_string
from apps.common.utils import get_sql_server_connection

app = Flask(__name__)

HTML = """
<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<title>eBay Messages</title>
<style>
  body { font-family: Arial; margin: 20px; background: #f5f5f5; }
  h1   { color: #333; }
  table { width: 100%; border-collapse: collapse; background: white; }
  th   { background: #1a4b7a; color: white; padding: 10px; text-align: left; }
  td   { padding: 10px; border-bottom: 1px solid #ddd; vertical-align: top; }
  tr:hover { background: #f0f7ff; }
  .auto_reply   { background: #e8f5e9; }
  .needs_review { background: #fff8e1; }
  .badge-auto   { background: #4caf50; color: white; padding: 2px 8px; border-radius: 10px; font-size: 12px; }
  .badge-review { background: #ff9800; color: white; padding: 2px 8px; border-radius: 10px; font-size: 12px; }
  .body-text    { max-width: 300px; }
  .draft        { max-width: 300px; color: #555; font-size: 13px; }
</style>
</head>
<body>
<h1>📨 eBay Messages</h1>
<p>{{ messages|length }}件</p>
<table>
  <tr>
    <th>受信日時</th>
    <th>アカウント</th>
    <th>送信者</th>
    <th>商品</th>
    <th>本文</th>
    <th>分類</th>
    <th>返信案（英）</th>
    <th>返信案（和）</th>
  </tr>
  {% for m in messages %}
  <tr class="{{ m.category }}">
    <td>{{ m.received_at }}</td>
    <td>{{ m.account }}</td>
    <td>{{ m.sender_id }}</td>
    <td style="max-width:150px;">{{ m.item_title or m.item_id }}</td>
    <td class="body-text">{{ m.body_text }}</td>
    <td>
      {% if m.category == 'auto_reply' %}
        <span class="badge-auto">自動返信</span><br>{{ m.auto_reply_type }}
      {% else %}
        <span class="badge-review">要確認</span>
      {% endif %}
    </td>
    <td class="draft">{{ m.reply_draft_en or '―' }}</td>
    <td class="draft">{{ m.reply_draft_ja or '―' }}</td>
  </tr>
  {% endfor %}
</table>
</body>
</html>
"""

@app.route("/")
def index():
    cn  = get_sql_server_connection()
    cur = cn.cursor()
    cur.execute("""
        SELECT account, sender_id, item_id, item_title,
               received_at, body_text, category, auto_reply_type,
               reply_draft_en, reply_draft_ja, reply_sent
        FROM trx.ebay_messages
        ORDER BY received_at DESC
    """)
    cols = [d[0] for d in cur.description]
    messages = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    cn.close()
    return render_template_string(HTML, messages=messages)


if __name__ == "__main__":
    app.run(debug=True, port=5050)