from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import json
import traceback
import subprocess
import threading
import requests
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as xml_escape
from openai import OpenAI
from datetime import datetime
from flask import Flask, render_template_string, jsonify, request
from apps.common.utils import get_sql_server_connection
from apps.adapters.ebay_api import get_access_token_new
# 値引き交渉の分類・金額計算ロジックはfetch_messages_ebay.pyと共通化し、
# ここ(AI返信生成)でも同じ判定結果(category/価格/値引き率)を利用する
from apps.etc.fetch_messages_ebay import (
    analyze_price_negotiation,
    _get_listing_price,
    CATEGORY_GUIDE,
    compute_negotiation_display,
)

EBAY_TRADING_URL = "https://api.ebay.com/ws/api.dll"

# 自動更新の間隔（秒）。ここを変えるだけでフロント側の自動更新周期も変わる。
AUTO_REFRESH_SECONDS = 600

# /api/fetch の多重実行防止用ロック（自動更新と手動更新、複数タブからの同時実行をまとめて防ぐ）
_fetch_lock = threading.Lock()

app = Flask(__name__)

HTML = """<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>eBay Messages</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    font-family: 'Segoe UI', -apple-system, sans-serif;
    background: #f3f3f3;
    height: 100vh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }

  /* ヘッダー */
  .header {
    background: #3665f3;
    color: white;
    padding: 12px 20px;
    display: flex;
    align-items: center;
    gap: 12px;
    flex-shrink: 0;
  }
  .header h1 { font-size: 16px; font-weight: 600; }
  .btn-fetch {
    background: rgba(255,255,255,0.2); color: white; border: 1px solid rgba(255,255,255,0.4);
    padding: 5px 12px; border-radius: 14px; font-size: 12px; cursor: pointer;
    margin-left: auto;
  }
  .btn-fetch:hover { background: rgba(255,255,255,0.35); }
  .btn-fetch:disabled { opacity: 0.6; cursor: not-allowed; }
  .account-select {
    background: rgba(255,255,255,0.15); color: white;
    border: 1px solid rgba(255,255,255,0.4);
    padding: 4px 8px; border-radius: 14px; font-size: 12px;
    cursor: pointer; outline: none;
  }
  .account-select option { color: #333; background: white; }
  .header .badge {
    background: rgba(255,255,255,0.25);
    border-radius: 12px;
    padding: 2px 10px;
    font-size: 12px;
  }

  /* メインレイアウト */
  .main {
    display: flex;
    flex: 1;
    overflow: hidden;
  }

  /* 左：スレッド一覧 */
  .left-col {
    width: 320px;
    flex-shrink: 0;
    display: flex;
    flex-direction: column;
    border-right: 1px solid #e0e0e0;
  }
  .filter-bar {
    display: flex;
    gap: 6px;
    padding: 10px 12px;
    background: white;
    border-bottom: 1px solid #f0f0f0;
    flex-shrink: 0;
  }
  .filter-btn {
    flex: 1;
    padding: 5px 0;
    border-radius: 16px;
    font-size: 12px;
    cursor: pointer;
    border: 1px solid #ddd;
    background: white;
    color: #555;
  }
  .filter-btn.active { background: #3665f3; color: white; border-color: #3665f3; font-weight: 600; }
  .thread-list {
    flex: 1;
    background: white;
    overflow-y: auto;
  }

  .thread-thumb {
    width: 40px; height: 40px; border-radius: 4px;
    object-fit: cover; flex-shrink: 0;
  }
  .thread-thumb-initials {
    width: 40px; height: 40px; border-radius: 4px; flex-shrink: 0;
    background: #e8f0fe; color: #3665f3;
    display: flex; align-items: center; justify-content: center;
    font-weight: 600; font-size: 13px;
  }
  .header-thumb {
    width: 48px; height: 48px; border-radius: 4px;
    object-fit: cover; flex-shrink: 0;
  }

  .thread-item {
    padding: 14px 16px;
    border-bottom: 1px solid #f0f0f0;
    cursor: pointer;
    display: flex;
    gap: 12px;
    align-items: flex-start;
    transition: background 0.1s;
  }
  .thread-item:hover { background: #f5f9ff; }
  .thread-item.active { background: #e8f0fe; border-left: 3px solid #3665f3; }

  /* 注文あり（購入後メッセージ）：一覧で一目で分かるよう背景と左バーで強調する。
     .active より後ろに、かつクラス2つ以上の指定で書くことで選択中でも色を維持する */
  .thread-item.has-order         { background: #fce4ec; border-left: 4px solid #d81b60; }
  .thread-item.has-order:hover   { background: #f8bbd0; }
  .thread-item.has-order.active  { background: #f8bbd0; border-left: 4px solid #ad1457; }
  .thread-item.has-order .thread-sender { color: #ad1457; }

  .thread-info { flex: 1; min-width: 0; }
  .thread-sender {
    font-size: 14px; font-weight: 600; color: #191919;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .thread-item-title {
    font-size: 12px; color: #767676;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    margin: 2px 0;
  }
  .thread-preview {
    font-size: 12px; color: #444;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  }
  .thread-meta {
    display: flex; flex-direction: column; align-items: flex-end; gap: 4px; flex-shrink: 0;
  }
  .thread-time { font-size: 11px; color: #767676; }
  .badge-unreplied { background: #fff3e0; color: #e65100;  font-size: 10px; padding: 2px 6px; border-radius: 10px; }
  .badge-replied   { background: #f0f0f0; color: #767676;  font-size: 10px; padding: 2px 6px; border-radius: 10px; }
  .badge-skip      { background: #e8eaf6; color: #5c6bc0;  font-size: 10px; padding: 2px 6px; border-radius: 10px; }
  .badge-order     {
    background: #d81b60; color: #fff; font-size: 10px; font-weight: 700;
    padding: 2px 6px; border-radius: 10px; white-space: nowrap;
  }

  /* 右：チャット */
  .chat-area {
    flex: 1;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    background: #fafafa;
  }

  .chat-header {
    background: white;
    padding: 14px 20px;
    border-bottom: 1px solid #e0e0e0;
    flex-shrink: 0;
  }
  .chat-header-sender { font-size: 16px; font-weight: 600; color: #191919; display: flex; align-items: center; gap: 8px; }
  .chat-header-item { font-size: 13px; color: #767676; margin-top: 6px; display: flex; align-items: center; gap: 8px; }
  .badge-skip-header {
    background: #e8eaf6; color: #5c6bc0;
    font-size: 11px; padding: 2px 8px; border-radius: 10px; font-weight: 400;
  }
  .badge-order-header {
    background: #d81b60; color: #fff;
    font-size: 12px; padding: 3px 10px; border-radius: 10px; font-weight: 700;
  }
  /* 購入後メッセージであることを見落とさないためのバナー（チャットヘッダー直下） */
  .order-banner {
    background: #fce4ec; border-bottom: 2px solid #d81b60;
    color: #ad1457; font-size: 13px; font-weight: 700;
    padding: 8px 20px; flex-shrink: 0;
    display: flex; align-items: center; gap: 10px;
  }
  .order-banner .order-banner-sub { font-size: 12px; font-weight: 400; color: #880e4f; }
  .header-btn {
    padding: 4px 10px; border-radius: 4px; font-size: 11px;
    font-weight: 600; cursor: pointer; border: none;
    text-decoration: none; display: inline-block; line-height: 1.4;
  }
  .btn-ebay   { background: #e53238; color: white; }
  .btn-ebay:hover { background: #c42930; }
  .btn-vendor { background: #2e7d32; color: white; }
  .btn-vendor:hover { background: #1b5e20; }

  .chat-messages {
    flex: 1;
    overflow-y: auto;
    padding: 20px;
    display: flex;
    flex-direction: column;
    gap: 16px;
  }

  /* メッセージバブル */
  .msg-row {
    display: flex;
    flex-direction: column;
    max-width: 70%;
  }
  .msg-row.buyer { align-self: flex-start; align-items: flex-start; }
  .msg-row.seller { align-self: flex-end; align-items: flex-end; }

  .msg-bubble {
    padding: 10px 14px;
    border-radius: 18px;
    font-size: 14px;
    line-height: 1.5;
    word-break: break-word;
  }
  .buyer .msg-bubble {
    background: white;
    border: 1px solid #e0e0e0;
    border-bottom-left-radius: 4px;
    color: #191919;
  }
  .seller .msg-bubble {
    background: #3665f3;
    color: white;
    border-bottom-right-radius: 4px;
  }

  .msg-time {
    font-size: 11px; color: #9e9e9e; margin-top: 4px; padding: 0 4px;
  }
  .msg-translation {
    font-size: 12px; color: #9e9e9e;
    margin-top: 4px; padding: 0 4px;
    font-style: italic;
  }

  /* アクションバー（対応不要 / 返信作成） */
  .action-bar {
    background: white;
    border-top: 1px solid #e0e0e0;
    padding: 10px 20px;
    display: flex;
    justify-content: space-between;
    flex-shrink: 0;
  }
  .btn-action-skip {
    background: white; color: #5c6bc0; border: 1px solid #c5cae9;
    padding: 7px 18px; border-radius: 20px; font-size: 13px; cursor: pointer;
  }
  .btn-action-skip:hover { background: #e8eaf6; }
  .btn-action-compose {
    background: #3665f3; color: white; border: none;
    padding: 7px 18px; border-radius: 20px; font-size: 13px;
    cursor: pointer; font-weight: 600;
  }
  .btn-action-compose:hover { background: #2b55d9; }

  /* 返信パネル */
  .reply-panel {
    background: white;
    border-top: 1px solid #e0e0e0;
    padding: 14px 20px 16px;
    flex-shrink: 0;
    display: none;
  }
  .reply-panel.open { display: block; }
  .reply-panel-body { display: flex; gap: 16px; margin-bottom: 12px; }
  .template-notice {
    background: #e8f5e9; color: #2e7d32; border: 1px solid #a5d6a7;
    border-radius: 6px; padding: 7px 12px; font-size: 12px; font-weight: 600;
    margin-bottom: 10px;
  }
  .reply-left {
    flex: 0 0 40%;
    display: flex; flex-direction: column; gap: 8px;
  }
  .reply-right { flex: 1; display: flex; flex-direction: column; gap: 8px; }
  .reply-panel label { font-size: 12px; color: #767676; }
  .reply-japanese, .reply-instruction {
    width: 100%; border: 1px solid #ddd; border-radius: 8px;
    padding: 8px 10px; font-size: 13px; resize: vertical;
    font-family: inherit; line-height: 1.5;
  }
  .reply-japanese:focus, .reply-instruction:focus { outline: none; border-color: #3665f3; }
  .reply-japanese { min-height: 140px; flex: 2; }
  .reply-instruction { min-height: 80px; flex: 1; }
  .btn-regenerate {
    background: #f0f4ff; color: #3665f3; border: 1px solid #c5d3f8;
    padding: 7px 12px; border-radius: 8px; font-size: 13px;
    cursor: pointer; font-weight: 600; margin-top: auto;
  }
  .btn-regenerate:hover { background: #e0eaff; }
  .btn-regenerate:disabled { opacity: 0.5; cursor: not-allowed; }
  .reply-en-area {
    width: 100%; border: 1px solid #ddd; border-radius: 8px;
    padding: 10px 12px; font-size: 14px; resize: vertical; min-height: 225px;
    font-family: inherit; line-height: 1.5;
  }
  .reply-en-area:focus { outline: none; border-color: #3665f3; }
  .reply-ja-area {
    font-size: 12px; color: #767676; background: #f8f8f8;
    padding: 8px 12px; border-radius: 6px; line-height: 1.5; min-height: 90px;
  }
  .reply-panel-footer { display: flex; justify-content: space-between; gap: 8px; }
  .btn-skip-reply {
    background: white; color: #5c6bc0; border: 1px solid #c5cae9;
    padding: 8px 16px; border-radius: 20px; font-size: 14px; cursor: pointer;
  }
  .btn-skip-reply:hover { background: #e8eaf6; }
  .btn-send-final {
    background: #3665f3; color: white; border: none;
    padding: 8px 24px; border-radius: 20px; font-size: 14px;
    cursor: pointer; font-weight: 600;
  }
  .btn-send-final:hover { background: #2b55d9; }
  .btn-send-final:disabled { opacity: 0.5; cursor: not-allowed; }

  /* モデル切り替えボタン */
  .model-toggle {
    display: flex;
    gap: 4px;
    margin-bottom: 4px;
  }
  .btn-model {
    padding: 3px 10px;
    border-radius: 12px;
    font-size: 11px;
    cursor: pointer;
    border: 1px solid #c5cae9;
    background: white;
    color: #5c6bc0;
  }
  .btn-model.active {
    background: #3665f3;
    color: white;
    border-color: #3665f3;
    font-weight: 600;
  }

  /* 空状態 */
  .empty-state {
    flex: 1; display: flex; align-items: center; justify-content: center;
    flex-direction: column; gap: 12px; color: #9e9e9e;
  }
  .empty-state svg { width: 48px; height: 48px; opacity: 0.3; }
</style>
</head>
<body>

<div class="header">
  <h1>📨 eBay Messages</h1>
  <span class="badge" id="total-badge">読み込み中...</span>
  <select class="account-select" id="account-select" onchange="setAccount(this.value)">
    <option value="">全アカウント</option>
  </select>
  <button class="btn-fetch" id="btn-fetch" onclick="runFetch()">🔄 更新</button>
</div>

<div class="main">
  <div class="left-col">
    <div class="filter-bar">
      <button class="filter-btn active" id="filter-unreplied" onclick="setFilter('unreplied')">未返信</button>
      <button class="filter-btn" id="filter-skip"     onclick="setFilter('skip')">対応不要</button>
      <button class="filter-btn" id="filter-replied"  onclick="setFilter('replied')">返信済</button>
      <button class="filter-btn" id="filter-all"      onclick="setFilter('all')">全て</button>
    </div>
    <div class="thread-list" id="thread-list">
      <div style="padding:20px;color:#9e9e9e;font-size:14px;">読み込み中...</div>
    </div>
  </div><!-- /.left-col -->

  <div class="chat-area" id="chat-area">
    <div class="empty-state">
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
        <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
      </svg>
      <p>スレッドを選択してください</p>
    </div>
  </div>
</div>

<script>
const REPLY_TEMPLATES = {
  // 具体的なBuyer希望価格がある場合(0%以上20%未満)。$[BUYER_OFFER]相当の金額は
  // 呼び出し側でfmtMoney(negotiation_offered_price)を渡して埋め込む。
  price_negotiation: {
    reply_en: (price) => `${price} would require my boss's approval.\\n\\nIf it gets approved, can you promise to complete the purchase?\\n\\nAlso, please note that we aim to ship within 5 business days after payment. I would appreciate it if you could confirm that this shipping timeframe is acceptable as well.\\n\\nIf you can confirm both, I'll do my best to negotiate with my boss for you.`,
    reply_ja: (price) => `${price}については上司の承認が必要となります。承認が下りた場合、必ずご購入いただけますでしょうか。また当店ではお支払い後5営業日以内の発送を目指しておりますので、この発送までの期間についてもご了承いただけるかあわせてご確認をお願いいたします。両方をご確認いただけましたら、上司との交渉に最善を尽くします。`
  },
  // 具体的な希望価格がないvague("best price?"等)の場合。price_negotiationと同じcategoryだが
  // 金額を埋め込めないため、希望額を尋ねる専用文を使う。
  price_negotiation_vague: {
    reply_en: "Thank you for your interest.\\n\\nPlease let me know the price you have in mind, and I will see what I can do.",
    reply_ja: "ご興味をお持ちいただきありがとうございます。ご希望の金額をお知らせいただけましたら、可能な範囲で検討いたします。"
  },
  price_negotiation_large: {
    reply_en: "Thank you for your offer.\\n\\nWe generally only consider discounts of around 10% from the listed price, so unfortunately, your offer is lower than what we can accept.\\n\\nIf you are still interested, please feel free to make an offer closer to that range.",
    reply_ja: "ご提案ありがとうございます。当店では基本的に、出品価格から約10%程度の値引きのみを検討しております。誠に恐れ入りますが、いただいたご提案はお受けできる範囲を下回っております。もしまだご興味をお持ちでしたら、その範囲に近いご提案を改めてお願いいたします。"
  },
  rude_offer: {
    reply_en: "Thank you for your message.\\n\\nWe do not continue negotiations with buyers who initially offer 50% or less of the listed price.\\n\\nThank you for your understanding.",
    reply_ja: "メッセージありがとうございます。当店では、最初のご提案が出品価格の50%以下となるお客様とは値引き交渉を継続しておりません。ご理解のほどよろしくお願いいたします。"
  },
  // 真贋確認のみのメッセージ("Is this authentic?"「本物ですか？」等)用の定型文。
  // 現時点では自動送信の対象外(定型文セットのみ)。
  authenticity_check: {
    reply_en: "Yes, this item is authentic and genuine.\\n\\nThank you for your question.",
    reply_ja: "はい、こちらの商品は本物・正規品です。ご質問ありがとうございます。"
  }
};

// 自動更新の間隔（秒）。サーバー側の AUTO_REFRESH_SECONDS 定数から注入される。
const AUTO_REFRESH_SECONDS = {{ auto_refresh_seconds }};

let threads = [];
let activeThread = null;
let activeThreadSenderId = null;
let activeThreadItemId = null;
let currentReplyMessageId = null;
let currentFilter  = 'unreplied';
let currentAccount = '';
let selectedModel  = 'gpt-4o-mini';

// /api/fetch の実行中Promiseを共有し、手動・自動どちらから来ても多重実行させない
let fetchInFlightPromise = null;
// 自動更新タイマーの再入防止（fetchが60秒を超えて実行中の場合に次のtickを無視する）
let autoTimerBusy = false;

function selectModel(model) {
  selectedModel = model;
  const miniBtn = document.getElementById('btn-model-mini');
  const fullBtn = document.getElementById('btn-model-full');
  if (!miniBtn || !fullBtn) return;
  miniBtn.classList.toggle('active', model === 'gpt-4o-mini');
  fullBtn.classList.toggle('active', model === 'gpt-4o');
}

// スレッド状態の単一ソース
let currentThread = null;
// { sender_id, listing_id, last_buyer_message_id, skip_reply: 0|1 }


function renderHeaderBadge() {
  const senderEl = document.querySelector('.chat-header-sender');
  if (!senderEl || !currentThread) return;
  const existing = senderEl.querySelector('.badge-skip-header');
  if (currentThread.skip_reply && !existing) {
    const badge = document.createElement('span');
    badge.className = 'badge-skip-header';
    badge.textContent = '対応不要';
    senderEl.appendChild(badge);
  } else if (!currentThread.skip_reply && existing) {
    existing.remove();
  }
}

function setFilter(f) {
  currentFilter = f;
  ['unreplied','skip','replied','all'].forEach(k => {
    document.getElementById('filter-' + k).classList.toggle('active', k === f);
  });
  renderThreads();
}

function setAccount(val) {
  currentAccount = val;
  renderThreads();
}

function filteredThreads() {
  if (!Array.isArray(threads)) return [];
  let result = threads;
  if (currentAccount) result = result.filter(t => t.account === currentAccount);
  if (currentFilter === 'unreplied') return result.filter(t => t.thread_status === '未返信');
  if (currentFilter === 'skip')      return result.filter(t => t.thread_status === '対応不要');
  if (currentFilter === 'replied')   return result.filter(t => t.thread_status === '返信済');
  return result;
}

function renderThreads() {
  const visible = filteredThreads();
  document.getElementById('total-badge').textContent = visible.length + '件';

  const list = document.getElementById('thread-list');
  if (visible.length === 0) {
    list.innerHTML = '<div style="padding:20px;color:#9e9e9e;font-size:13px;">該当なし</div>';
    return;
  }

  list.innerHTML = visible.map(t => {
    const badgeMap = { '未返信': 'badge-unreplied', '返信済': 'badge-replied', '対応不要': 'badge-skip' };
    const badge = `<span class="${badgeMap[t.thread_status] || 'badge-unreplied'}">${t.thread_status}</span>`;
    const time = t.received_at ? new Date(t.received_at).toLocaleString('ja-JP', {timeZone:'Asia/Tokyo',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'}) : '';
    const isActive = t.sender_id === activeThreadSenderId && t.listing_id === activeThreadItemId;

    const thumb = t.image_url1
      ? `<img class="thread-thumb" src="${t.image_url1}" loading="lazy"
             onerror="this.style.display='none';this.nextElementSibling.style.display='flex'">`
        + `<div class="thread-thumb-initials" style="display:none">${(t.sender_id||'?').slice(0,2).toUpperCase()}</div>`
      : `<div class="thread-thumb-initials">${(t.sender_id||'?').slice(0,2).toUpperCase()}</div>`;

    // 注文あり（購入後メッセージ）：行全体の色分け＋バッジで購入前と明確に区別する
    const hasOrder   = !!t.has_order;
    const orderBadge = hasOrder ? '<span class="badge-order">🛒 注文あり</span>' : '';

    return `<div class="thread-item${hasOrder ? ' has-order' : ''}${isActive ? ' active' : ''}"
              data-status="${t.thread_status}" data-order="${hasOrder ? 1 : 0}"
              onclick="openThread('${t.sender_id}','${t.listing_id}', this)">
      ${thumb}
      <div class="thread-info">
        <div class="thread-sender">${t.sender_id || '不明'} <span style="font-size:12px;color:#aaa;font-weight:400;">${t.account || ''}</span></div>
        <div class="thread-item-title">${t.item_title || t.listing_id || ''}</div>
        <div class="thread-preview">${(t.last_message || '').slice(0,50)}</div>
      </div>
      <div class="thread-meta">
        <span class="thread-time">${time}</span>
        ${orderBadge}
        ${badge}
      </div>
    </div>`;
  }).join('');
}

async function loadThreads() {
  const res = await fetch('/api/threads');
  const data = await res.json();
  if (data.error) { console.error('threads error:', data.error); return; }
  threads = data;
  renderThreads();
}

function moveToNextUnreplied() {
  const items = document.querySelectorAll('.thread-item[data-status="未返信"]');
  if (items.length > 0) items[0].click();
}

function safeMsgText(str) {
  return (str || '')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/\\n/g, '<br>');
}

// 現在開いているスレッドの新着メッセージだけをチャット欄に追記する。
// 返信パネル（日本語/指示/英語返信案の入力中テキスト）や選択状態には一切触れない。
async function refreshActiveThreadMessages() {
  if (!activeThreadSenderId || !activeThreadItemId || !activeThread) return;

  let data;
  try {
    const res = await fetch('/api/thread/' + encodeURIComponent(activeThreadSenderId) + '/' + encodeURIComponent(activeThreadItemId));
    data = await res.json();
  } catch (e) {
    console.error('auto refresh (thread) error:', e);
    return;
  }
  if (!data || data.error) return;

  const known = new Set((activeThread.messages || []).map(m => m.message_id));
  const newMsgs = (data.messages || []).filter(m => !known.has(m.message_id));
  if (newMsgs.length === 0) return;

  const container = document.getElementById('chat-messages');
  if (!container) return;

  const wasNearBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 60;

  const html = newMsgs.map(m => {
    const side = m.direction === 'seller' ? 'seller' : 'buyer';
    const time = m.received_at ? new Date(m.received_at).toLocaleString('ja-JP', {timeZone:'Asia/Tokyo'}) : '';
    const translation = m.body_text_ja ? `<div class="msg-translation">🇯🇵 ${m.body_text_ja}</div>` : '';
    return `<div class="msg-row ${side}">
      <div class="msg-bubble">${safeMsgText(m.body_text)}</div>
      ${translation}
      <div class="msg-time">${time}</div>
    </div>`;
  }).join('');

  container.insertAdjacentHTML('beforeend', html);
  activeThread.messages = data.messages;
  if (wasNearBottom) container.scrollTop = container.scrollHeight;
}

// /api/fetch を呼び出す。既にリクエストが進行中ならそれをそのまま共有し、
// 手動更新・自動更新のどちらから呼ばれても多重にサブプロセスが走らないようにする。
function doFetch() {
  if (fetchInFlightPromise) return fetchInFlightPromise;
  fetchInFlightPromise = fetch('/api/fetch', { method: 'POST' })
    .then(res => res.json())
    .catch(e => ({ ok: false, error: String(e) }))
    .finally(() => { fetchInFlightPromise = null; });
  return fetchInFlightPromise;
}

// fetch成功後の画面反映。スレッド一覧は軽量に再取得し、開いているスレッドがあれば
// 新着メッセージだけを差分追記する（入力中の返信文・選択中スレッドはそのまま）。
async function afterFetchRefresh() {
  await loadThreads();
  await refreshActiveThreadMessages();
}

// 自動更新タイマー本体。前回の実行が終わっていなければ何もしない。
async function autoRefreshTick() {
  if (autoTimerBusy) return;
  autoTimerBusy = true;
  try {
    const data = await doFetch();
    if (data && data.ok) {
      await afterFetchRefresh();
    }
  } finally {
    autoTimerBusy = false;
  }
}

// --- 値引き交渉の「定型文がセットされました」通知テキスト生成 ---
// サーバー(api_thread)側で既に計算済みの negotiation_* フィールド（GPT呼び出しなし、
// 保存済みoffer_type/value/currencyからの再計算のみ）をそのまま使って文言を組み立てる。
function fmtMoney(v) {
  const n = Math.round(Number(v) * 100) / 100;
  return Number.isInteger(n) ? `$${n}` : `$${n.toFixed(2)}`;
}
function fmtPercent(rate) {
  return (rate * 100).toFixed(1) + '%';
}
function fmtPercentValue(v) {
  const n = Math.round(Number(v) * 10) / 10;
  return Number.isInteger(n) ? `${n}%` : `${n.toFixed(1)}%`;
}
function buildTemplateNoticeText(m) {
  const base = '💬 定型文がセットされました';
  if (!m) return base;

  if (m.negotiation_reason === 'vague') {
    return `${base}（値引き交渉・具体的な希望額なし）`;
  }
  if (m.negotiation_reason === 'currency_mismatch') {
    return `${base}（通貨が異なるため値引き率は未判定）`;
  }
  if (m.negotiation_offered_price == null || m.negotiation_discount_rate == null) {
    return `${base}（値引き率を算出できないため通常の値引き交渉として判定）`;
  }

  const price = fmtMoney(m.negotiation_offered_price);
  const rate  = m.negotiation_discount_rate;
  let severity = '';
  if (m.negotiation_category === 'price_negotiation_large') severity = '・大幅値引き';
  else if (m.negotiation_category === 'rude_offer')          severity = '・大幅な値引き要求';
  const rateSuffix = `${fmtPercent(rate)}値引き${severity}`;

  if (m.negotiation_offer_type === 'amount_off' && m.negotiation_value != null) {
    const off = fmtMoney(m.negotiation_value);
    return `${base}（${off}値引き → 希望価格 ${price} / ${rateSuffix}）`;
  }
  if (m.negotiation_offer_type === 'percent_off' && m.negotiation_value != null) {
    const pctReq = fmtPercentValue(m.negotiation_value);
    // percent_offはBuyer指定%と算出discount_rateが常に一致するため、無印(price_negotiation)の
    // 場合は重複表記を避け、大幅値引き等の付加情報がある場合のみ値引き率も併記する
    if (!severity) {
      return `${base}（${pctReq}値引き → 希望価格 ${price}）`;
    }
    return `${base}（${pctReq}値引き → 希望価格 ${price} / ${rateSuffix}）`;
  }
  // absolute_price、またはその他の場合
  return `${base}（希望価格 ${price} / ${rateSuffix}）`;
}

async function openThread(senderId, itemId, el) {
  document.querySelectorAll('.thread-item').forEach(e => e.classList.remove('active'));
  el.classList.add('active');

  activeThreadSenderId = senderId;
  activeThreadItemId   = itemId;
  currentReplyMessageId = null;

  const res = await fetch('/api/thread/' + encodeURIComponent(senderId) + '/' + encodeURIComponent(itemId));
  const data = await res.json();
  if (data.error) { console.error('thread error:', data.error); return; }

  const thread = threads.find(t => t.sender_id === senderId && t.listing_id === itemId);
  activeThread = data;

  const area = document.getElementById('chat-area');

  function safeText(str) {
    return (str || '')
      .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"').replace(/&#39;/g, "'")
      .replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/\\n/g, '<br>');
  }

  const messagesHtml = (data.messages || []).map(m => {
    const side = m.direction === 'seller' ? 'seller' : 'buyer';
    const text = m.body_text || '';
    const time = m.received_at ? new Date(m.received_at).toLocaleString('ja-JP', {timeZone:'Asia/Tokyo'}) : '';
    const translation = m.body_text_ja
      ? `<div class="msg-translation">🇯🇵 ${m.body_text_ja}</div>` : '';

    return `<div class="msg-row ${side}">
      <div class="msg-bubble">${safeText(text)}</div>
      ${translation}
      <div class="msg-time">${time}</div>
    </div>`;
  }).join('');

  // 最後のバイヤーメッセージを特定
  const lastBuyer = [...(data.messages || [])].reverse().find(m => m.direction !== 'seller');
  currentReplyMessageId = lastBuyer ? lastBuyer.message_id : null;

  // スレッド状態を currentThread に集約
  const skipReplyValue = thread && thread.thread_status === '対応不要' ? 1 : 0;
  currentThread = {
    sender_id:             senderId,
    listing_id:            itemId,
    last_buyer_message_id: currentReplyMessageId,
    skip_reply:            skipReplyValue,
  };

  const replyPanelHtml = currentReplyMessageId ? `
    <div class="reply-panel open" id="reply-panel">
      <div class="reply-panel-body">
        <div class="reply-left">
          <div class="model-toggle">
            <button class="btn-model active" id="btn-model-mini" onclick="selectModel('gpt-4o-mini')">GPT-4o-mini</button>
            <button class="btn-model" id="btn-model-full" onclick="selectModel('gpt-4o')">GPT-4o</button>
          </div>
          <label>日本語</label>
          <textarea class="reply-japanese" id="reply-japanese" placeholder="例：土日は出荷担当がお休みなので、商品の状態については月曜日に確認してお知らせします。&#10;箱や書類はありませんが、梱包はしっかり行います。"></textarea>
          <label>指示</label>
          <textarea class="reply-instruction" id="reply-instruction" placeholder="例：もっと丁寧に、値引き不可を強調して、短くして"></textarea>
          <button class="btn-regenerate" id="btn-regenerate" onclick="generateReply()">🤖 AI返信作成</button>
        </div>
        <div class="reply-right">
          <label>英語返信案</label>
          <textarea class="reply-en-area" id="reply-en"></textarea>
          <div class="reply-ja-area" id="reply-ja"></div>
        </div>
      </div>
      <div class="reply-panel-footer">
        <button class="btn-skip-reply" onclick="skipReply()">対応不要</button>
        <button class="btn-send-final" id="btn-send-final" onclick="sendReply()">送信 GO →</button>
      </div>
    </div>` : '';

  const ebayUrl   = itemId ? `https://www.ebay.com/itm/${itemId}` : null;
  const vendorUrl = data.vendor_url || null;
  const ebayBtn   = ebayUrl   ? `<a href="${ebayUrl}"   target="_blank" class="header-btn btn-ebay">eBay</a>` : '';
  const vendorBtn = vendorUrl ? `<a href="${vendorUrl}" target="_blank" class="header-btn btn-vendor">仕入先</a>` : '';

  const headerThumb = data.image_url1
    ? `<img class="header-thumb" src="${data.image_url1}" loading="lazy" onerror="this.style.display='none'">`
    : '';

  // 注文有無（/api/thread が trx.ebay_orders から取得した既存データ。
  // メッセージ画面からeBayの注文APIは呼ばない）
  const orderInfo = data.order_info || { has_order: false };
  const orderHeaderBadge = orderInfo.has_order
    ? '<span class="badge-order-header">🛒 注文あり</span>' : '';
  const orderDateText = orderInfo.last_order_date
    ? new Date(orderInfo.last_order_date).toLocaleString('ja-JP', {timeZone:'Asia/Tokyo',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'})
    : '';
  const orderIdsText = (orderInfo.order_ids && orderInfo.order_ids.length)
    ? ' / 注文ID ' + orderInfo.order_ids.join(', ') : '';
  const orderBannerHtml = orderInfo.has_order ? `
    <div class="order-banner">
      <span>🛒 注文あり — このバイヤーは本商品を購入済みです（購入後の問い合わせ）</span>
      <span class="order-banner-sub">${orderInfo.order_count || 1}件${orderDateText ? ' / 最新注文 ' + orderDateText : ''}${orderIdsText}</span>
    </div>` : '';

  area.innerHTML = `
    <div class="chat-header">
      <div class="chat-header-sender"><span onclick="window.open('https://www.ebay.com/usr/${senderId}', '_blank')" style="cursor:pointer; text-decoration:underline;">${senderId}</span>${orderHeaderBadge}${ebayBtn}${vendorBtn}</div>
      <div class="chat-header-item">${headerThumb}${thread ? (thread.item_title || thread.listing_id || '') : ''}</div>
    </div>
    ${orderBannerHtml}
    <div class="chat-messages" id="chat-messages">${messagesHtml}</div>
    ${replyPanelHtml}
  `;

  const msgs = area.querySelector('#chat-messages');
  if (msgs) msgs.scrollTop = msgs.scrollHeight;

  renderHeaderBadge();

  // 未返信のbuyerメッセージを全件チェックして優先カテゴリを決定
  const allMsgs = data.messages || [];
  const lastSellerIdx = allMsgs.reduce((idx, m, i) => m.direction === 'seller' ? i : idx, -1);
  const unrepliedBuyers = allMsgs.filter((m, i) => m.direction !== 'seller' && i > lastSellerIdx);
  const CATEGORY_PRIORITY = ['rude_offer', 'price_negotiation_large', 'price_negotiation', 'authenticity_check'];
  let templateKey = null;
  let templateMsg = null;
  for (const key of CATEGORY_PRIORITY) {
    const found = unrepliedBuyers.find(m => m.category === key);
    if (found) { templateKey = key; templateMsg = found; break; }
  }
  if (templateKey) {
    let replyEn = null;
    let replyJa = null;
    if (templateKey === 'price_negotiation' && templateMsg.negotiation_offered_price == null) {
      // 具体的な希望価格を算出できない(vague/通貨不一致等)場合は、金額を埋め込む定型文ではなく
      // 希望額を尋ねる専用定型文を使う
      replyEn = REPLY_TEMPLATES.price_negotiation_vague.reply_en;
      replyJa = REPLY_TEMPLATES.price_negotiation_vague.reply_ja;
    } else if (templateKey === 'price_negotiation') {
      const price = fmtMoney(templateMsg.negotiation_offered_price);
      replyEn = REPLY_TEMPLATES.price_negotiation.reply_en(price);
      replyJa = REPLY_TEMPLATES.price_negotiation.reply_ja(price);
    } else if (REPLY_TEMPLATES[templateKey]) {
      replyEn = REPLY_TEMPLATES[templateKey].reply_en;
      replyJa = REPLY_TEMPLATES[templateKey].reply_ja;
    }
    if (replyEn != null) {
      document.getElementById('reply-en').value = replyEn;
      document.getElementById('reply-ja').textContent = '🇯🇵 ' + replyJa;
      const panel = document.getElementById('reply-panel');
      if (panel && !panel.querySelector('.template-notice')) {
        const notice = document.createElement('div');
        notice.className = 'template-notice';
        notice.textContent = buildTemplateNoticeText(templateMsg);
        panel.insertBefore(notice, panel.firstChild);
      }
    }
  }
}


async function generateReply() {
  if (!currentReplyMessageId) return;
  const japaneseText = document.getElementById('reply-japanese').value;
  const instruction  = document.getElementById('reply-instruction').value;
  const currentReply = document.getElementById('reply-en').value.trim();
  const regen = document.getElementById('btn-regenerate');
  const enEl  = document.getElementById('reply-en');
  const jaEl  = document.getElementById('reply-ja');

  regen.disabled = true;
  regen.textContent = '生成中...';
  enEl.placeholder = currentReply ? '✨ 再生成中...' : '✨ 返信案を生成中...';
  jaEl.textContent = '🔄 日本語訳を生成中...';

  const controller = new AbortController();
  const timeoutId  = setTimeout(() => controller.abort(), 60000);

  try {
    const res  = await fetch('/api/generate/' + currentReplyMessageId, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({japanese_text: japaneseText, instruction: instruction, current_reply: currentReply, model: selectedModel}),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    const data = await res.json();

    if (data.error) {
      console.error('generate error:', data.error);
      enEl.placeholder = '生成エラー（コンソールを確認）';
      jaEl.textContent = '';
    } else {
      enEl.placeholder = '';
      enEl.value = data.reply_en || '';
      jaEl.textContent = data.reply_ja ? ('🇯🇵 ' + data.reply_ja) : '';
    }
  } catch (e) {
    clearTimeout(timeoutId);
    const msg = e.name === 'AbortError' ? 'タイムアウト（60秒）' : String(e);
    console.error('generate error:', msg);
    enEl.placeholder = `生成エラー: ${msg}`;
    jaEl.textContent = '';
  } finally {
    regen.disabled = false;
    regen.textContent = '再生成';
  }
}

async function sendReply() {
  if (!currentReplyMessageId) return;
  const text = document.getElementById('reply-en').value;
  if (!text.trim()) {
    alert('英語返信案が入力されていません。');
    return;
  }

  const btn = document.getElementById('btn-send-final');
  btn.disabled = true;
  btn.textContent = '送信中...';

  const jaEl = document.getElementById('reply-ja');
  const replyJa = jaEl ? jaEl.textContent.replace(/^🇯🇵\\s*/, '') : '';

  let data;
  try {
    const res = await fetch('/api/send/' + currentReplyMessageId, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({reply_text: text, reply_ja: replyJa})
    });
    data = await res.json();
  } catch (e) {
    console.error('send communication error:', e);
    alert('送信に失敗しました（通信エラー）: ' + e);
    btn.disabled = false;
    btn.textContent = '送信 GO →';
    return;
  }

  if (data.ok) {
    btn.textContent = '✅ 送信完了';
    currentReplyMessageId = null;
    await loadThreads();
    moveToNextUnreplied();
  } else {
    console.error('send error:', data.error || '不明');
    alert('送信に失敗しました: ' + (data.error || '不明なエラー'));
    btn.disabled = false;
    btn.textContent = '送信 GO →';
  }
}

async function skipReply() {
  if (!currentThread || !currentThread.last_buyer_message_id) return;
  const res  = await fetch('/api/skip/' + currentThread.last_buyer_message_id, { method: 'POST' });
  const data = await res.json();
  if (data.ok) {
    currentThread.skip_reply = 1;
    renderActionBar();
    renderHeaderBadge();
    // 返信パネルが開いていた場合も「対応不要」ボタンを更新
    document.querySelectorAll('.btn-skip-reply').forEach(btn => {
      btn.textContent = '✓ 対応不要にしました';
      btn.disabled = true;
    });
    await loadThreads();  // 左パネルのみ更新
  } else {
    console.error('skip error:', data.error || '不明');
  }
}

async function runFetch() {
  const btn = document.getElementById('btn-fetch');
  btn.disabled = true;
  btn.textContent = '🔄 取得中...';
  try {
    const data = await doFetch();  // 自動更新が進行中ならそれに相乗りする
    if (data.ok) {
      await afterFetchRefresh();
      btn.textContent = '✅ 完了';
      setTimeout(() => { btn.textContent = '🔄 更新'; btn.disabled = false; }, 2000);
    } else {
      console.error('fetch error:', data.error || '不明');
      btn.textContent = '🔄 更新';
      btn.disabled = false;
    }
  } catch (e) {
    console.error('fetch communication error:', e);
    btn.textContent = '🔄 更新';
    btn.disabled = false;
  }
}

async function loadAccounts() {
  try {
    const res  = await fetch('/api/accounts');
    const data = await res.json();
    if (!Array.isArray(data)) {
      console.error('accounts error:', data.error || data);
      return;
    }
    const sel = document.getElementById('account-select');
    data.forEach(acc => {
      const opt = document.createElement('option');
      opt.value = acc;
      opt.textContent = acc;
      sel.appendChild(opt);
    });
    // ブラウザのフォーム履歴による意図しない選択を防ぐ
    sel.value = '';
    currentAccount = '';
  } catch (e) {
    console.error('accounts fetch error:', e);
  }
}

loadAccounts();
loadThreads();
// ページを開いている間だけ、AUTO_REFRESH_SECONDS 間隔でeBayメッセージを自動取得する
setInterval(autoRefreshTick, AUTO_REFRESH_SECONDS * 1000);
</script>
</body>
</html>
"""

# --------------------------------------------------
# 注文有無の判定（購入前 / 購入後メッセージの区別）
#
# 注文情報の取得・保存は apps/inventory/fetch_orders_ebay.py に一本化してあり、
# メッセージ画面はそこが trx.ebay_orders に保存済みの既存データを参照するだけ。
# ここから eBay の注文APIは絶対に呼ばない。
#
# 紐付けキー:
#   trx.ebay_orders.ebay_id (= listing の ItemID) = trx.ebay_messages.listing_id
#   trx.ebay_orders.buyer   (= バイヤーのeBay ID) = trx.ebay_messages.sender_id
# 同一listingを複数のバイヤーが購入するケース（在庫1でも再出品/複数個出品で発生）が
# 実データ上も存在するため、listing_id だけでは判定せず必ず sender_id との
# 組み合わせで判定する。
#
# 将来 fetch_orders 側で注文status（未発送・発送済・返品など）を持つように
# なった場合は、_ORDER_INFO_KEYS に status 系のキーを足し、下の2つのクエリで
# その列を取得して order_info に詰めれば、画面・AIプロンプトの双方へ自動的に
# 伝播する（呼び出し側の構造は変更不要）。
# --------------------------------------------------

def _empty_order_info() -> dict:
    """注文が1件も無い場合の order_info。キー構成は _build_order_info と揃える。"""
    return {
        "has_order":       False,
        "order_count":     0,
        "order_ids":       [],
        "last_order_date": None,
        # 将来 fetch_orders 側でstatus管理を整備したらここに詰める（現時点は常にNone）
        "order_status":    None,
    }


def _build_order_info(order_count: int, last_order_date, order_ids) -> dict:
    """trx.ebay_orders の集計結果を画面/AI共通の order_info 形式に整える。"""
    ids = []
    for oid in (order_ids or []):
        if oid and oid not in ids:
            ids.append(oid)
    return {
        "has_order":       bool(order_count),
        "order_count":     int(order_count or 0),
        "order_ids":       ids,
        "last_order_date": last_order_date.isoformat() + "Z" if last_order_date else None,
        "order_status":    None,
    }


def _fetch_order_info_map(cur) -> dict:
    """
    全スレッド分の注文有無を1クエリでまとめて取得する（/api/threads用）。
    戻り値: {(listing_id, sender_id_lower): order_info}
    """
    cur.execute("""
        SELECT ebay_id, buyer, COUNT(*) AS order_count, MAX(order_date) AS last_order_date
        FROM trx.ebay_orders
        WHERE ebay_id IS NOT NULL AND buyer IS NOT NULL
        GROUP BY ebay_id, buyer
    """)
    result = {}
    for ebay_id, buyer, order_count, last_order_date in cur.fetchall():
        key = (str(ebay_id), str(buyer).lower())
        result[key] = _build_order_info(order_count, last_order_date, [])
    return result


def _fetch_order_info(cur, sender_id: str, listing_id: str) -> dict:
    """
    1スレッド（この相手・この商品）についての注文有無を取得する。
    /api/thread と /api/generate から共通で使う。
    """
    if not sender_id or not listing_id:
        return _empty_order_info()
    cur.execute("""
        SELECT order_id, order_date
        FROM trx.ebay_orders
        WHERE ebay_id = ? AND buyer = ?
        ORDER BY order_date DESC
    """, listing_id, sender_id)
    rows = cur.fetchall()
    if not rows:
        return _empty_order_info()
    return _build_order_info(
        order_count=len(rows),
        last_order_date=rows[0][1],
        order_ids=[r[0] for r in rows],
    )


def _build_order_prompt_section(order_info: dict) -> str:
    """
    AI返信生成プロンプトへ渡す「注文あり / 注文なし」コンテキストを組み立てる。
    注文statusを扱えるようになったら、ここに status の説明行を足すだけでよい。
    """
    info = order_info or _empty_order_info()
    if not info.get("has_order"):
        return (
            "Order context: This buyer has NOT purchased this item. "
            "This is a PRE-PURCHASE inquiry from a potential buyer.\n\n"
        )
    lines = [
        "Order context: This buyer has ALREADY PURCHASED this item. "
        "This is a POST-PURCHASE inquiry from an existing customer.",
    ]
    if info.get("order_count"):
        lines.append("- Orders for this buyer on this listing: %d" % info["order_count"])
    if info.get("last_order_date"):
        lines.append("- Most recent order date (UTC): %s" % info["last_order_date"])
    if info.get("order_status"):
        lines.append("- Order status: %s" % info["order_status"])
    lines.append(
        "- Handle it as an inquiry about an existing order (shipping, tracking, delivery, "
        "the item they already bought, returns, etc.). Do NOT ask them to purchase the item "
        "and do NOT treat it as a price negotiation for a future purchase."
    )
    return "\n".join(lines) + "\n\n"


@app.route("/")
def index():
    return render_template_string(HTML, auto_refresh_seconds=AUTO_REFRESH_SECONDS)


@app.route("/api/accounts")
def api_accounts():
    cn = cur = None
    try:
        cn  = get_sql_server_connection()
        cur = cn.cursor()
        cur.execute("SELECT account FROM mst.ebay_accounts WHERE is_excluded = 0 ORDER BY account")
        rows = [row[0] for row in cur.fetchall()]
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cur: cur.close()
        if cn: cn.close()


@app.route("/api/threads")
def api_threads():
    cn = cur = None
    try:
        cn  = get_sql_server_connection()
        cur = cn.cursor()
        cur.execute("""
            SELECT
                m.sender_id,
                m.listing_id,
                m.item_title,
                m.body_text      AS last_message,
                m.received_at,
                m.category,
                m.account,
                m.direction,
                m.skip_reply,
                vi.vendor_name,
                vi.vendor_item_id,
                vi.image_url1
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY sender_id, listing_id
                           ORDER BY received_at DESC
                       ) AS rn
                FROM trx.ebay_messages
            ) m
            LEFT JOIN (
                SELECT account, listing_id, vendor_item_id, vendor_name
                FROM (
                    SELECT account, listing_id, vendor_item_id, vendor_name,
                           ROW_NUMBER() OVER (
                               PARTITION BY account, listing_id
                               ORDER BY is_deleted ASC, deleted_at DESC
                           ) AS rn
                    FROM trx.listings
                ) x WHERE x.rn = 1
            ) l ON l.account    = m.account
               AND l.listing_id = m.listing_id
            LEFT JOIN trx.vendor_item vi
                   ON vi.vendor_item_id = l.vendor_item_id
                  AND vi.vendor_name    = l.vendor_name
            WHERE m.rn = 1
            ORDER BY m.received_at DESC
        """)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        # 注文有無（fetch_orders が保存済みの trx.ebay_orders を参照するだけ）。
        # listing_id + sender_id の組で引くので、同一listingを別バイヤーが
        # 購入していても他スレッドが「注文あり」に誤判定されることはない。
        order_map = _fetch_order_info_map(cur)

        for r in rows:
            if r.get('received_at'):
                r['received_at'] = r['received_at'].isoformat() + 'Z'

            oi = order_map.get(
                (str(r.get('listing_id') or ''), str(r.get('sender_id') or '').lower())
            ) or _empty_order_info()
            r['has_order']       = oi['has_order']
            r['order_count']     = oi['order_count']
            r['last_order_date'] = oi['last_order_date']
            r['order_status']    = oi['order_status']

            if r.get('direction') == 'seller':
                r['thread_status'] = '返信済'
            elif r.get('skip_reply'):
                r['thread_status'] = '対応不要'
            else:
                r['thread_status'] = '未返信'
            vendor_name = r.get('vendor_name') or ''
            vid         = r.get('vendor_item_id') or ''
            if vendor_name == 'メルカリshops' and vid:
                r['vendor_url'] = 'https://mercari-shops.com/products/' + vid
            elif vid:
                r['vendor_url'] = 'https://jp.mercari.com/item/' + vid
            else:
                r['vendor_url'] = None
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cur: cur.close()
        if cn: cn.close()


@app.route("/api/thread/<sender_id>/<listing_id>")
def api_thread(sender_id, listing_id):
    import time
    t0 = time.time()
    cn = cur = None
    try:
        cn  = get_sql_server_connection()
        t1 = time.time()
        print(f"[thread] DB接続: {t1-t0:.3f}s")
        cur = cn.cursor()

        # メッセージ一覧
        cur.execute("""
            SELECT message_id, sender_id, listing_id, item_title,
                   direction, received_at, body_text, body_text_ja,
                   category, auto_reply_type
            FROM trx.ebay_messages
            WHERE sender_id = ? AND listing_id = ?
            ORDER BY received_at ASC
        """, sender_id, listing_id)
        cols = [d[0] for d in cur.description]
        msgs = [dict(zip(cols, row)) for row in cur.fetchall()]
        t2 = time.time()
        print(f"[thread] メッセージ取得({len(msgs)}件): {t2-t1:.3f}s")

        # 値引き交渉メッセージの表示用情報(希望価格・値引き率)を付加する。
        # fetch時にauto_reply_typeへ保存済みのoffer_type/value/currencyを再利用し、
        # GPTを呼ばずにPython側の既存計算(_compute_offer等)だけで再計算する。
        NEGOTIATION_CATEGORIES = ("price_negotiation", "price_negotiation_large", "rude_offer")
        negotiation_start_price = None
        if any(m.get('category') in NEGOTIATION_CATEGORIES for m in msgs):
            negotiation_start_price = _get_listing_price(listing_id)

        for m in msgs:
            if m.get('received_at'):
                m['received_at'] = m['received_at'].isoformat() + 'Z'
            if m.get('direction') == 'buyer' and m.get('category') in NEGOTIATION_CATEGORIES:
                display = compute_negotiation_display(m.get('auto_reply_type'), negotiation_start_price)
                m['negotiation_offer_type']     = display['offer_type']
                m['negotiation_value']          = display['value']
                m['negotiation_offered_price']  = display['offered_price']
                m['negotiation_discount_rate']  = display['discount_rate']
                m['negotiation_category']       = display['category']
                m['negotiation_reason']         = display['reason']

        # 仕入先情報（account はメッセージから取得）
        cur.execute("""
            SELECT TOP 1 l.vendor_item_id, vi.vendor_name, vi.image_url1
            FROM trx.ebay_messages em
            JOIN trx.listings l
              ON l.account    = em.account
             AND l.listing_id = em.listing_id
            LEFT JOIN trx.vendor_item vi
              ON vi.vendor_item_id = l.vendor_item_id
             AND vi.vendor_name    = l.vendor_name
            WHERE em.sender_id = ? AND em.listing_id = ?
            ORDER BY l.is_deleted ASC, l.deleted_at DESC
        """, sender_id, listing_id)
        row = cur.fetchone()
        t3 = time.time()
        print(f"[thread] 仕入先情報取得: {t3-t2:.3f}s")

        vendor_item_id = row[0] if row else None
        vendor_name    = row[1] if row else None
        image_url1     = row[2] if row else None

        if vendor_name == 'メルカリshops' and vendor_item_id:
            vendor_url = 'https://mercari-shops.com/products/' + vendor_item_id
        elif vendor_item_id:
            vendor_url = 'https://jp.mercari.com/item/' + vendor_item_id
        else:
            vendor_url = None

        # 注文有無（この相手・この商品に対する注文が trx.ebay_orders に存在するか）
        order_info = _fetch_order_info(cur, sender_id, listing_id)

        result = jsonify({
            "messages":       msgs,
            "vendor_url":     vendor_url,
            "vendor_name":    vendor_name,
            "vendor_item_id": vendor_item_id,
            "image_url1":     image_url1,
            "order_info":     order_info,
        })
        print(f"[thread] 合計: {time.time()-t0:.3f}s")
        return result
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cur: cur.close()
        if cn: cn.close()


@app.route("/api/generate/<message_id>", methods=["POST"])
def api_generate(message_id):
    try:
        data          = request.get_json() or {}
        japanese_text = data.get("japanese_text", "").strip()
        instruction   = data.get("instruction",   "").strip()
        current_reply = data.get("current_reply", "").strip()
        model         = data.get("model", "gpt-4o-mini")
        if model not in ("gpt-4o-mini", "gpt-4o"):
            model = "gpt-4o"

        cn = cur = None
        try:
            cn  = get_sql_server_connection()
            cur = cn.cursor()
            cur.execute(
                "SELECT sender_id, listing_id, item_title FROM trx.ebay_messages WHERE message_id = ?",
                message_id
            )
            row = cur.fetchone()
            if not row:
                return jsonify({"error": "message not found"}), 404

            sender_id, listing_id, item_title = row

            cur.execute("""
                SELECT direction, body_text FROM trx.ebay_messages
                WHERE sender_id = ? AND listing_id = ?
                ORDER BY received_at ASC
            """, sender_id, listing_id)
            history = cur.fetchall()

            # 購入前 / 購入後を区別してAIに返信案を作らせるため、注文有無を取得する。
            # 取得元は fetch_orders が保存した trx.ebay_orders のみ（eBay APIは叩かない）。
            try:
                order_info = _fetch_order_info(cur, sender_id, listing_id)
            except Exception as e:
                print(f"[generate] order info skipped: {e}")
                order_info = _empty_order_info()
        finally:
            if cur: cur.close()
            if cn: cn.close()

        # 最新のバイヤーメッセージと会話履歴を抽出
        latest_buyer_body = ""
        conv_lines = []
        for direction, body in history:
            role = "Buyer" if direction == "buyer" else "Seller"
            conv_lines.append(f"{role}: {body or ''}")
            if direction == "buyer":
                latest_buyer_body = body or ""

        context_block = ""
        if len(history) > 1:
            context_block = "Conversation history:\n" + "\n".join(conv_lines) + "\n\n"

        # 注文あり/なしのコンテキスト（購入後の問い合わせを取り違えないため）
        order_section = _build_order_prompt_section(order_info)

        # 値引き交渉コンテキスト(現在価格・Buyer希望価格・値引き率・category)をAI返信生成へ渡す。
        # 判定はfetch_messages_ebay.pyの分類ロジックと共通の関数で行う(GPTには最終計算をさせない)。
        # ここで失敗しても通常の返信生成自体は止めない。
        negotiation_section = ""
        try:
            if latest_buyer_body:
                start_price = _get_listing_price(listing_id)
                analysis = analyze_price_negotiation(latest_buyer_body, listing_id=listing_id, start_price=start_price)
                if analysis["category"] in ("price_negotiation", "price_negotiation_large", "rude_offer"):
                    lines = []
                    if start_price:
                        lines.append(f"- Current listing price: ${start_price:.2f}")
                    if analysis["offered_price"] is not None:
                        lines.append(f"- Buyer requested price: ${analysis['offered_price']:.2f}")
                    if analysis["discount_rate"] is not None:
                        lines.append(f"- Discount rate: {analysis['discount_rate'] * 100:.1f}%")
                    else:
                        lines.append("- Buyer did not state a specific price, amount, or percentage")
                    guide = CATEGORY_GUIDE.get(analysis["category"], "")
                    lines.append(f"- Negotiation category: {analysis['category']} — {guide}")

                    if japanese_text:
                        # 日本語欄に具体的な内容がある場合、ここはあくまで状況理解のための
                        # 補助情報。CATEGORY_GUIDEの一般方針(購入意思確認・発送日数・上司承認など)を
                        # reply_enへ勝手に追加させないよう、明示的に禁止する。
                        negotiation_section = (
                            "Negotiation context (background information ONLY, to help you understand the "
                            "situation — this is NOT a list of things to say. The category guide text below "
                            "describes our general policy for this category, but do NOT use it to add any new "
                            "facts, conditions, questions, promises, or requests to reply_en that are not already "
                            "present in the Japanese source text or MANDATORY INSTRUCTIONS below):\n"
                            + "\n".join(lines) + "\n\n"
                        )
                    else:
                        negotiation_section = (
                            "Negotiation context (background information to help you understand the situation; "
                            "if it conflicts with the MANDATORY INSTRUCTIONS below, the MANDATORY INSTRUCTIONS win):\n"
                            + "\n".join(lines) + "\n\n"
                        )
        except Exception as e:
            print(f"[generate] negotiation analysis skipped: {e}")
            negotiation_section = ""

        print(f"[generate] message_id={message_id}")
        print(f"[generate] model={model}")
        print(f"[generate] japanese_text={repr(japanese_text[:80]) if japanese_text else '(empty)'}")
        print(f"[generate] instruction={repr(instruction)}")
        print(f"[generate] current_reply={repr(current_reply[:80]) if current_reply else '(empty)'}")
        print(f"[generate] order_section={repr(order_section)}")
        print(f"[generate] negotiation_section={repr(negotiation_section)}")

        current_draft_section  = f"Current draft:\n{current_reply}" if current_reply else "Current draft:\n(none)"
        instruction_section    = instruction if instruction else "(none)"
        japanese_source_section = (
            f"Japanese source text - this is the primary and complete source of WHAT to say in reply_en. "
            f"Translate/adapt it into natural, polite eBay-appropriate English (you may smooth out phrasing, "
            f"adjust politeness, and add minimal greetings), but do NOT add any new facts, conditions, questions, "
            f"promises, or requests that are not present in this text:\n{japanese_text}"
            if japanese_text else "Japanese source text:\n(none)"
        )

        user_prompt = f"""You are a Japanese eBay seller.

{context_block}Buyer message: {latest_buyer_body}

{order_section}{negotiation_section}{japanese_source_section}

{current_draft_section}

MANDATORY INSTRUCTIONS - describe how to adjust/style the reply. YOU MUST FOLLOW ALL OF THESE:
{instruction_section}

Priority order for deciding WHAT reply_en says (highest to lowest):
1. MANDATORY INSTRUCTIONS
2. Japanese source text
3. Conversation history / latest Buyer message
4. Negotiation context (category / discount rate / CATEGORY_GUIDE)
5. Order context (whether the buyer has already purchased this item)

Rules:
- Order context is background information for judging the situation (pre-purchase vs post-purchase), NOT content to include. Do not state order IDs, order counts, or order dates in reply_en unless the Japanese source text or MANDATORY INSTRUCTIONS ask for it.
- If Japanese source text is provided: reply_en's content must come ONLY from that text (plus anything MANDATORY INSTRUCTIONS explicitly adds). You may translate/adapt it into natural, polite eBay-appropriate English, fix awkward phrasing, and add minimal greetings — but do NOT add new facts, conditions, questions, promises, or negotiation terms (e.g. purchase-intent confirmation, shipping timeframe, boss's approval) that are not present in the Japanese source text, even if the Negotiation context or its category guide mentions them. Do not treat the Negotiation context as a checklist to cover.
- If Japanese source text is empty: use the Negotiation context (price/discount rate/category/CATEGORY_GUIDE) and the conversation history to write an appropriate reply from scratch, as before.
- MANDATORY INSTRUCTIONS always take priority over both the Japanese source text and the Negotiation context. Read each instruction carefully and judge whether it is a STYLE instruction (tone, politeness, length, wording — e.g. "be more polite") or a CONTENT instruction (asks for a new topic/question/condition to be added — e.g. "also confirm purchase intent"). For a STYLE instruction, only restyle the existing Japanese-source content; do NOT pull in new facts/conditions from the Negotiation context or its category guide just because it is "allowed" by priority order. Only add content from the Negotiation context when an instruction explicitly asks for it as new content.
- Follow every instruction above without exception
- reply_en must be 100% English only, no Japanese
- reply_ja must be the Japanese translation of reply_en (NEVER leave it empty)
- Do not add sign-off or placeholder names
- Reply JSON only: {{"reply_en": "...", "reply_ja": "日本語訳..."}}

Example of a STYLE-only instruction (do NOT add new content):
  Japanese source text: "１０％程度の値引きしか考えていません"
  MANDATORY INSTRUCTIONS: "もっと丁寧に" (be more polite)
  WRONG reply_en (adds a new condition not in the source): "Thank you for your offer. I can consider a discount of about 10%, but I would need to confirm if your offer can be accepted."
  CORRECT reply_en (same content, just more polite): "Thank you so much for your interest. I'm afraid we can only offer a discount of around 10% at this time."
This same principle applies to any other style-only instruction (e.g. "shorter", "more casual", "add a greeting")."""

        print(f"[generate] prompt=\n{user_prompt}\n---")

        print("①翻訳開始")
        client   = OpenAI()
        print("②API呼び出し前")
        response = client.chat.completions.create(
            model=model,
            max_tokens=1500,
            response_format={"type": "json_object"},
            timeout=60,
            messages=[{"role": "user", "content": user_prompt}],
        )
        print("③API応答受信")
        raw = (response.choices[0].message.content or "") if response.choices else ""

        print(f"[generate] raw={repr(raw[:200])}")

        try:
            result = json.loads(raw)
            if not isinstance(result, dict):
                raise ValueError("not a dict")
        except Exception:
            result = {"reply_en": raw, "reply_ja": "（解析エラー）"}
        print("④JSON解析完了")

        result.setdefault("reply_en", "")
        result.setdefault("reply_ja", "")
        print("⑤画面更新開始")
        response_data = jsonify(result)
        print("⑥画面更新完了")
        return response_data

    except Exception as e:
        print(f"[generate] ERROR: {e}")
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/api/skip/<message_id>", methods=["POST"])
def api_skip(message_id):
    cn = cur = None
    try:
        cn  = get_sql_server_connection()
        cur = cn.cursor()
        cur.execute("UPDATE trx.ebay_messages SET skip_reply = 1 WHERE message_id = ?", message_id)
        cn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cur: cur.close()
        if cn: cn.close()


@app.route("/api/unskip/<message_id>", methods=["POST"])
def api_unskip(message_id):
    cn = cur = None
    try:
        cn  = get_sql_server_connection()
        cur = cn.cursor()
        cur.execute("UPDATE trx.ebay_messages SET skip_reply = 0 WHERE message_id = ?", message_id)
        cn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cur: cur.close()
        if cn: cn.close()


def _send_ebay_reply(account: str, listing_id: str, sender_id: str,
                     parent_message_id: str, body: str) -> dict:
    token = get_access_token_new(account)
    if not token:
        return {"ok": False, "error": "token取得失敗"}

    xml_body = f"""<?xml version="1.0" encoding="utf-8"?>
<AddMemberMessageRTQRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <RequesterCredentials>
    <eBayAuthToken>{token}</eBayAuthToken>
  </RequesterCredentials>
  <ItemID>{listing_id}</ItemID>
  <MemberMessage>
    <Body>{xml_escape(body)}</Body>
    <RecipientID>{sender_id}</RecipientID>
    <ParentMessageID>{parent_message_id}</ParentMessageID>
  </MemberMessage>
</AddMemberMessageRTQRequest>""".encode("utf-8")

    headers = {
        "Content-Type":                   "text/xml",
        "X-EBAY-API-CALL-NAME":           "AddMemberMessageRTQ",
        "X-EBAY-API-SITEID":              "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
        "X-EBAY-API-IAF-TOKEN":           token,
    }

    try:
        r = requests.post(EBAY_TRADING_URL, data=xml_body, headers=headers, timeout=30)
        r.encoding = "utf-8"
    except Exception as e:
        return {"ok": False, "error": f"HTTP error: {e}"}

    try:
        ns   = {"e": "urn:ebay:apis:eBLBaseComponents"}
        root = ET.fromstring(r.content)
        ack  = root.findtext("e:Ack", namespaces=ns) or ""
    except Exception as e:
        return {"ok": False, "error": f"XML parse error: {e} / body: {r.text[:200]}"}

    if ack in ("Success", "Warning"):
        return {"ok": True}

    errors = [err.text for err in root.findall(".//e:Errors/e:LongMessage", ns) if err.text]
    return {"ok": False, "error": " / ".join(errors) or f"API error (Ack={ack})"}


@app.route("/api/send/<message_id>", methods=["POST"])
def api_send(message_id):
    data       = request.get_json() or {}
    reply_text = data.get("reply_text", "")
    reply_ja   = data.get("reply_ja", "") or None

    # メッセージ情報をDBから取得
    cn = cur = None
    try:
        cn  = get_sql_server_connection()
        cur = cn.cursor()
        cur.execute(
            "SELECT account, sender_id, listing_id, item_title FROM trx.ebay_messages WHERE message_id = ?",
            message_id
        )
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "message not found"}), 404
        account, sender_id, listing_id, item_title = row
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cur: cur.close()
        if cn: cn.close()

    # eBay Trading API で実際に送信
    result = _send_ebay_reply(account, listing_id, sender_id, message_id, reply_text)
    if not result["ok"]:
        return jsonify({"error": result["error"]}), 500

    # 送信成功後にDBへ保存
    cn = cur = None
    try:
        cn  = get_sql_server_connection()
        cur = cn.cursor()
        now    = datetime.utcnow()
        new_id = f"SELLER-{int(now.timestamp() * 1000)}"
        cur.execute("""
            INSERT INTO trx.ebay_messages (
                message_id, account, listing_id, item_title, sender_id,
                direction, received_at, body_text, body_text_ja,
                category, auto_reply_type
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, new_id, account, listing_id, item_title, sender_id,
             "seller", now, reply_text, reply_ja, None, None)
        cn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cur: cur.close()
        if cn: cn.close()


@app.route("/api/fetch", methods=["POST"])
def api_fetch():
    # 手動更新・自動更新・複数タブからの同時呼び出しでもサブプロセスが重複起動しないようにする
    if not _fetch_lock.acquire(blocking=False):
        return jsonify({"ok": True, "skipped": True, "log": "他の更新処理が実行中のためスキップしました"})

    try:
        fetch_script = Path(__file__).resolve().parent / "fetch_messages_ebay.py"
        try:
            result = subprocess.run(
                [sys.executable, str(fetch_script), "--once"],
                capture_output=True, text=True, encoding='utf-8', timeout=300
            )
            if result.returncode == 0:
                return jsonify({"ok": True, "log": result.stdout[-2000:]})
            else:
                return jsonify({"ok": False, "error": result.stderr[-500:] or result.stdout[-500:]})
        except subprocess.TimeoutExpired:
            return jsonify({"ok": False, "error": "タイムアウト（5分）"}), 500
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500
    finally:
        _fetch_lock.release()


if __name__ == "__main__":
    from waitress import serve
    print("Starting server on http://0.0.0.0:5050")
    serve(app, host="0.0.0.0", port=5050)