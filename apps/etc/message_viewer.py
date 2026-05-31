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
import requests
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape as xml_escape
from openai import OpenAI
from datetime import datetime
from flask import Flask, render_template_string, jsonify, request
from apps.common.utils import get_sql_server_connection
from apps.adapters.ebay_api import get_access_token_new

EBAY_TRADING_URL = "https://api.ebay.com/ws/api.dll"

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
  .reply-instruction {
    width: 100%; border: 1px solid #ddd; border-radius: 8px;
    padding: 8px 10px; font-size: 13px; resize: vertical; min-height: 200px;
    font-family: inherit; line-height: 1.5; flex: 1;
  }
  .reply-instruction:focus { outline: none; border-color: #3665f3; }
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
  price_negotiation: {
    reply_en: "Thank you for your interest. We only offer small discounts, and large reductions are unlikely. If you are seriously considering purchasing, please let me know your best offer.",
    reply_ja: "ご興味をお持ちいただきありがとうございます。小幅な値引きのみ対応しており、大幅な値下げは難しい状況です。ご購入をご検討でしたら、ご希望の金額をお知らせください。"
  },
  price_negotiation_large: {
    reply_en: "Thank you for your offer. I appreciate your interest, but this discount is too large to consider. I'm open to reasonable offers, but this one is far below the item's value. Thank you for your understanding.",
    reply_ja: "ご提案ありがとうございます。ご興味をお持ちいただき嬉しく思いますが、この値引き幅は大きすぎてお受けすることができません。合理的なご提案であれば検討いたしますが、今回のご提案は商品の価値を大きく下回っております。ご理解のほどよろしくお願いいたします。"
  },
  rude_offer: {
    reply_en: "Thank you for your message. Unfortunately, the offer you proposed is significantly below our acceptable range. Therefore, we are completely unable to accommodate any discount requests for this transaction.",
    reply_ja: "メッセージありがとうございます。ご提案いただいた金額は当店の許容範囲を大きく下回っております。そのため、今回のお取引では値引きのご要望には一切お答えできかねます。"
  }
};

let threads = [];
let activeThread = null;
let activeThreadSenderId = null;
let activeThreadItemId = null;
let currentReplyMessageId = null;
let currentFilter  = 'unreplied';
let currentAccount = '';
let selectedModel  = 'gpt-4o-mini';

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

function renderActionBar() {
  const bar = document.getElementById('action-bar');
  if (!bar || !currentThread) return;
  if (currentThread.skip_reply) {
    bar.innerHTML = `
      <button class="btn-action-skip" disabled>✓ 対応不要にしました</button>
      <button class="btn-action-compose" onclick="openReplyPanel()">✏️ 返信作成</button>`;
  } else {
    bar.innerHTML = `
      <button class="btn-action-skip" onclick="skipReply()">対応不要</button>
      <button class="btn-action-compose" onclick="openReplyPanel()">✏️ 返信作成</button>`;
  }
}

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

    return `<div class="thread-item${isActive ? ' active' : ''}" data-status="${t.thread_status}"
              onclick="openThread('${t.sender_id}','${t.listing_id}', this)">
      ${thumb}
      <div class="thread-info">
        <div class="thread-sender">${t.sender_id || '不明'} <span style="font-size:12px;color:#aaa;font-weight:400;">${t.account || ''}</span></div>
        <div class="thread-item-title">${t.item_title || t.listing_id || ''}</div>
        <div class="thread-preview">${(t.last_message || '').slice(0,50)}</div>
      </div>
      <div class="thread-meta">
        <span class="thread-time">${time}</span>
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
    <div class="reply-panel" id="reply-panel">
      <div class="reply-panel-body">
        <div class="reply-left">
          <div class="model-toggle">
            <button class="btn-model active" id="btn-model-mini" onclick="selectModel('gpt-4o-mini')">GPT-4o-mini</button>
            <button class="btn-model" id="btn-model-full" onclick="selectModel('gpt-4o')">GPT-4o</button>
          </div>
          <label>指示（任意）</label>
          <textarea class="reply-instruction" id="reply-instruction" placeholder="例：もっと丁寧に、値引き不可を強調して、短くして"></textarea>
          <button class="btn-regenerate" id="btn-regenerate" onclick="generateReply()">再生成</button>
        </div>
        <div class="reply-right">
          <label>英語返信案</label>
          <textarea class="reply-en-area" id="reply-en"></textarea>
          <div class="reply-ja-area" id="reply-ja"></div>
        </div>
      </div>
      <div class="reply-panel-footer">
        <button class="btn-skip-reply" onclick="skipReply()">対応不要</button>
        <button class="btn-send-final" id="btn-send-final" onclick="sendReply()" disabled>送信 GO →</button>
      </div>
    </div>` : '';

  const ebayUrl   = itemId ? `https://www.ebay.com/itm/${itemId}` : null;
  const vendorUrl = data.vendor_url || null;
  const ebayBtn   = ebayUrl   ? `<a href="${ebayUrl}"   target="_blank" class="header-btn btn-ebay">eBay</a>` : '';
  const vendorBtn = vendorUrl ? `<a href="${vendorUrl}" target="_blank" class="header-btn btn-vendor">仕入先</a>` : '';

  const headerThumb = data.image_url1
    ? `<img class="header-thumb" src="${data.image_url1}" loading="lazy" onerror="this.style.display='none'">`
    : '';

  area.innerHTML = `
    <div class="chat-header">
      <div class="chat-header-sender">${senderId}${ebayBtn}${vendorBtn}</div>
      <div class="chat-header-item">${headerThumb}${thread ? (thread.item_title || thread.listing_id || '') : ''}</div>
    </div>
    <div class="chat-messages" id="chat-messages">${messagesHtml}</div>
    ${currentReplyMessageId ? '<div class="action-bar" id="action-bar"></div>' : ''}
    ${replyPanelHtml}
  `;

  const msgs = area.querySelector('#chat-messages');
  if (msgs) msgs.scrollTop = msgs.scrollHeight;

  // 状態に基づいてアクションバーとバッジを描画
  renderActionBar();
  renderHeaderBadge();

  // 未返信のbuyerメッセージを全件チェックして優先カテゴリを決定
  const allMsgs = data.messages || [];
  const lastSellerIdx = allMsgs.reduce((idx, m, i) => m.direction === 'seller' ? i : idx, -1);
  const unrepliedBuyers = allMsgs.filter((m, i) => m.direction !== 'seller' && i > lastSellerIdx);
  const CATEGORY_PRIORITY = ['rude_offer', 'price_negotiation_large', 'price_negotiation'];
  let templateKey = null;
  for (const key of CATEGORY_PRIORITY) {
    if (unrepliedBuyers.some(m => m.category === key)) { templateKey = key; break; }
  }
  if (templateKey && REPLY_TEMPLATES[templateKey]) {
    const tpl = REPLY_TEMPLATES[templateKey];
    document.getElementById('action-bar').style.display = 'none';
    document.getElementById('reply-panel').classList.add('open');
    document.getElementById('reply-en').value = tpl.reply_en;
    document.getElementById('reply-ja').textContent = '🇯🇵 ' + tpl.reply_ja;
    document.getElementById('btn-send-final').disabled = false;
    const panel = document.getElementById('reply-panel');
    if (panel && !panel.querySelector('.template-notice')) {
      const notice = document.createElement('div');
      notice.className = 'template-notice';
      notice.textContent = '💬 定型文が自動セットされました';
      panel.insertBefore(notice, panel.firstChild);
    }
  }
}

async function openReplyPanel() {
  if (!currentThread) return;

  // 対応不要状態だった場合はリセット
  if (currentThread.skip_reply) {
    const unskipRes  = await fetch('/api/unskip/' + currentThread.last_buyer_message_id, { method: 'POST' });
    const unskipData = await unskipRes.json();
    if (!unskipData.ok) {
      console.error('unskip error:', unskipData.error || '対応不要のリセットに失敗しました');
      return;
    }
    currentThread.skip_reply = 0;
    renderActionBar();
    renderHeaderBadge();
    // 返信パネルフッターの「対応不要」ボタンも初期状態に戻す
    document.querySelectorAll('.btn-skip-reply').forEach(btn => {
      btn.textContent = '対応不要';
      btn.disabled = false;
      btn.style.opacity = '';
      btn.style.cursor = '';
    });
    loadThreads();  // 左パネルを非同期で更新
  }

  document.getElementById('action-bar').style.display = 'none';
  document.getElementById('reply-panel').classList.add('open');
  generateReply();
}

async function generateReply() {
  if (!currentReplyMessageId) return;
  const instruction  = document.getElementById('reply-instruction').value;
  const currentReply = document.getElementById('reply-en').value.trim();
  const regen = document.getElementById('btn-regenerate');
  const send  = document.getElementById('btn-send-final');
  const enEl  = document.getElementById('reply-en');
  const jaEl  = document.getElementById('reply-ja');

  regen.disabled = true;
  regen.textContent = '生成中...';
  send.disabled = true;
  enEl.placeholder = currentReply ? '✨ 再生成中...' : '✨ 返信案を生成中...';
  jaEl.textContent = '🔄 日本語訳を生成中...';

  const controller = new AbortController();
  const timeoutId  = setTimeout(() => controller.abort(), 60000);

  try {
    const res  = await fetch('/api/generate/' + currentReplyMessageId, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({instruction: instruction, current_reply: currentReply, model: selectedModel}),
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
      send.disabled = false;
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
  if (!text.trim()) return;

  const btn = document.getElementById('btn-send-final');
  btn.disabled = true;
  btn.textContent = '送信中...';

  const jaEl = document.getElementById('reply-ja');
  const replyJa = jaEl ? jaEl.textContent.replace(/^🇯🇵\\s*/, '') : '';

  const res = await fetch('/api/send/' + currentReplyMessageId, {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({reply_text: text, reply_ja: replyJa})
  });
  const data = await res.json();

  if (data.ok) {
    currentReplyMessageId = null;
    await loadThreads();
    moveToNextUnreplied();
  } else {
    console.error('send error:', data.error || '不明');
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
    const res  = await fetch('/api/fetch', { method: 'POST' });
    const data = await res.json();
    if (data.ok) {
      await loadThreads();
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
setInterval(loadThreads, 60000);
</script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(HTML)


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
                SELECT account, listing_id, vendor_item_id
                FROM (
                    SELECT account, listing_id, vendor_item_id,
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
            WHERE m.rn = 1
            ORDER BY m.received_at DESC
        """)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
        for r in rows:
            if r.get('received_at'):
                r['received_at'] = r['received_at'].isoformat() + 'Z'
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
    cn = cur = None
    try:
        cn  = get_sql_server_connection()
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
        for m in msgs:
            if m.get('received_at'):
                m['received_at'] = m['received_at'].isoformat() + 'Z'

        # 仕入先情報（account はメッセージから取得）
        cur.execute("""
            SELECT TOP 1 l.vendor_item_id, vi.vendor_name, vi.image_url1
            FROM trx.ebay_messages em
            JOIN trx.listings l
              ON l.account    = em.account
             AND l.listing_id = em.listing_id
            LEFT JOIN trx.vendor_item vi
              ON vi.vendor_item_id = l.vendor_item_id
            WHERE em.sender_id = ? AND em.listing_id = ?
            ORDER BY l.is_deleted ASC, l.deleted_at DESC
        """, sender_id, listing_id)
        row = cur.fetchone()

        vendor_item_id = row[0] if row else None
        vendor_name    = row[1] if row else None
        image_url1     = row[2] if row else None

        if vendor_name == 'メルカリshops' and vendor_item_id:
            vendor_url = 'https://mercari-shops.com/products/' + vendor_item_id
        elif vendor_item_id:
            vendor_url = 'https://jp.mercari.com/item/' + vendor_item_id
        else:
            vendor_url = None

        return jsonify({
            "messages":       msgs,
            "vendor_url":     vendor_url,
            "vendor_name":    vendor_name,
            "vendor_item_id": vendor_item_id,
            "image_url1":     image_url1,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if cur: cur.close()
        if cn: cn.close()


@app.route("/api/generate/<message_id>", methods=["POST"])
def api_generate(message_id):
    try:
        data          = request.get_json() or {}
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

        print(f"[generate] message_id={message_id}")
        print(f"[generate] model={model}")
        print(f"[generate] instruction={repr(instruction)}")
        print(f"[generate] current_reply={repr(current_reply[:80]) if current_reply else '(empty)'}")

        current_draft_section = f"Current draft:\n{current_reply}" if current_reply else "Current draft:\n(none)"
        instruction_section   = instruction if instruction else "(none)"

        user_prompt = f"""You are a Japanese eBay seller.

{context_block}Buyer message: {latest_buyer_body}

{current_draft_section}

MANDATORY INSTRUCTIONS - YOU MUST FOLLOW ALL OF THESE:
{instruction_section}

Rules:
- Follow every instruction above without exception
- reply_en must be 100% English only, no Japanese
- reply_ja must be the Japanese translation of reply_en (NEVER leave it empty)
- Do not add sign-off or placeholder names
- Reply JSON only: {{"reply_en": "...", "reply_ja": "日本語訳..."}}"""

        print(f"[generate] prompt=\n{user_prompt}\n---")

        client   = OpenAI()
        response = client.chat.completions.create(
            model=model,
            max_tokens=1500,
            response_format={"type": "json_object"},
            timeout=60,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = (response.choices[0].message.content or "") if response.choices else ""

        print(f"[generate] raw={repr(raw[:200])}")

        try:
            result = json.loads(raw)
            if not isinstance(result, dict):
                raise ValueError("not a dict")
        except Exception:
            result = {"reply_en": raw, "reply_ja": "（解析エラー）"}

        result.setdefault("reply_en", "")
        result.setdefault("reply_ja", "")
        return jsonify(result)

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
    fetch_script = Path(__file__).resolve().parent / "fetch_messages_ebay.py"
    try:
        result = subprocess.run(
            [sys.executable, str(fetch_script), "--once"],
            capture_output=True, text=True, timeout=300
        )
        if result.returncode == 0:
            return jsonify({"ok": True, "log": result.stdout[-2000:]})
        else:
            return jsonify({"ok": False, "error": result.stderr[-500:] or result.stdout[-500:]})
    except subprocess.TimeoutExpired:
        return jsonify({"ok": False, "error": "タイムアウト（5分）"}), 500
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5050, host="0.0.0.0", use_reloader=False)