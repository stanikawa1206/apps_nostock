# -*- coding: utf-8 -*-
# new_publish_ebay.py — listings / vendor_item 対応（Shops/通常 両対応・簡潔版）

import concurrent.futures, sys, os, re, time, math, json, random
from decimal import Decimal, ROUND_HALF_UP
from typing import Tuple, Dict, Any, List, Optional
from datetime import datetime
from pathlib import Path
import pyodbc
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import utils
from utils import get_sql_server_connection, compute_start_price_usd, send_mail, translate_to_english
from publish_ebay_adapter import post_one_item, ApiHandledError, ListingLimitError
from ebay_common import fetch_active_presets, make_search_url
from scrape_utils import scroll_until_stagnant_collect_items, scroll_until_stagnant_collect_shops

IMG_LIMIT = 10
TEST_MODE = False

ACCOUNTS_PLAN = [
    {"account": "谷川②", "post_target": 10},
    {"account": "谷川③", "post_target": 10},
]

SHIPPING_JPY = 3000
CATEGORY_ID = "45258"
DEPARTMENT = "Women"
DEFAULT_BRAND_EN = "Louis Vuitton"

# ===== Driver =====
def build_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-notifications")
    opts.add_argument("--lang=ja-JP,ja")
    opts.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/119 Safari/537.36")
    opts.page_load_strategy = "eager"

    driver = webdriver.Chrome(service=Service(), options=opts)
    driver.set_window_size(1400, 1000)
    driver.set_page_load_timeout(30)
    driver.set_script_timeout(30)
    return driver

# ===== Utility =====
def _close_any_modal(driver):
    try:
        js = """
          return Array.from(document.querySelectorAll('button,[role=button]')).find(b=>{
            const t=(b.innerText||'').trim();
            return ['同意','閉じる','OK','Accept','Close'].some(k=>t.includes(k));
          });
        """
        btn = driver.execute_script(js)
        if btn:
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.2)
    except Exception:
        pass

def _try_extract_title(driver, vis_timeout=8.0):
    sels = [
        (By.CSS_SELECTOR, '#item-info h1'),
        (By.CSS_SELECTOR, '[data-testid="item-name"]'),
        (By.CSS_SELECTOR, 'h1[role="heading"]'),
        (By.CSS_SELECTOR, 'h1')
    ]
    for by, sel in sels:
        try:
            el = WebDriverWait(driver, vis_timeout).until(EC.visibility_of_element_located((by, sel)))
            t = (el.text or "").strip()
            if t:
                return t
        except Exception:
            continue
    try:
        og = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:title"]')
        return (og.get_attribute("content") or "").strip()
    except Exception:
        return ""

def _find_seller_info(driver):
    sels = [
        "a[href^='/user/profile/']",
        "[data-testid='seller-info'] a[href*='/user/']",
        "a[href*='/user/']",
    ]
    for sel in sels:
        try:
            el = WebDriverWait(driver, 3).until(EC.visibility_of_element_located((By.CSS_SELECTOR, sel)))
            href = (el.get_attribute("href") or "").strip()
            if not href:
                continue
            seller_id = href.rstrip("/").split("/")[-1]
            seller_name = (el.text or "").strip().splitlines()[0]
            near = el.text or ""
            m = re.search(r"\d{1,6}", near)
            rating_count = int(m.group()) if m else 0
            return seller_id, seller_name, rating_count
        except Exception:
            continue
    return "", "", 0

# ===== Shops専用 =====
def _extract_shops_seller(driver):
    a = WebDriverWait(driver, 6).until(
        EC.visibility_of_element_located((By.CSS_SELECTOR, 'a[data-testid="shops-profile-link"]'))
    )
    href = (a.get_attribute("href") or "").strip()
    seller_id = href.rstrip("/").split("/")[-1]
    block = (a.text or "").strip()
    m = re.search(r"(\d[\d,]*)", block)
    rating = int(m.group(1).replace(",", "")) if m else 0
    return seller_id, block, rating

def collect_images_shops(driver, limit=10):
    urls = []
    for el in driver.find_elements(By.CSS_SELECTOR, '.slick-slide img[src]'):
        src = (el.get_attribute("src") or "").strip()
        if src and src not in urls:
            urls.append(src)
        if len(urls) >= limit:
            break
    return urls + [None] * (limit - len(urls))

# ===== 詳細解析 =====
def parse_detail_shops(driver, url, preset, vendor_name):
    driver.get(url)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    _close_any_modal(driver)

    title, price, last_updated_str = "", 0, ""
    try:
        title = driver.find_element(By.CSS_SELECTOR, '[data-testid="product-title-section"] h1').text.strip()
        box = driver.find_element(By.CSS_SELECTOR, '[data-testid="product-price"]').text
        price = int(re.sub(r"[^\d]", "", box))
        last_updated_str = driver.find_element(By.CSS_SELECTOR, '#product-info > section:nth-child(2) > p').text.strip()
    except Exception:
        pass
    try:
        seller_id, seller_name, rating_count = _extract_shops_seller(driver)
    except Exception:
        seller_id, seller_name, rating_count = "", "", 0
    images = collect_images_shops(driver, limit=IMG_LIMIT)

    return dict(vendor_name=vendor_name, item_id=url.split("/")[-1], title_jp=title, title_en="",
                price=price, last_updated_str=last_updated_str, seller_id=seller_id,
                seller_name=seller_name, rating_count=rating_count, images=images, preset=preset)

def parse_detail_personal(driver, url, preset, vendor_name):
    driver.get(url)
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    _close_any_modal(driver)

    title, price, last_updated_str = _try_extract_title(driver), 0, ""
    try:
        element = driver.find_element(By.CSS_SELECTOR, '[data-testid*="price"]')
        price = int(re.sub(r"[^\d]", "", element.text))
        sec = driver.find_element(By.CSS_SELECTOR, '#item-info section')
        last_updated_str = (sec.text or "").splitlines()[-1].strip()
    except Exception:
        pass
    seller_id, seller_name, rating_count = _find_seller_info(driver)
    images = []
    for img in driver.find_elements(By.CSS_SELECTOR, "article img[src], article source[srcset]"):
        src = img.get_attribute("src") or ""
        if src and src not in images:
            images.append(src)
        if len(images) >= IMG_LIMIT:
            break
    images += [None] * (IMG_LIMIT - len(images))

    return dict(vendor_name=vendor_name, item_id=url.split("/")[-1], title_jp=title, title_en="",
                price=price, last_updated_str=last_updated_str, seller_id=seller_id,
                seller_name=seller_name, rating_count=rating_count, images=images, preset=preset)
