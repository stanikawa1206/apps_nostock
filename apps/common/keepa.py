# -*- coding: utf-8 -*-
from __future__ import annotations
import os, requests
from typing import Any, Dict, Optional

# ← 追加: apps/common/.env を読む
try:
    from dotenv import load_dotenv
    _COMMON_DIR = os.path.dirname(__file__)
    _ENV_CANDIDATES = [
        os.path.join(_COMMON_DIR, ".env"),
        os.path.join(os.path.dirname(_COMMON_DIR), ".env"),
        os.path.join(os.path.dirname(os.path.dirname(_COMMON_DIR)), ".env"),
    ]
    for _p in _ENV_CANDIDATES:
        if os.path.exists(_p):
            load_dotenv(_p, override=False)
    load_dotenv(override=False)
except Exception:
    pass


class KeepaClient:
    def __init__(self, api_key: Optional[str]=None, domain: int = 5):
        self.api_key = api_key or os.getenv("KEEPA_API_KEY")
        if not self.api_key:
            raise SystemExit("[FATAL] env 'KEEPA_API_KEY' is required.")
        self.domain = domain  # JP=5

    def fetch_product(self, asin: str) -> Dict[str,Any]:
        url = (
            "https://api.keepa.com/product"
            f"?key={self.api_key}&domain={self.domain}&asin={asin}&stats=1&buybox=1&history=1"
        )
        r = requests.get(url, timeout=30)
        j = r.json()
        prods = j.get("products") or []
        return prods[0] if prods else {}
