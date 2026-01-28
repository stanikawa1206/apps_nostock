# -*- coding: utf-8 -*-
from __future__ import annotations
import os, hmac, hashlib, datetime as dt
from typing import Dict, Any, Optional
from urllib.parse import urlencode, urlsplit
import requests

# ← 追加: apps/common/.env を優先的にロード
try:
    from dotenv import load_dotenv
    _COMMON_DIR = os.path.dirname(__file__)                         # .../apps/common
    _ENV_CANDIDATES = [
        os.path.join(_COMMON_DIR, ".env"),                          # apps/common/.env（あなたの配置）
        os.path.join(os.path.dirname(_COMMON_DIR), ".env"),         # apps/.env
        os.path.join(os.path.dirname(os.path.dirname(_COMMON_DIR)), ".env"),  # プロジェクト直下 ./.env
    ]
    for _p in _ENV_CANDIDATES:
        if os.path.exists(_p):
            load_dotenv(_p, override=False)
    # 最後に、既に設定済みの環境変数だけ再確認（上書きしない）
    load_dotenv(override=False)
except Exception:
    pass

class SpapiSession:
    def __init__(self,
                 lwa_client_id: Optional[str]=None,
                 lwa_client_secret: Optional[str]=None,
                 refresh_token: Optional[str]=None,
                 aws_access_key_id: Optional[str]=None,
                 aws_secret_access_key: Optional[str]=None,
                 aws_region: Optional[str]=None,
                 marketplace_id: Optional[str]=None,
                 endpoint: Optional[str]=None):
        self.LWA_CLIENT_ID     = lwa_client_id     or os.getenv("LWA_CLIENT_ID")
        self.LWA_CLIENT_SECRET = lwa_client_secret or os.getenv("LWA_CLIENT_SECRET")
        self.REFRESH_TOKEN     = refresh_token     or os.getenv("REFRESH_TOKEN")
        self.AWS_ACCESS_KEY_ID = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        self.AWS_SECRET_ACCESS_KEY = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.AWS_REGION        = aws_region        or os.getenv("AWS_REGION", "ap-northeast-1")
        self.MARKETPLACE_ID    = marketplace_id    or os.getenv("MARKETPLACE_ID", "A1VC38T7YXB528")
        self.ENDPOINT          = endpoint          or os.getenv("SPAPI_ENDPOINT", "https://sellingpartnerapi-fe.amazon.com")

        for k,v in [
            ("LWA_CLIENT_ID",self.LWA_CLIENT_ID),("LWA_CLIENT_SECRET",self.LWA_CLIENT_SECRET),
            ("REFRESH_TOKEN",self.REFRESH_TOKEN),("AWS_ACCESS_KEY_ID",self.AWS_ACCESS_KEY_ID),
            ("AWS_SECRET_ACCESS_KEY",self.AWS_SECRET_ACCESS_KEY)
        ]:
            if not v: raise SystemExit(f"[FATAL] env '{k}' is required.")

        self._access_token: Optional[str] = None

    # -------- LWA token (lazy) --------
    def _get_access_token(self) -> str:
        if self._access_token: return self._access_token
        r = requests.post("https://api.amazon.com/auth/o2/token", data={
            "grant_type":"refresh_token",
            "refresh_token": self.REFRESH_TOKEN,
            "client_id": self.LWA_CLIENT_ID,
            "client_secret": self.LWA_CLIENT_SECRET,
        }, timeout=20)
        if r.status_code != 200:
            raise SystemExit(f"[LWA ERROR] {r.status_code} {r.text}")
        self._access_token = r.json()["access_token"]
        return self._access_token

    # -------- SigV4 helpers --------
    @staticmethod
    def _hmac(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    def _sign_headers(self, method: str, url: str, query: str, payload_hash: str) -> Dict[str,str]:
        service="execute-api"
        parsed = urlsplit(url)
        host, path = parsed.netloc, (parsed.path or "/")
        now = dt.datetime.now(dt.timezone.utc)
        amzdate = now.strftime("%Y%m%dT%H%M%SZ")
        ds = amzdate[:8]

        kDate    = self._hmac(("AWS4"+self.AWS_SECRET_ACCESS_KEY).encode(), ds)
        kRegion  = self._hmac(kDate, self.AWS_REGION)
        kService = self._hmac(kRegion, service)
        kSign    = self._hmac(kService, "aws4_request")

        if method == "GET":
            canonical_headers = f"host:{host}\n" + f"x-amz-date:{amzdate}\n"
            signed_headers    = "host;x-amz-date"
        else:
            canonical_headers = f"content-type:application/json\nhost:{host}\nx-amz-date:{amzdate}\n"
            signed_headers    = "content-type;host;x-amz-date"

        canonical_request = "\n".join([method, path, query, canonical_headers, signed_headers, payload_hash])
        scope = f"{ds}/{self.AWS_REGION}/{service}/aws4_request"
        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256", amzdate, scope,
            hashlib.sha256(canonical_request.encode()).hexdigest()
        ])
        signature = hmac.new(kSign, string_to_sign.encode(), hashlib.sha256).hexdigest()
        return {
            "host": host,
            "x-amz-date": amzdate,
            "Authorization": f"AWS4-HMAC-SHA256 Credential={self.AWS_ACCESS_KEY_ID}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
            "signed_headers": signed_headers,
            "path": path,
        }

    # -------- Public HTTP wrappers --------
    def get(self, path: str, params: Dict[str,Any]) -> requests.Response:
        url = self.ENDPOINT + path
        q = urlencode(sorted(params.items()), doseq=True)
        hdr = self._sign_headers("GET", url, q, hashlib.sha256(b"").hexdigest())
        headers = {
            "host": hdr["host"],
            "x-amz-date": hdr["x-amz-date"],
            "Authorization": hdr["Authorization"],
            "x-amz-access-token": self._get_access_token(),
            "content-type": "application/json",
        }
        return requests.get(url, params=params, headers=headers, timeout=30)

    def post(self, path: str, body_json: str) -> requests.Response:
        url = self.ENDPOINT + path
        q = ""
        payload_hash = hashlib.sha256(body_json.encode("utf-8")).hexdigest()
        hdr = self._sign_headers("POST", url, q, payload_hash)
        headers = {
            "content-type": "application/json",
            "host": hdr["host"],
            "x-amz-date": hdr["x-amz-date"],
            "x-amz-access-token": self._get_access_token(),
            "Authorization": hdr["Authorization"],
        }
        return requests.post(url, data=body_json, headers=headers, timeout=30)
