"""
出品に使う商品画像の選別と、縦横とも 500px に足りない画像の拡大（メルカリ・メルカリShops 共通）。

- 縦横とも IMAGE_MIN_PX 以上の画像は、そのまま（URLも画像も変えない）。
- 足りない画像は、短い辺が IMAGE_MIN_PX になるよう縦横比を保って拡大する（切り抜き・余白なし）。
  必要な倍率が IMAGE_MAX_UPSCALE を超える画像は、これまでどおり使わない。
- 拡大した画像は R2 に置き、その URL を eBay に渡す（publish_ebay.build_pic_urls）。
  eBay は渡した URL から画像を取り込むので、元の小さい画像の URL を渡すと拡大した意味がない。

拡大の上限（2026-11-16 実測）:
  画像不足で出品できなかった商品の写真は 429×600・430×600・450×600・640×480・600×450 などが大半で、
  商品ごとに一番少ない倍率で済む写真の倍率は、今日の Shops 246件で 1.04〜1.21倍（3件だけ 1.48倍）、
  過去のメルカリ・Shops の見本 84件でも大半が 1.25倍以下だった。
  1.25倍（短い辺 400px 以上）までで 今日の Shops 243/246件、過去の見本 75/84件が1枚以上使える。
  1.5倍にしても増えるのは数件で、拡大による画質の低下に見合わない。
"""
from __future__ import annotations

from io import BytesIO
from typing import Dict, List, Tuple

import requests
from PIL import Image

IMAGE_MIN_PX = 500
IMAGE_MAX_UPSCALE = 1.25
UPSCALED_JPEG_QUALITY = 95


def upscaled_size(width: int, height: int) -> Tuple[int, int]:
    """短い辺を IMAGE_MIN_PX にそろえた大きさ（縦横比を保つ）。例: 429×600 → 500×699。"""
    if width <= height:
        return IMAGE_MIN_PX, round(height * IMAGE_MIN_PX / width)
    return round(width * IMAGE_MIN_PX / height), IMAGE_MIN_PX


def needed_scale(width: int, height: int) -> float:
    """縦横とも IMAGE_MIN_PX 以上にするのに必要な倍率（足りていれば 1 以下）。"""
    return IMAGE_MIN_PX / min(width, height)


def select_listing_images(raw_urls) -> Tuple[List[str], Dict[str, Tuple[int, int]]]:
    """
    写真を1枚ずつ取得して大きさを測り、出品に使う写真を元の順に選ぶ。
    返り値: (使う写真の URL, {拡大して使う写真の URL: (元の幅, 元の高さ)})
    """
    images: List[str] = []
    upscale: Dict[str, Tuple[int, int]] = {}
    for img_url in raw_urls:
        try:
            res_img = requests.get(img_url, timeout=10)
            res_img.raise_for_status()
            width, height = Image.open(BytesIO(res_img.content)).size
        except Exception as e:
            print(f"[IMAGE ERROR] {img_url} {e}")
            continue

        scale = needed_scale(width, height)
        if scale <= 1:
            images.append(img_url)
        elif scale <= IMAGE_MAX_UPSCALE:
            images.append(img_url)
            upscale[img_url] = (width, height)
            print(f"[IMAGE] 拡大して使う {width}x{height} → {'x'.join(map(str, upscaled_size(width, height)))} {img_url}")
    return images, upscale


def upscale_image(content: bytes) -> bytes:
    """画像を短い辺が IMAGE_MIN_PX になるよう拡大して JPEG で返す。倍率が上限を超える画像は例外。"""
    img = Image.open(BytesIO(content))
    width, height = img.size
    scale = needed_scale(width, height)
    if scale > IMAGE_MAX_UPSCALE:
        raise ValueError(f"拡大の上限({IMAGE_MAX_UPSCALE}倍)を超える画像です: {width}x{height}")
    out = img.convert("RGB").resize(upscaled_size(width, height), Image.Resampling.LANCZOS)
    buf = BytesIO()
    out.save(buf, "JPEG", quality=UPSCALED_JPEG_QUALITY, subsampling=0)
    return buf.getvalue()


def upload_upscaled_image(r2, bucket: str, public_base: str, image_url: str, key: str) -> str:
    """
    image_url の画像を拡大して R2 の key に置き、公開 URL を返す。
    失敗したら例外にする（元の小さい画像の URL を返しても eBay では使えないため）。
    """
    res = requests.get(image_url, timeout=30)
    res.raise_for_status()
    body = upscale_image(res.content)
    clean_key = key.lstrip("/")
    r2.put_object(Bucket=bucket, Key=clean_key, Body=body, ContentType="image/jpeg")
    url = f"{public_base.rstrip('/')}/{clean_key}"
    print(f"[IMAGE] 拡大した画像を R2 に置きました {url} ({len(body)} bytes)")
    return url
