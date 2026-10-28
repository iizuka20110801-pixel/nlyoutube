"""
んょしtube - 高速YouTubeビューア (FastAPI)
- インスタンスをヘルススコア順に並べ、120ms間隔のヘッジングレースで最速応答を採用
- コメント/ストリームは複数APIを同時レースして最初の有効応答を返す
- TTLキャッシュ + インフライト重複排除 + 起動時ウォームアップ
"""
import asyncio
import base64
import json
import os
import random
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse

import httpx
from fastapi import FastAPI, Query, Request
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
    StreamingResponse,
)
from fastapi.templating import Jinja2Templates

APP_NAME = "んょしtube"

NOCOOKIE_BASE = "https://www.youtube-nocookie.com/embed/"
SIA_BASE = os.environ.get("SIA_API_BASE", "https://siatube.com")
SENNIN_BASE = os.environ.get(
    "SENNIN_API_BASE",
    "https://discerning-adventure-production-ebfc.up.railway.app",
)
RAPID_HOST = "ytstream-download-youtube-videos.p.rapidapi.com"

LIST_VIDEO = [
    "https://raw.githubusercontent.com/ikirikittsuhao-ctrl/Invidious-check/refs/heads/main/lists/video.json",
    "https://raw.githubusercontent.com/kuru-bana/yt-data/refs/heads/main/list/injidious.json",
]
LIST_SEARCH = [
    "https://raw.githubusercontent.com/ikirikittsuhao-ctrl/Invidious-check/refs/heads/main/lists/search.json",
]

FALLBACK_INSTANCES = [
    "https://invidious.f5.si",
    "https://invidious.ritoge.com",
    "https://yt.omada.cafe",
    "https://invidious.darkness.services",
    "https://invidious.ducks.party",
    "https://y.com.sb",
    "https://super8.absturztau.be",
    "https://inv.zoomerville.com",
    "https://invidious.nerdvpn.de",
    "https://inv.thepixora.com",
]

PIPED_INSTANCES = [
    "https://pipedapi.wireway.ch",
    "https://api.piped.private.coffee",
    "https://pipedapi.winscloud.net",
]

_RAPID_KEYS = [
    "ZTYxNTE4MzAzNG1zaDJkZmRhMzFhNDdhNmYxMnAxZmE2Y2Nqc241OWExYTVlMDY0MTU=",
    "NjllMjk5OWE3OW1zaGNiNjU3MTg0YmE2NzMxY3AxNmY2ODRqc24zMjA1NGEwNzBiYTU=",
]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)

_LIMITS = httpx.Limits(
    max_connections=800, max_keepalive_connections=300, keepalive_expiry=60.0
)
client = httpx.AsyncClient(
    timeout=httpx.Timeout(4.0, connect=2.0, read=3.5, write=3.5),
    limits=_LIMITS,
    follow_redirects=True,
    headers={"User-Agent": UA, "Accept-Language": "ja,en;q=0.8"},
)
noredirect = httpx.AsyncClient(
    timeout=httpx.Timeout(3.0, connect=1.5, read=2.5),
    limits=_LIMITS,
    follow_redirects=False,
    headers={"User-Agent": UA},
)

templates = Jinja2Templates(directory="templates")
templates.env.globals["app_name"] = APP_NAME

app = FastAPI(title=APP_NAME, docs_url=None, redoc_url=None)


# --------------------------------------------------------------------------
# キャッシュ / インフライト重複排除
# --------------------------------------------------------------------------
_CACHE: Dict[str, Dict[str, Any]] = {}
_INFLIGHT: Dict[str, asyncio.Future] = {}
_MAX_CACHE = 4000


def _cache_get(key: str) -> Optional[Any]:
    e = _CACHE.get(key)
    if not e:
        return None
    if e["exp"] < time.time():
        _CACHE.pop(key, None)
        return None
    return e["val"]


def _cache_set(key: str, val: Any, ttl: float) -> None:
    if len(_CACHE) > _MAX_CACHE:
        for k in sorted(_CACHE, key=lambda k: _CACHE[k]["exp"])[: _MAX_CACHE // 10]:
            _CACHE.pop(k, None)
    _CACHE[key] = {"val": val, "exp": time.time() + ttl}


async def cached(key: str, ttl: float, fn, timeout: float = 12.0) -> Optional[Any]:
    hit = _cache_get(key)
    if hit is not None:
        return hit
    fut = _INFLIGHT.get(key)
    if fut is not None:
        try:
            return await asyncio.shield(fut)
        except Exception:
            pass
    loop = asyncio.get_event_loop()
    fut = loop.create_future()
    _INFLIGHT[key] = fut
    try:
        res = await asyncio.wait_for(fn(), timeout=timeout)
        if res is not None:
            _cache_set(key, res, ttl)
        if not fut.done():
            fut.set_result(res)
        return res
    except Exception as exc:
        if not fut.done():
            fut.set_exception(exc)
            fut.exception()
        return _cache_get(key)
    finally:
        _INFLIGHT.pop(key, None)


# --------------------------------------------------------------------------
# インスタンス管理（ヘルススコア + ヘッジングレース）
# --------------------------------------------------------------------------
_INSTANCES: Dict[str, List[str]] = {"video": [], "search": []}
_INST_TS = 0.0
_HEALTH: Dict[str, Dict[str, float]] = {}


def _score(base: str) -> float:
    h = _HEALTH.get(base)
    if not h:
        return 0.5
    if h["fail"] >= 3 and time.time() - h["last"] < 180:
        return 99.0
    return h["score"]


def _record(base: str, ok: bool, dt: float = 0.0) -> None:
    h = _HEALTH.setdefault(base, {"score": 0.6, "fail": 0, "last": 0.0})
    if ok:
        h["score"] = h["score"] * 0.7 + max(dt, 0.02) * 0.3
        h["fail"] = max(0, h["fail"] - 1)
    else:
        h["fail"] += 1
        h["score"] = h["score"] * 0.85 + 0.4
        h["last"] = time.time()


def _clean_instances(items: List[Any]) -> List[str]:
    out: List[str] = []
    for it in items:
        base = it.get("instance") if isinstance(it, dict) else it
        if not isinstance(base, str):
            continue
        base = base.strip().rstrip("/")
        if not base.startswith("http"):
            continue
        if base not in out:
            out.append(base)
    return out


async def refresh_instances() -> None:
    global _INST_TS
    if time.time() - _INST_TS < 240 and _INSTANCES["video"]:
        return
    _INST_TS = time.time()
    for kind, urls in (("video", LIST_VIDEO), ("search", LIST_SEARCH)):
        got: List[str] = []
        for u in urls:
            try:
                r = await client.get(u, timeout=3.0)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, list):
                        got += _clean_instances(data)
            except Exception:
                continue
            if got:
                break
        if not got:
            got = list(FALLBACK_INSTANCES)
        random.shuffle(got)
        _INSTANCES[kind] = got


def ordered_instances(kind: str, limit: int = 10) -> List[str]:
    pool = list(_INSTANCES.get(kind) or [])
    if not pool:
        pool = list(FALLBACK_INSTANCES)
    pool.sort(key=_score)
    return pool[:limit]


async def _probe(base: str, path: str, params: Optional[Dict[str, Any]], timeout: float):
    t0 = time.perf_counter()
    try:
        r = await client.get(base + path, params=params, timeout=timeout)
        if r.status_code != 200:
            _record(base, False)
            return None
        data = r.json()
        _record(base, True, time.perf_counter() - t0)
        return data
    except Exception:
        _record(base, False)
        return None


async def hedged(
    path: str,
    params: Optional[Dict[str, Any]] = None,
    kind: str = "video",
    valid=None,
    timeout: float = 3.0,
    hedge: float = 0.12,
    limit: int = 8,
) -> Optional[Any]:
    """最速応答を採用するヘッジングレース。120msごとに次のインスタンスを投入。"""
    await refresh_instances()
    targets = ordered_instances(kind, limit)
    if not targets:
        return None
    pending = set()
    it = iter(targets)
    try:
        while True:
            nxt = next(it, None)
            if nxt is not None:
                pending.add(asyncio.create_task(_probe(nxt, path, params, timeout)))
            if not pending:
                return None
            done, pending = await asyncio.wait(
                pending, timeout=hedge, return_when=asyncio.FIRST_COMPLETED
            )
            for t in done:
                if t.cancelled() or t.exception():
                    continue
                res = t.result()
                if res is None:
                    continue
                if valid is None or valid(res):
                    return res
    finally:
        for t in pending:
            t.cancel()


async def warmup() -> None:
    await asyncio.sleep(2)
    while True:
        await refresh_instances()
        for base in ordered_instances("video", 12):
            try:
                t0 = time.perf_counter()
                r = await client.get(base + "/api/v1/stats", timeout=2.0)
                _record(base, r.status_code == 200, time.perf_counter() - t0)
            except Exception:
                _record(base, False)
        await asyncio.sleep(180)


async def keepalive() -> None:
    self_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    await asyncio.sleep(60)
    while True:
        if self_url:
            try:
                await client.get(self_url + "/healthz", timeout=10)
            except Exception:
                pass
        await asyncio.sleep(600)


# --------------------------------------------------------------------------
# 正規化ユーティリティ
# --------------------------------------------------------------------------
def _thumb(vid: str, q: str = "mqdefault") -> str:
    return f"/proxy/thumb?v={quote(vid)}&q={q}"


def _avatar(url: str) -> str:
    if not url:
        return ""
    return "/proxy/img?url=" + quote(url, safe="")


def fmt_count(n: Any) -> str:
    try:
        n = int(n)
    except Exception:
        return str(n or "")
    if n >= 100_000_000:
        return f"{n / 100_000_000:.1f}億"
    if n >= 10_000:
        return f"{n / 10_000:.1f}万"
    return f"{n:,}"


def fmt_duration(sec: Any) -> str:
    try:
        sec = int(sec)
    except Exception:
        return ""
    if sec <= 0:
        return ""
    h, m, s = sec // 3600, (sec % 3600) // 60, sec % 60
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def _valid_list(res: Any) -> bool:
    if not isinstance(res, list) or not res:
        return False
    return any(isinstance(i, dict) and (i.get("videoId") or i.get("type")) for i in res)


def _valid_obj(res: Any) -> bool:
    return isinstance(res, dict) and not res.get("error") and bool(
        res.get("title") or res.get("videoId") or res.get("author")
    )


def norm_search_item(i: Dict[str, Any]) -> Dict[str, Any]:
    thumbs = i.get("videoThumbnails") or []
    thumb = ""
    if isinstance(thumbs, list):
        for t in reversed(thumbs):
            if isinstance(t, dict) and t.get("url"):
                thumb = t["url"]
                break
    vid = i.get("videoId") or ""
    return {
        "type": i.get("type"),
        "videoId": vid,
        "title": i.get("title") or "",
        "author": i.get("author") or "",
        "authorId": i.get("authorId") or "",
        "views": fmt_count(i.get("viewCount")) if i.get("viewCount") else (
            i.get("viewCountText") or ""
        ),
        "published": i.get("publishedText") or "",
        "duration": fmt_duration(i.get("lengthSeconds")),
        "thumb": _thumb(vid) if vid else (thumb or ""),
        "channelThumb": _avatar(
            (i.get("authorThumbnails") or [{}])[-1].get("url", "")
            if isinstance(i.get("authorThumbnails"), list) and i.get("authorThumbnails")
            else ""
        ),
        "playlistId": i.get("playlistId") or "",
        "videoCount": i.get("videoCount") or 0,
        "subCount": i.get("subCountText") or "",
    }


def norm_video(d: Dict[str, Any]) -> Dict[str, Any]:
    thumbs = d.get("authorThumbnails") or []
    icon = ""
    if isinstance(thumbs, list) and thumbs:
        icon = (thumbs[-1] or {}).get("url", "")
    return {
        "videoId": d.get("videoId") or "",
        "title": d.get("title") or "",
        "author": d.get("author") or "",
        "authorId": d.get("authorId") or "",
        "authorIcon": _avatar(icon),
        "subCount": d.get("subCountText") or "",
        "views": fmt_count(d.get("viewCount")),
        "likes": fmt_count(d.get("likeCount")),
        "published": d.get("publishedText") or "",
        "description": d.get("descriptionHtml") or (
            (d.get("description") or "").replace("\n", "<br>")
        ),
        "length": fmt_duration(d.get("lengthSeconds")),
        "live": bool(d.get("liveNow")),
        "related": [
            norm_search_item(r)
            for r in (d.get("recommendedVideos") or [])
            if isinstance(r, dict) and r.get("videoId")
        ],
    }


def norm_comment(c: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(c, dict):
        return None
    author = c.get("author")
    if isinstance(author, dict):
        name = author.get("name") or ""
        icon = author.get("avatar") or ""
        cid = author.get("channelId") or ""
        creator = bool(author.get("creator"))
        verified = bool(author.get("verified"))
    else:
        name = author or c.get("authorName") or ""
        thumbs = c.get("authorThumbnails") or []
        icon = thumbs[-1].get("url", "") if isinstance(thumbs, list) and thumbs else ""
        cid = c.get("authorId") or ""
        creator = bool(c.get("isCreator"))
        verified = bool(c.get("isVerified"))
    text = c.get("content") or c.get("text") or ""
    likes = c.get("likeCount")
    if isinstance(likes, dict):
        likes = likes.get("count")
    if likes is None and isinstance(c.get("likes"), dict):
        likes = c["likes"].get("count")
    replies = c.get("replyCount")
    if replies is None and isinstance(c.get("replies"), dict):
        replies = c["replies"].get("count")
    return {
        "author": name,
        "authorId": cid,
        "icon": _avatar(icon),
        "text": text,
        "published": c.get("publishedText") or c.get("publishedTime") or "",
        "likes": fmt_count(likes or 0),
        "replies": replies or 0,
        "creator": creator,
        "verified": verified,
    }


def norm_comment_payload(data: Any) -> Tuple[List[Dict[str, Any]], str]:
    if not isinstance(data, dict):
        return [], ""
    raw = data.get("comments")
    if not isinstance(raw, list):
        return [], ""
    out = [x for x in (norm_comment(c) for c in raw) if x]
    return out, data.get("continuation") or ""


# --------------------------------------------------------------------------
# コメント取得（3API同時レース）
# --------------------------------------------------------------------------
async def _comments_invidious(v: str, cont: Optional[str]):
    params = {"continuation": cont} if cont else None
    data = await hedged(f"/api/v1/comments/{v}", params, valid=lambda r: isinstance(r, dict))
    if not isinstance(data, dict):
        raise RuntimeError("invidious comments")
    return data


async def _comments_sia(v: str):
    r = await client.get(f"{SIA_BASE}/api/comments", params={"videoId": v}, timeout=3.0)
    r.raise_for_status()
    return r.json()


async def _comments_sennin(v: str, cont: Optional[str] = None):
    params = {"videoId": v, "sort": "top"}
    if cont:
        params["continuation"] = cont
    r = await client.get(f"{SENNIN_BASE}/api/comments", params=params, timeout=3.0)
    r.raise_for_status()
    return r.json()


async def fetch_comments(v: str, cont: Optional[str] = None) -> Dict[str, Any]:
    key = f"c:{v}:{cont or ''}"

    async def impl():
        tasks = {
            asyncio.create_task(_comments_invidious(v, cont)): "invidious",
            asyncio.create_task(_comments_sia(v)): "sia",
            asyncio.create_task(_comments_sennin(v, cont)): "sennin",
        }
        pending = set(tasks)
        try:
            while pending:
                done, pending = await asyncio.wait(
                    pending, timeout=3.2, return_when=asyncio.FIRST_COMPLETED
                )
                for t in done:
                    if t.cancelled() or t.exception():
                        continue
                    comments, next_cont = norm_comment_payload(t.result())
                    if comments:
                        return {
                            "comments": comments,
                            "continuation": next_cont,
                            "source": tasks[t],
                        }
                if not done:
                    break
        finally:
            for t in pending:
                t.cancel()
        return {"comments": [], "continuation": "", "source": "none"}

    res = await cached(key, 90.0, impl, timeout=10.0)
    return res or {"comments": [], "continuation": "", "source": "none"}


# --------------------------------------------------------------------------
# ストリーム取得（5API同時レース）
# --------------------------------------------------------------------------
def _norm_formats(items: Any, kind: str, audio: str = "") -> List[Dict[str, str]]:
    out = []
    if not isinstance(items, list):
        return out
    for it in items:
        if not isinstance(it, dict):
            continue
        url = it.get("url") or it.get("streamUrl")
        if not url:
            continue
        res = (
            it.get("qualityLabel")
            or it.get("quality")
            or it.get("formatNote")
            or it.get("height")
            or "auto"
        )
        if isinstance(res, int):
            res = f"{res}p"
        out.append({"url": url, "resolution": str(res), "format": kind, "audioUrl": audio})
    return out


async def _stream_sia(v: str):
    r = await client.get(f"{SIA_BASE}/api/stream/{v}", timeout=2.6)
    r.raise_for_status()
    d = r.json()
    streams = d.get("streams") or {}
    audio = ""
    for a in (streams.get("audioOnly") or []):
        audio = a.get("streamUrl") or a.get("url") or ""
        if audio:
            break
    urls = _norm_formats(streams.get("muxed") or d.get("formats"), "mp4/mixed")
    urls += _norm_formats(streams.get("videoOnly"), "mp4/videoOnly", audio)
    hls = d.get("hls") or d.get("m3u8")
    if isinstance(hls, str) and hls:
        urls.append({"url": hls, "resolution": "HLS", "format": "application/x-mpegURL", "audioUrl": ""})
    return {"streamUrls": urls, "source": "sia"}


async def _stream_piped(v: str):
    for base in random.sample(PIPED_INSTANCES, len(PIPED_INSTANCES)):
        try:
            r = await client.get(f"{base}/streams/{v}", timeout=2.6)
            if r.status_code != 200:
                continue
            d = r.json()
            audio = ""
            for a in (d.get("audioStreams") or []):
                audio = a.get("url") or ""
                if audio:
                    break
            urls = []
            for s in (d.get("videoStreams") or []):
                if s.get("videoOnly", True):
                    continue
                urls.append(
                    {
                        "url": s.get("url"),
                        "resolution": s.get("quality") or "auto",
                        "format": "mp4/mixed",
                        "audioUrl": "",
                    }
                )
            if not urls:
                urls = _norm_formats(
                    [s for s in (d.get("videoStreams") or []) if s.get("videoOnly", True)],
                    "mp4/videoOnly",
                    audio,
                )
            if d.get("hls"):
                urls.append(
                    {"url": d["hls"], "resolution": "HLS", "format": "application/x-mpegURL", "audioUrl": ""}
                )
            if urls:
                return {"streamUrls": [u for u in urls if u["url"]], "source": "piped"}
        except Exception:
            continue
    raise RuntimeError("piped")


async def _stream_rapid(v: str):
    for key in random.sample(_RAPID_KEYS, len(_RAPID_KEYS)):
        try:
            r = await client.get(
                f"https://{RAPID_HOST}/dl",
                params={"id": v},
                headers={"X-RapidAPI-Key": base64.b64decode(key).decode(), "X-RapidAPI-Host": RAPID_HOST},
                timeout=2.6,
            )
            if r.status_code != 200:
                continue
            d = r.json()
            ad
