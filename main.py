"""んょしtube - 高速YouTubeビューア (FastAPI)"""
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
    except Exception:
        if not fut.done():
            fut.set_exception(RuntimeError("failed"))
            fut.exception()
        _INFLIGHT.pop(key, None)
        return _cache_get(key)
    _INFLIGHT.pop(key, None)
    if res is not None:
        _cache_set(key, res, ttl)
    if not fut.done():
        fut.set_result(res)
    return res


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
                        got = got + _clean_instances(data)
            except Exception:
                continue
            if got:
                break
        if not got:
            got = list(FALLBACK_INSTANCES)
        random.shuffle(got)
        _INSTANCES[kind] = got


def ordered_instances(kind: str, limit: int = 6) -> List[str]:
    pool = list(_INSTANCES.get(kind) or [])
    if not pool:
        pool = list(FALLBACK_INSTANCES)
    pool.sort(key=_score)
    return pool[:limit]


async def race(coros, budget: float, check=None):
    tasks = [asyncio.create_task(c) for c in coros]
    pending = set(tasks)
    end = time.monotonic() + budget
    try:
        while pending:
            left = end - time.monotonic()
            if left <= 0:
                return None
            done, pending = await asyncio.wait(
                pending, timeout=left, return_when=asyncio.FIRST_COMPLETED
            )
            for t in done:
                if t.cancelled() or t.exception():
                    continue
                r = t.result()
                if r is not None and (check is None or check(r)):
                    return r
        return None
    finally:
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


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
    limit: int = 6,
):
    await refresh_instances()
    targets = ordered_instances(kind, limit)
    if not targets:
        return None
    return await race(
        [_probe(b, path, params, timeout) for b in targets], 8.0, valid
    )


async def warmup() -> None:
    await asyncio.sleep(2)
    while True:
        await refresh_instances()
        for base in ordered_instances("video", 12):
            t0 = time.perf_counter()
            try:
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
    if n >= 100000000:
        return f"{n / 100000000:.1f}億"
    if n >= 10000:
        return f"{n / 10000:.1f}万"
    return f"{n:,}"


def fmt_duration(sec: Any) -> str:
    try:
        sec = int(sec)
    except Exception:
        return ""
    if sec <= 0:
        return ""
    h, m, s = sec // 3600, (sec % 3600) // 60, sec % 60
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


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
    author_thumbs = i.get("authorThumbnails") or []
    channel_thumb = ""
    if isinstance(author_thumbs, list) and author_thumbs:
        channel_thumb = (author_thumbs[-1] or {}).get("url", "")
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
        "channelThumb": _avatar(channel_thumb),
        "playlistId": i.get("playlistId") or "",
        "videoCount": i.get("videoCount") or 0,
        "subCount": i.get("subCountText") or "",
    }


def norm_video(d: Dict[str, Any]) -> Dict[str, Any]:
    thumbs = d.get("authorThumbnails") or []
    icon = ""
    if isinstance(thumbs, list) and thumbs:
        icon = (thumbs[-1] or {}).get("url", "")
    related = []
    for r in d.get("recommendedVideos") or []:
        if isinstance(r, dict) and r.get("videoId"):
            related.append(norm_search_item(r))
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
        "related": related,
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
    else:
        name = author or ""
        thumbs = c.get("authorThumbnails") or []
        icon = ""
        if isinstance(thumbs, list) and thumbs:
            icon = (thumbs[-1] or {}).get("url", "")
        cid = c.get("authorId") or ""
        creator = bool(c.get("isCreator"))
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
    }


def norm_comment_payload(data: Any) -> Tuple[List[Dict[str, Any]], str]:
    if not isinstance(data, dict):
        return [], ""
    raw = data.get("comments")
    if not isinstance(raw, list):
        return [], ""
    out = []
    for c in raw:
        n = norm_comment(c)
        if n:
            out.append(n)
    return out, data.get("continuation") or ""


async def _c_invidious(v: str, cont: Optional[str]):
    params = {"continuation": cont} if cont else None
    data = await hedged(
        f"/api/v1/comments/{v}", params, valid=lambda r: isinstance(r, dict)
    )
    if not isinstance(data, dict):
        return None
    c, n = norm_comment_payload(data)
    if not c:
        return None
    return {"comments": c, "continuation": n, "source": "invidious"}


async def _c_sia(v: str):
    r = await client.get(f"{SIA_BASE}/api/comments", params={"videoId": v}, timeout=3.0)
    if r.status_code != 200:
        return None
    c, n = norm_comment_payload(r.json())
    if not c:
        return None
    return {"comments": c, "continuation": n, "source": "sia"}


async def _c_sennin(v: str, cont: Optional[str]):
    params = {"videoId": v, "sort": "top"}
    if cont:
        params["continuation"] = cont
    r = await client.get(f"{SENNIN_BASE}/api/comments", params=params, timeout=3.0)
    if r.status_code != 200:
        return None
    c, n = norm_comment_payload(r.json())
    if not c:
        return None
    return {"comments": c, "continuation": n, "source": "sennin"}


async def fetch_comments(v: str, cont: Optional[str] = None) -> Dict[str, Any]:
    key = f"c:{v}:{cont or ''}"

    async def impl():
        res = await race(
            [_c_invidious(v, cont), _c_sia(v), _c_sennin(v, cont)],
            3.2,
            lambda r: bool(r.get("comments")),
        )
        if not res:
            return {"comments": [], "continuation": "", "source": "none"}
        return res

    out = await cached(key, 90.0, impl, timeout=8.0)
    if not out:
        return {"comments": [], "continuation": "", "source": "none"}
    return out


def _norm_formats(items: Any, kind: str, audio: str = "") -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
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
        out.append(
            {"url": url, "resolution": str(res), "format": kind, "audioUrl": audio}
        )
    return out


async def _s_sia(v: str):
    r = await client.get(f"{SIA_BASE}/api/stream/{v}", timeout=2.6)
    if r.status_code != 200:
        return None
    d = r.json()
    streams = d.get("streams") or {}
    audio = ""
    for a in streams.get("audioOnly") or []:
        audio = a.get("streamUrl") or a.get("url") or ""
        if audio:
            break
    urls = _norm_formats(streams.get("muxed") or d.get("formats"), "mp4/mixed")
    urls = urls + _norm_formats(streams.get("videoOnly"), "mp4/videoOnly", audio)
    hls = d.get("hls") or d.get("m3u8")
    if isinstance(hls, str) and hls:
        urls.append(
            {"url": hls, "resolution": "HLS", "format": "application/x-mpegURL", "audioUrl": ""}
        )
    if not urls:
        return None
    return {"streamUrls": urls, "source": "sia"}


async def _s_piped(v: str):
    for base in random.sample(PIPED_INSTANCES, len(PIPED_INSTANCES)):
        try:
            r = await client.get(f"{base}/streams/{v}", timeout=2.6)
            if r.status_code != 200:
                continue
            d = r.json()
            audio = ""
            for a in d.get("audioStreams") or []:
                audio = a.get("url") or ""
                if audio:
                    break
            urls = []
            for s in d.get("videoStreams") or []:
                if not s.get("videoOnly", True):
                    urls.append(
                        {
                            "url": s.get("url"),
                            "resolution": s.get("quality") or "auto",
                            "format": "mp4/mixed",
                            "audioUrl": "",
                        }
                    )
            if not urls:
                vo = [s for s in (d.get("videoStreams") or []) if s.get("videoOnly", True)]
                urls = _norm_formats(vo, "mp4/videoOnly", audio)
            if d.get("hls"):
                urls.append(
                    {"url": d["hls"], "resolution": "HLS", "format": "application/x-mpegURL", "audioUrl": ""}
                )
            urls = [u for u in urls if u.get("url")]
            if urls:
                return {"streamUrls": urls, "source": "piped"}
        except Exception:
            continue
    return None


async def _s_rapid(v: str):
    for key in random.sample(_RAPID_KEYS, len(_RAPID_KEYS)):
        try:
            r = await client.get(
                f"https://{RAPID_HOST}/dl",
                params={"id": v},
                headers={
                    "X-RapidAPI-Key": base64.b64decode(key).decode(),
                    "X-RapidAPI-Host": RAPID_HOST,
                },
                timeout=2.6,
            )
            if r.status_code != 200:
                continue
            d = r.json()
            adaptive = d.get("adaptiveFormats") or []
            audio = ""
            for a in adaptive:
                if str(a.get("mimeType", "")).startswith("audio/"):
                    audio = a.get("url") or ""
                    if audio:
                        break
            urls = _norm_formats(d.get("formats"), "mp4/mixed")
            urls = urls + _norm_formats(adaptive, "mp4/videoOnly", audio)
            if urls:
                return {"streamUrls": urls, "source": "rapidapi"}
        except Exception:
            continue
    return None


async def _s_zernio(v: str):
    target = f"https://www.youtube.com/watch?v={v}"
    for fid, res in ((2, "360p"), (3, "480p"), (4, "720p"), (5, "1080p"), (1, "240p")):
        try:
            r = await noredirect.get(
                "https://getlate.dev/api/tools/youtube-live-downloader",
                params={"url": target, "formatId": fid},
                timeout=2.8,
            )
            loc = r.headers.get("location")
            if r.status_code in (301, 302, 303, 307, 308) and loc:
                return {
                    "streamUrls": [
                        {"url": loc, "resolution": res, "format": "mp4/mixed", "audioUrl": ""}
                    ],
                    "source": "zernio",
                }
        except Exception:
            continue
    return None


async def _s_invidious(v: str):
    d = await hedged(
        f"/api/v1/videos/{v}",
        None,
        valid=lambda r: isinstance(r, dict)
        and bool(r.get("formatStreams") or r.get("adaptiveFormats")),
    )
    if not isinstance(d, dict):
        return None
    urls = _norm_formats(d.get("formatStreams"), "mp4/mixed")
    audio = ""
    for a in d.get("adaptiveFormats") or []:
        if "audio" in str(a.get("type", "")):
            audio = a.get("url") or ""
            if audio:
                break
    urls = urls + _norm_formats(d.get("adaptiveFormats"), "mp4/videoOnly", audio)
    if not urls:
        return None
    return {"streamUrls": urls, "source": "invidious"}


async def fetch_stream(v: str) -> Dict[str, Any]:
    key = f"s:{v}"

    async def impl():
        res = await race(
            [_s_sia(v), _s_invidious(v), _s_rapid(v), _s_piped(v), _s_zernio(v)],
            3.0,
            lambda r: bool(r.get("streamUrls")),
        )
        if not res:
            return {"streamUrls": [], "source": "none"}
        return res

    out = await cached(key, 60.0, impl, timeout=8.0)
    if not out:
        return {"streamUrls": [], "source": "none"}
    return out


async def fetch_search(q: str, page: int, typ: str) -> List[Dict[str, Any]]:
    key = f"q:{typ}:{q}:{page}"

    async def impl():
        data = await hedged(
            "/api/v1/search",
            {"q": q, "page": page, "type": typ},
            kind="search",
            valid=_valid_list,
            timeout=3.5,
        )
        if not isinstance(data, list):
            return None
        items = [norm_search_item(i) for i in data if isinstance(i, dict)]
        if typ == "video":
            items = [i for i in items if i["videoId"]]
        elif typ == "channel":
            items = [i for i in items if i["type"] == "channel"]
        else:
            items = [i for i in items if i["playlistId"]]
        return items

    return await cached(key, 120.0, impl) or []


async def fetch_video(v: str) -> Optional[Dict[str, Any]]:
    key = f"v:{v}"

    async def impl():
        data = await hedged(f"/api/v1/videos/{v}", None, valid=_valid_obj)
        if not isinstance(data, dict):
            return None
        return norm_video(data)

    return await cached(key, 240.0, impl)


async def fetch_trending(typ: str = "Default") -> List[Dict[str, Any]]:
    key = f"t:{typ}"

    async def impl():
        data = await hedged("/api/v1/trending", {"type": typ}, valid=_valid_list)
        if not isinstance(data, list):
            return None
        out = []
        for i in data:
            if isinstance(i, dict) and i.get("videoId"):
                out.append(norm_search_item(i))
        return out

    return await cached(key, 300.0, impl) or []


async def fetch_channel(cid: str, tab: str = "videos") -> Dict[str, Any]:
    key = f"ch:{cid}:{tab}"

    async def impl():
        info = await hedged(f"/api/v1/channels/{cid}", None, valid=_valid_obj)
        vids = await hedged(
            f"/api/v1/channels/{cid}/{tab}",
            None,
            valid=lambda r: isinstance(r, dict) and isinstance(r.get("videos"), list),
        )
        base = info if isinstance(info, dict) else {}
        thumbs = base.get("authorThumbnails") or []
        icon = (thumbs[-1] or {}).get("url", "") if thumbs else ""
        videos = []
        if isinstance(vids, dict):
            for i in vids.get("videos") or []:
                if isinstance(i, dict) and i.get("videoId"):
                    videos.append(norm_search_item(i))
        return {
            "id": cid,
            "name": base.get("author") or cid,
            "icon": _avatar(icon),
            "subs": base.get("subCountText") or "",
            "description": base.get("description") or "",
            "videos": videos,
        }

    out = await cached(key, 120.0, impl)
    if out:
        return out
    return {
        "id": cid,
        "name": cid,
        "icon": "",
        "subs": "",
        "description": "",
        "videos": [],
    }


async def fetch_playlist(pid: str) -> Dict[str, Any]:
    key = f"pl:{pid}"

    async def impl():
        data = await hedged(
            f"/api/v1/playlists/{pid}",
            None,
            valid=lambda r: isinstance(r, dict) and isinstance(r.get("videos"), list),
        )
        if not isinstance(data, dict):
            return None
        videos = []
        for i in data.get("videos") or []:
            if isinstance(i, dict):
                videos.append(norm_search_item(i))
        return {
            "id": pid,
            "title": data.get("title") or "プレイリスト",
            "author": data.get("author") or "",
            "videos": videos,
        }

    out = await cached(key, 180.0, impl)
    if out:
        return out
    return {"id": pid, "title": "", "author": "", "videos": []}


_ALLOWED = (
    "googlevideo.com",
    "ytimg.com",
    "ggpht.com",
    "googleusercontent.com",
    "youtube.com",
    "youtube-nocookie.com",
    "odycdn.com",
    "akamaized.net",
    "siatube.com",
    "wireway.ch",
    "private.coffee",
    "winscloud.net",
    "getlate.dev",
    "googleapis.com",
)


def _allowed(url: str) -> bool:
    p = urlparse(url)
    if p.scheme not in ("http", "https"):
        return False
    host = (p.hostname or "").lower()
    for a in _ALLOWED:
        if host == a or host.endswith("." + a):
            return True
    return False


@app.get("/proxy/thumb")
async def proxy_thumb(v: str, q: str = "mqdefault"):
    key = f"th:{v}:{q}"

    async def impl():
        try:
            r = await client.get(f"https://i.ytimg.com/vi/{v}/{q}.jpg", timeout=3.0)
            if r.status_code == 200:
                return r.content
        except Exception:
            return None
        return None

    data = await cached(key, 86400.0, impl)
    if not data:
        return Response(status_code=404)
    return Response(
        content=data,
        media_type="image/jpeg",
        headers={"cache-control": "public, max-age=86400"},
    )


@app.get("/proxy/img")
async def proxy_img(url: str):
    if not _allowed(url):
        return Response(status_code=400)
    key = f"im:{url}"

    async def impl():
        try:
            r = await client.get(url, timeout=4.0)
            if r.status_code == 200:
                return r.content
        except Exception:
            return None
        return None

    data = await cached(key, 86400.0, impl)
    if not data:
        return Response(status_code=404)
    return Response(
        content=data,
        media_type="image/jpeg",
        headers={"cache-control": "public, max-age=86400"},
    )


def _rewrite_manifest(body: str, base: str) -> str:
    out = []
    for line in body.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("#"):
            if 'URI="' in s:
                s = re.sub(
                    r'URI="([^"]+)"',
                    lambda m: 'URI="/proxy/stream?url=' + quote(urljoin(base, m.group(1))) + '"',
                    s,
                )
            out.append(s)
        else:
            out.append("/proxy/stream?url=" + quote(urljoin(base, s)))
    return "\n".join(out) + "\n"


@app.get("/proxy/stream")
async def proxy_stream(request: Request, url: str):
    if not _allowed(url):
        return JSONResponse({"error": "bad url"}, status_code=400)
    headers = {}
    rng = request.headers.get("range")
    if rng:
        headers["Range"] = rng

    if ".m3u8" in url:
        try:
            r = await client.get(url, headers=headers, timeout=8.0)
            return PlainTextResponse(
                _rewrite_manifest(r.text, str(r.url)),
                status_code=r.status_code,
                media_type="application/vnd.apple.mpegurl",
                headers={"cache-control": "no-cache"},
            )
        except Exception as exc:
            return JSONResponse({"error": str(exc)}, status_code=502)

    req = client.build_request("GET", url, headers=headers)
    try:
        resp = await client.send(req, stream=True)
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=502)

    fwd = {}
    for h in ("content-type", "content-length", "content-range", "accept-ranges"):
        if h in resp.headers:
            fwd[h] = resp.headers[h]
    fwd.setdefault("accept-ranges", "bytes")

    async def gen():
        try:
            async for chunk in resp.aiter_bytes(65536):
                yield chunk
        finally:
            await resp.aclose()

    return StreamingResponse(gen(), status_code=resp.status_code, headers=fwd)


def _res(t: asyncio.Task):
    if t.done() and not t.cancelled() and t.exception() is None:
        return t.result()
    return None


@app.get("/healthz")
async def healthz():
    return {"ok": True, "instances": len(_INSTANCES["video"])}


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    videos = await fetch_trending("Default")
    return templates.TemplateResponse(request, "home.html", {"videos": videos, "tab": "home"})


@app.get("/trending", response_class=HTMLResponse)
async def trending(request: Request, type: str = "Default"):
    videos = await fetch_trending(type)
    return templates.TemplateResponse(
        request, "home.html", {"videos": videos, "tab": "trending", "type": type}
    )


@app.get("/search", response_class=HTMLResponse)
async def search(request: Request, q: str = "", type: str = "video", page: int = 1):
    if not q.strip():
        return RedirectResponse("/", status_code=303)
    typ = type if type in ("video", "channel", "playlist") else "video"
    results = await fetch_search(q, page, typ)
    resp = templates.TemplateResponse(
        request,
        "search.html",
        {"q": q, "type": typ, "page": page, "results": results},
    )
    try:
        hist = json.loads(request.cookies.get("sh", "[]"))
    except Exception:
        hist = []
    hist = [q] + [h for h in hist if h != q]
    resp.set_cookie("sh", json.dumps(hist[:8]), max_age=2592000, httponly=True)
    return resp


@app.get("/suggest")
async def suggest(keyword: str = ""):
    if not keyword:
        return JSONResponse([])
    key = f"sg:{keyword}"

    async def impl():
        try:
            r = await client.get(
                "https://suggestqueries.google.com/complete/search",
                params={"client": "firefox", "q": keyword, "hl": "ja"},
                timeout=2.0,
            )
            d = json.loads(r.content.decode("utf-8", "replace"))
            if isinstance(d, list) and len(d) > 1:
                return d[1]
            return []
        except Exception:
            return []

    return JSONResponse(await cached(key, 600.0, impl) or [])


@app.get("/watch", response_class=HTMLResponse)
async def watch(request: Request, v: str = Query(...), list: Optional[str] = None):
    coros = [fetch_video(v), fetch_comments(v)]
    if list:
        coros.append(fetch_playlist(list))
    tasks = [asyncio.create_task(c) for c in coros]
    done, pending = await asyncio.wait(tasks, timeout=4.5)
    for t in pending:
        t.add_done_callback(lambda x: x.cancelled() or x.exception())

    video = _res(tasks[0])
    comments = _res(tasks[1])
    playlist = _res(tasks[2]) if list and len(tasks) > 2 else None

    if not isinstance(video, dict):
        return templates.TemplateResponse(request, "error.html", {"v": v}, status_code=502)

    cdata = comments if isinstance(comments, dict) else {}
    return templates.TemplateResponse(
        request,
        "watch.html",
        {
            "v": video,
            "comments": cdata.get("comments", []),
            "continuation": cdata.get("continuation", ""),
            "playlist": playlist if isinstance(playlist, dict) else None,
            "nocookie": NOCOOKIE_BASE + quote(v, safe=""),
        },
    )


@app.get("/shorts/{v}", response_class=HTMLResponse)
async def shorts(request: Request, v: str):
    video = await fetch_video(v)
    if not video:
        return templates.TemplateResponse(request, "error.html", {"v": v}, status_code=502)
    return templates.TemplateResponse(
        request, "shorts.html", {"v": video, "nocookie": NOCOOKIE_BASE + quote(v, safe="")}
    )


@app.get("/channel/{cid}", response_class=HTMLResponse)
async def channel(request: Request, cid: str, tab: str = "videos"):
    tab = tab if tab in ("videos", "shorts", "streams") else "videos"
    ch = await fetch_channel(cid, tab)
    return templates.TemplateResponse(request, "channel.html", {"ch": ch, "tab": tab})


@app.get("/playlist", response_class=HTMLResponse)
async def playlist(request: Request, list: str = Query(...)):
    pl = await fetch_playlist(list)
    return templates.TemplateResponse(request, "playlist.html", {"pl": pl})


@app.get("/api/comments")
async def api_comments(v: str, continuation: Optional[str] = None):
    return JSONResponse(await fetch_comments(v, continuation))


@app.get("/api/stream")
async def api_stream(v: str):
    return JSONResponse(await fetch_stream(v))


@app.get("/api/search")
async def api_search(q: str, page: int = 1, type: str = "video"):
    return JSONResponse(await fetch_search(q, page, type))


@app.get("/api/suggest")
async def api_suggest(keyword: str = ""):
    return await suggest(keyword)


@app.get("/api/recommended")
async def api_recommended(request: Request):
    try:
        hist = json.loads(request.cookies.get("sh", "[]"))
    except Exception:
        hist = []
    words = hist[:4] or ["音楽", "ニュース", "料理", "ゲーム実況"]
    lists = await asyncio.gather(*[fetch_search(w, 1, "video") for w in words])
    seen = set()
    out = []
    for items in lists:
        for i in items:
            if i["videoId"] in seen:
                continue
            seen.add(i["videoId"])
            out.append(i)
    random.shuffle(out)
    return JSONResponse(out[:24])


@app.get("/api/stats")
async def api_stats():
    rows = []
    for k, v in _HEALTH.items():
        rows.append({"instance": k, "score": round(v["score"], 3), "fail": v["fail"]})
    rows.sort(key=lambda x: x["score"])
    return JSONResponse(
        {"instances": len(_INSTANCES["video"]), "health": rows[:20], "cache": len(_CACHE)}
    )


@app.get("/favicon.ico")
async def favicon():
    return RedirectResponse("/proxy/thumb?v=dQw4w9WgXcQ&q=default", status_code=302)


@app.exception_handler(404)
async def not_found(request: Request, exc):
    return templates.TemplateResponse(request, "error.html", {"v": ""}, status_code=404)


@app.on_event("startup")
async def startup():
    asyncio.create_task(warmup())
    asyncio.create_task(keepalive())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
