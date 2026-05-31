import asyncio
import logging
import os
import re
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

# Windows-та .exe, Linux-та жоқ
_EXE = ".exe" if sys.platform == "win32" else ""

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
import yt_dlp
from hydrogram import Client

import base64

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID    = os.getenv("API_ID")
API_HASH  = os.getenv("API_HASH")
DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# Railway env var арқылы cookies жүктеу
# COOKIES_CONTENT_1, COOKIES_CONTENT_2, ... форматын қолдайды (үлкен файлдар үшін)
# немесе COOKIES_CONTENT (бір var, кішкентай файлдар үшін)
COOKIES_FILE = Path("cookies.txt")

def _load_cookies_from_env() -> None:
    import gzip as _gz
    # Бірнеше chunk жинайды
    chunks = []
    for i in range(1, 20):
        chunk = os.getenv(f"COOKIES_CONTENT_{i}")
        if not chunk:
            break
        chunks.append(chunk)

    if not chunks:
        single = os.getenv("COOKIES_CONTENT")
        if single:
            chunks = [single]

    if not chunks:
        return

    try:
        b64 = "".join(chunks)
        raw = base64.b64decode(b64)
        try:
            data = _gz.decompress(raw)
        except Exception:
            data = raw
        COOKIES_FILE.write_bytes(data)
    except Exception as e:
        print(f"[cookies] env-дан жүктеу қатесі: {e}")

_load_cookies_from_env()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Кез келген http/https сілтемені қабылдайды
URL_REGEX = re.compile(r"https?://\S+", re.IGNORECASE)

# YouTube домендері — арнайы extractor_args қолдану үшін
YOUTUBE_DOMAINS = ("youtube.com", "youtu.be", "youtube-nocookie.com")

# TikTok домендері — браузер impersonation керек
TIKTOK_DOMAINS = ("tiktok.com", "vm.tiktok.com", "vt.tiktok.com")

# Кіруді қажет ететін сайттар — cookies.txt пайдаланады
AUTH_DOMAINS = (
    "instagram.com", "facebook.com", "fb.com", "fb.watch",
    "threads.net", "threads.com", "tiktok.com",
    "vk.com", "vk.ru", "vkvideo.ru",
)

USER_URL_KEY = "dl_url"
ACTIVE_USERS: set[int] = set()

def _is_threads(url: str) -> bool:
    return any(d in url.lower() for d in ("threads.net", "threads.com"))

# Query параметрлерін алып тастайтын домендер
CLEAN_URL_DOMAINS = (
    "threads.net", "threads.com",
    "instagram.com",
    "tiktok.com",
    "vk.com", "vk.ru",
)


def _clean_url(url: str) -> str:
    """Query параметрлерін алып тастайды және домендерді түзетеді."""
    from urllib.parse import urlparse, urlunparse
    # yt-dlp тек threads.net қолдайды
    url = url.replace("www.threads.com", "www.threads.net").replace("threads.com", "threads.net")
    parsed = urlparse(url)
    if any(d in parsed.netloc for d in CLEAN_URL_DOMAINS):
        return urlunparse(parsed._replace(query="", fragment=""))
    return url


def _patch_cookies_for_threads(path: str) -> None:
    """threads.com cookies-ін threads.net-ке де жазады."""
    try:
        content = Path(path).read_text(encoding="utf-8")
        lines = content.splitlines()
        extra = []
        for line in lines:
            if line.startswith(".threads.com") or line.startswith("threads.com"):
                extra.append(line.replace("threads.com", "threads.net", 1))
        if extra:
            with open(path, "a", encoding="utf-8") as f:
                f.write("\n" + "\n".join(extra))
    except Exception:
        pass


def _make_progress_hook(loop: asyncio.AbstractEventLoop, msg):
    """yt-dlp жүктеу барысында хабарды жаңартады."""
    last_pct = [0]

    def hook(d: dict) -> None:
        if d.get("status") != "downloading":
            return
        total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
        downloaded = d.get("downloaded_bytes", 0)
        if total <= 0:
            return
        pct = int(downloaded * 100 / total)
        if pct - last_pct[0] < 10:
            return
        last_pct[0] = pct
        speed = d.get("speed") or 0
        eta = d.get("eta") or 0
        speed_str = f" • {speed / 1024 / 1024:.1f} МБ/с" if speed > 0 else ""
        eta_str = f" • ⏱{eta}с" if 0 < eta < 3600 else ""
        asyncio.run_coroutine_threadsafe(
            msg.edit_text(f"⬇️ Жүктелуде... {pct}%{speed_str}{eta_str}"),
            loop,
        )

    return hook


def _format_duration(seconds: int | None) -> str:
    """Секундты 1:23:45 форматына айналдырады."""
    if not seconds:
        return ""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


# ---------------------------------------------------------------------------
# Threads жүктеуші (Instagram API арқылы)
# ---------------------------------------------------------------------------

def _load_cookies_dict() -> dict:
    """cookies.txt → {name: value} сөздігі"""
    cookies: dict = {}
    if not COOKIES_FILE.exists():
        return cookies
    for line in COOKIES_FILE.read_text(encoding="utf-8").splitlines():
        if line.startswith("#") or "\t" not in line:
            continue
        parts = line.strip().split("\t")
        if len(parts) >= 7:
            cookies[parts[5]] = parts[6]
    return cookies


def _threads_download(url: str, out_dir: Path) -> tuple[Path, str]:
    """
    Threads постынан видеоны Playwright headless браузер арқылы жүктейді.
    Нақты браузер сессиясы — cookies.txt қажет.
    """
    import requests
    import re
    from playwright.sync_api import sync_playwright

    # cookies.txt → Playwright форматы
    pw_cookies = []
    if COOKIES_FILE.exists():
        for line in COOKIES_FILE.read_text(encoding="utf-8").splitlines():
            if line.startswith("#") or "\t" not in line:
                continue
            parts = line.strip().split("\t")
            if len(parts) >= 7:
                pw_cookies.append({
                    "name": parts[5],
                    "value": parts[6],
                    "domain": parts[0].lstrip("."),
                    "path": parts[2],
                    "secure": parts[3] == "TRUE",
                })

    clean_url = url.split("?")[0].rstrip("/")
    video_urls: list[str] = []
    title = clean_url.split("/post/")[-1]

    # Linux/Railway үшін --no-sandbox міндетті
    launch_args = ["--no-sandbox", "--disable-setuid-sandbox",
                   "--disable-dev-shm-usage", "--disable-gpu"]

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=launch_args)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        if pw_cookies:
            ctx.add_cookies(pw_cookies)

        page = ctx.new_page()

        def _on_response(resp):
            ru = resp.url
            if ".mp4" in ru and ("cdninstagram.com" in ru or "fbcdn.net" in ru):
                video_urls.append(ru)

        page.on("response", _on_response)

        try:
            page.goto(clean_url, wait_until="networkidle", timeout=40000)
        except Exception:
            # timeout болса да жиналған URL-дерді тексереміз
            pass

        # Видео элементінің src-ін тікелей алу (fallback)
        if not video_urls:
            try:
                src = page.evaluate("""
                    () => {
                        const v = document.querySelector('video');
                        return v ? v.src || v.currentSrc : null;
                    }
                """)
                if src and "mp4" in src:
                    video_urls.append(src)
            except Exception:
                pass

        # OG description → атауы
        try:
            og = page.query_selector('meta[property="og:description"]')
            if og:
                title = og.get_attribute("content") or title
        except Exception:
            pass

        # Debug: бет атауы
        try:
            page_title = page.title()
            logger.info(f"Threads page title: {page_title}")
        except Exception:
            pass

        browser.close()

    if not video_urls:
        raise Exception(
            "Threads видео URL табылмады.\n"
            "Cookies ескірген болуы мүмкін — жаңа cookies.txt жіберіңіз."
        )

    # Ең жоғары сапалы URL таңдайды (C3.XXXX width параметрі бойынша)
    def _quality(u: str) -> int:
        m = re.search(r"C3\.(\d+)", u)
        return int(m.group(1)) if m else 0

    video_urls.sort(key=_quality, reverse=True)
    best_url = video_urls[0]

    # Жүктейді
    uid = uuid.uuid4().hex[:8]
    out_path = out_dir / f"threads_{uid}.mp4"
    resp = requests.get(
        best_url,
        headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.threads.net/"},
        timeout=300,
        stream=True,
    )
    resp.raise_for_status()
    with open(out_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)

    return out_path, title[:80]


def _tiktok_download_playwright(url: str, out_dir: Path) -> tuple[Path, str]:
    """TikTok видеосын Playwright арқылы жүктейді — мобильді API блокталса қолданылады
    (Railway датацентр IP-сінде yt-dlp 'status code 0' қайтарады)."""
    import requests
    from playwright.sync_api import sync_playwright

    # cookies.txt → Playwright форматы
    pw_cookies = []
    if COOKIES_FILE.exists():
        for line in COOKIES_FILE.read_text(encoding="utf-8").splitlines():
            if line.startswith("#") or "\t" not in line:
                continue
            parts = line.strip().split("\t")
            if len(parts) >= 7:
                pw_cookies.append({
                    "name": parts[5],
                    "value": parts[6],
                    "domain": parts[0].lstrip("."),
                    "path": parts[2],
                    "secure": parts[3] == "TRUE",
                })

    clean_url = url.split("?")[0].rstrip("/")
    video_urls: list[str] = []
    title = clean_url.split("/video/")[-1].split("/")[0]

    # Linux/Railway үшін --no-sandbox міндетті
    launch_args = ["--no-sandbox", "--disable-setuid-sandbox",
                   "--disable-dev-shm-usage", "--disable-gpu"]

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=launch_args)
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
                "Mobile/15E148 Safari/604.1"
            ),
            locale="en-US",
            viewport={"width": 390, "height": 844},
        )
        if pw_cookies:
            ctx.add_cookies(pw_cookies)

        page = ctx.new_page()

        def _on_response(resp):
            ru = resp.url
            if any(cdn in ru for cdn in ("tiktokcdn.com", "tiktok.com")) and \
               any(ext in ru for ext in (".mp4", "video/mp4", "playAddr")):
                video_urls.append(ru)

        page.on("response", _on_response)

        try:
            page.goto(clean_url, wait_until="networkidle", timeout=40000)
        except Exception:
            # timeout болса да жиналған URL-дерді тексереміз
            pass

        # Видео элементінің src-ін тікелей алу (fallback)
        if not video_urls:
            try:
                src = page.evaluate("""
                    () => {
                        const v = document.querySelector('video');
                        return v ? (v.src || v.currentSrc) : null;
                    }
                """)
                if src and src.startswith("http"):
                    video_urls.append(src)
            except Exception:
                pass

        # Бет атауы
        try:
            og = page.query_selector('meta[property="og:title"]')
            if og:
                title = og.get_attribute("content") or title
        except Exception:
            pass

        browser.close()

    if not video_urls:
        raise Exception("TikTok видео URL табылмады (cookies ескірген болуы мүмкін).")

    # Ең ұзын URL-ді таңдайды (толық сапалы URL)
    best_url = max(video_urls, key=len)

    uid = uuid.uuid4().hex[:8]
    out_path = out_dir / f"tiktok_{uid}.mp4"
    resp = requests.get(
        best_url,
        headers={
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15",
            "Referer": "https://www.tiktok.com/",
        },
        timeout=300,
        stream=True,
    )
    resp.raise_for_status()
    with open(out_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)

    return out_path, title[:80]


def _tiktok_download_api(url: str, out_dir: Path, hd: bool = True) -> tuple[Path, str]:
    """TikTok-ты tikwm.com тегін API арқылы жүктейді (су таңбасыз, кілтсіз).
    Датацентр IP-де де (Railway) жұмыс істейді — видеоны tikwm сервері алып береді."""
    import requests
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    }
    r = requests.post(
        "https://www.tikwm.com/api/",
        data={"url": url, "hd": 1},
        headers=headers,
        timeout=60,
    )
    r.raise_for_status()
    j = r.json()
    if j.get("code") != 0 or not j.get("data"):
        raise Exception(f"tikwm қатесі: {j.get('msg', 'белгісіз')}")
    data = j["data"]
    if hd:
        video_url = data.get("hdplay") or data.get("play") or data.get("wmplay")
    else:
        video_url = data.get("play") or data.get("hdplay") or data.get("wmplay")
    title = data.get("title") or "TikTok видео"
    if not video_url:
        raise Exception("tikwm: видео URL табылмады")
    if video_url.startswith("/"):
        video_url = "https://www.tikwm.com" + video_url
    uid = uuid.uuid4().hex[:8]
    out_path = out_dir / f"tiktok_{uid}.mp4"
    vr = requests.get(video_url, headers=headers, timeout=300, stream=True)
    vr.raise_for_status()
    with open(out_path, "wb") as f:
        for chunk in vr.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    if not out_path.exists() or out_path.stat().st_size == 0:
        raise Exception("tikwm: бос файл жүктелді")
    return out_path, title[:80]


def _tiktok_download(url: str, out_dir: Path, hd: bool = True) -> tuple[Path, str]:
    """TikTok жүктеу әдістерін кезекпен сынайды: tikwm API → Playwright браузер."""
    errors = []
    for name, fn in (("tikwm", lambda u, d: _tiktok_download_api(u, d, hd)),
                     ("playwright", _tiktok_download_playwright)):
        try:
            path, title = fn(url, out_dir)
            if path and Path(path).exists() and Path(path).stat().st_size > 0:
                logger.info(f"TikTok сәтті жүктелді ({name})")
                return path, title
        except Exception as e:
            logger.warning(f"TikTok {name} сәтсіз: {e}")
            errors.append(f"{name}: {e}")
    raise Exception("TikTok жүктелмеді. " + " | ".join(errors))


# ---------------------------------------------------------------------------
# FFmpeg жолы
# ---------------------------------------------------------------------------

def _find_ffmpeg() -> str:
    exe = shutil.which("ffmpeg")
    if exe:
        return str(Path(exe).parent)
    winget = Path.home() / "AppData/Local/Microsoft/WinGet/Packages"
    for p in sorted(winget.glob("Gyan.FFmpeg_*/ffmpeg-*/bin"), reverse=True):
        if (p / f"ffmpeg{_EXE}").exists():
            return str(p)
    return ""


FFMPEG_DIR = _find_ffmpeg()
logger.info(f"FFmpeg: {FFMPEG_DIR or 'табылмады'}")


def _is_youtube(url: str) -> bool:
    return any(d in url.lower() for d in YOUTUBE_DOMAINS)


def _is_tiktok(url: str) -> bool:
    return any(d in url.lower() for d in TIKTOK_DOMAINS)


def _needs_auth(url: str) -> bool:
    return any(d in url.lower() for d in AUTH_DOMAINS)


def _is_playlist_url(url: str) -> bool:
    """Толық плейлист сілтемесі ме? (бір видео емес).
    watch?v=...&list=... — жеке видео деп есептейміз (тек сол видеоны алады)."""
    u = url.lower()
    if "/playlist" in u:
        return True
    if "list=" in u and "watch?" not in u and "/shorts/" not in u and "/video/" not in u:
        return True
    return False


def _base_ydl_opts(url: str = "") -> dict:
    opts: dict = {
        "ffmpeg_location": FFMPEG_DIR,
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "legacyserverconnect": True,
        "retries": 3,
    }
    if _is_youtube(url):
        opts["extractor_args"] = {
            "youtube": {
                "player_client": ["tv_embedded", "android_vr", "ios", "android"],
            }
        }
        opts["socket_timeout"] = 60
        opts["retries"] = 5
        # YouTube форматын кеңейту
        opts["format_sort"] = ["res", "ext:mp4:m4a", "quality", "tbr"]

    if _is_tiktok(url):
        # EU West серверінен Singapore endpoint жақсы жауап береді
        opts["extractor_args"] = {
            "tiktok": {
                "api_hostname": ["api16-normal-c-alisg.tiktokv.com"],
                "app_name": ["musical_ly"],
                "app_version": ["26.1.3"],
            }
        }
        opts["http_headers"] = {
            "User-Agent": (
                "com.zhiliaoapp.musically/2023501030 "
                "(Linux; U; Android 13; en_US; Pixel 7; "
                "Build/TQ3A.230901.001; Cronet/58.0.2991.0)"
            )
        }

    # Cookies бар болса — барлық сайтқа қолданамыз
    if COOKIES_FILE.exists():
        opts["cookiefile"] = str(COOKIES_FILE)
    return opts


def _ydl_download_with_retry(opts: dict, url: str) -> dict:
    """Алдымен қалыпты жүктейді, сәтсіз болса баламалы параметрлермен қайталайды."""
    try:
        return _ydl_download(opts, url)
    except Exception as first_err:
        err_str = str(first_err).lower()
        # YouTube блогы — барлық client-тарды кезекпен сынайды
        if _is_youtube(url):
            for client in [["tv_embedded"], ["android_vr"], ["mweb"], ["ios"], ["android"],
                           ["tv_embedded", "mweb"], ["android_vr", "mweb"]]:
                retry_opts = dict(opts)
                retry_opts["extractor_args"] = {
                    "youtube": {"player_client": client}
                }
                try:
                    return _ydl_download(retry_opts, url)
                except Exception:
                    continue
        # TikTok — барлық hostname + app_name комбинацияларын сынайды
        if _is_tiktok(url):
            tiktok_configs = [
                # Singapore (EU West-ке жақын)
                {"api_hostname": ["api19-normal-c-alisg.tiktokv.com"], "app_name": ["musical_ly"], "app_version": ["26.1.3"]},
                {"api_hostname": ["api22-normal-c-alisg.tiktokv.com"], "app_name": ["musical_ly"], "app_version": ["26.1.3"]},
                {"api_hostname": ["api16-normal-c-alisg.tiktokv.com"], "app_name": ["musical_ly"], "app_version": ["35.1.3"]},
                # US East
                {"api_hostname": ["api16-normal-c-useast1a.tiktokv.com"], "app_name": ["musical_ly"], "app_version": ["26.1.3"]},
                {"api_hostname": ["api19-normal-c-useast1a.tiktokv.com"], "app_name": ["musical_ly"], "app_version": ["26.1.3"]},
                {"api_hostname": ["api22-normal-c-useast1a.tiktokv.com"], "app_name": ["musical_ly"], "app_version": ["26.1.3"]},
                # trill app
                {"api_hostname": ["api16-normal-c-alisg.tiktokv.com"], "app_name": ["trill"], "app_version": ["26.1.3"]},
            ]
            for cfg in tiktok_configs:
                retry_opts = dict(opts)
                retry_opts["extractor_args"] = {"tiktok": cfg}
                try:
                    return _ydl_download(retry_opts, url)
                except Exception:
                    continue

        # Auth/block қатесі болса — cookie немесе басқа параметрлермен retry
        if any(k in err_str for k in ("blocked", "login", "authentication",
                                       "registered", "cookies", "private")):
            retry_opts = dict(opts)
            # Instagram үшін cookies olmadan басқа user-agent-пен
            if "instagram.com" in url.lower():
                retry_opts["http_headers"] = {
                    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                                  "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
                }
                try:
                    return _ydl_download(retry_opts, url)
                except Exception:
                    pass
        raise


# ---------------------------------------------------------------------------
# Видео форматтарын анықтайды
# ---------------------------------------------------------------------------

def get_video_info(url: str) -> dict:
    opts = _base_ydl_opts(url)
    opts["skip_download"] = True

    def _extract(o: dict, process: bool = False) -> dict:
        with yt_dlp.YoutubeDL(o) as ydl:
            # process=False — форматтарды тексермейді, тек metadata алады
            info = ydl.extract_info(url, download=False, process=process)
            # process=False кейде formats жоқ болады — process=True retry
            if not info:
                info = ydl.extract_info(url, download=False, process=True)
            return info

    try:
        return _extract(opts, process=False)
    except Exception:
        # YouTube — барлық client-тарды сынайды
        if _is_youtube(url):
            for client in [["tv_embedded"], ["android_vr"], ["ios"], ["android"], ["mweb"]]:
                retry = dict(opts)
                retry["extractor_args"] = {"youtube": {"player_client": client}}
                try:
                    return _extract(retry, process=False)
                except Exception:
                    continue
        if _is_tiktok(url):
            tiktok_configs = [
                {"api_hostname": ["api19-normal-c-useast1a.tiktokv.com"], "app_name": ["musical_ly"], "app_version": ["26.1.3"]},
                {"api_hostname": ["api22-normal-c-useast1a.tiktokv.com"], "app_name": ["musical_ly"], "app_version": ["26.1.3"]},
                {"api_hostname": ["api16-normal-c-useast1a.tiktokv.com"], "app_name": ["trill"], "app_version": ["26.1.3"]},
                {"api_hostname": ["api19-normal-c-useast1a.tiktokv.com"], "app_name": ["musical_ly"], "app_version": ["35.1.3"]},
            ]
            for cfg in tiktok_configs:
                retry = dict(opts)
                retry["extractor_args"] = {"tiktok": cfg}
                try:
                    with yt_dlp.YoutubeDL(retry) as ydl:
                        return ydl.extract_info(url, download=False)
                except Exception:
                    continue
        raise


def get_available_video_qualities(info: dict) -> list[dict]:
    """Бар видео сапаларын кемуші ретпен қайтарады (өлшеммен)."""
    formats = info.get("formats", [])

    # Әр height үшін ең жақсы видео + аудио форматын табады
    best_video: dict[int, dict] = {}
    for f in formats:
        height = f.get("height")
        if not height or f.get("vcodec", "none") == "none":
            continue
        prev = best_video.get(height)
        fsize = f.get("filesize") or f.get("filesize_approx") or 0
        prev_size = (prev.get("filesize") or prev.get("filesize_approx") or 0) if prev else 0
        if prev is None or fsize > prev_size:
            best_video[height] = f

    # Ең жақсы аудио өлшемі
    best_audio_size = 0
    for f in formats:
        if f.get("vcodec", "none") == "none" and f.get("acodec", "none") != "none":
            s = f.get("filesize") or f.get("filesize_approx") or 0
            if s > best_audio_size:
                best_audio_size = s

    qualities: list[dict] = []
    for height, f in best_video.items():
        vsize = f.get("filesize") or f.get("filesize_approx") or 0
        total = vsize + best_audio_size  # video-only + audio merge болады
        if total == 0:
            size_str = "? МБ"
        else:
            mb = total / (1024 * 1024)
            size_str = f"{mb:.0f} МБ"
        qualities.append({"label": f"{height}p", "height": height, "size": size_str,
                          "bytes": total})

    qualities.sort(key=lambda x: x["height"], reverse=True)
    return qualities


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

async def cmd_debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    cookies_ok = COOKIES_FILE.exists()
    cookies_size = COOKIES_FILE.stat().st_size if cookies_ok else 0
    env_ok = bool(os.getenv("COOKIES_CONTENT"))
    ffmpeg_ok = bool(FFMPEG_DIR)
    await update.message.reply_text(
        f"🔧 Debug:\n"
        f"cookies.txt: {'✅ бар' if cookies_ok else '❌ жоқ'} ({cookies_size} байт)\n"
        f"COOKIES_CONTENT env: {'✅' if env_ok else '❌'}\n"
        f"FFmpeg: {'✅' if ffmpeg_ok else '❌'}\n"
        f"Python path: {COOKIES_FILE.resolve()}"
    )


async def cmd_getcookies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Cookies-ті Railway env var-лар үшін chunk-тарға бөліп береді."""
    import io, gzip

    if not COOKIES_FILE.exists():
        await update.message.reply_text("❌ cookies.txt жоқ. Алдымен жіберіңіз.")
        return

    msg = await update.message.reply_text("⏳ Дайындалуда...")

    # Gzip + base64
    raw = COOKIES_FILE.read_bytes()
    compressed = gzip.compress(raw, compresslevel=9)
    b64 = base64.b64encode(compressed).decode()

    CHUNK = 30000  # Railway лимитінен аз
    chunks = [b64[i:i+CHUNK] for i in range(0, len(b64), CHUNK)]
    n = len(chunks)

    orig_kb = len(raw) // 1024
    comp_kb = len(b64) // 1024

    await msg.edit_text(
        f"📊 {orig_kb}КБ → gzip → {comp_kb}КБ → {n} бөлікке бөлінді\n\n"
        f"Railway → Variables-қа <b>{n} жаңа айнымалы</b> қосыңыз:\n"
        + "\n".join(f"  <code>COOKIES_CONTENT_{i+1}</code>" for i in range(n))
        + "\n\nТөменде {n} файл жіберіледі — әрқайсысын тиісті айнымалыға қойыңыз.",
        parse_mode="HTML"
    )

    for i, chunk in enumerate(chunks, 1):
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=io.BytesIO(chunk.encode()),
            filename=f"COOKIES_CONTENT_{i}.txt",
            caption=f"Railway → Variables → <code>COOKIES_CONTENT_{i}</code>",
            parse_mode="HTML"
        )


async def cmd_setcookies(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🍪 Cookies файлын жіберіңіз:\n\n"
        "1. Компьютерде Chrome ашыңыз\n"
        "2. «Get cookies.txt LOCALLY» extension орнатыңыз\n"
        "3. Instagram немесе Threads-ке кіріңіз\n"
        "4. Export жасап, cookies.txt файлын осы ботқа жіберіңіз (файл ретінде)"
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    doc = update.message.document
    if not doc.file_name.endswith(".txt"):
        return
    file = await context.bot.get_file(doc.file_id)
    await file.download_to_drive(str(COOKIES_FILE))
    _patch_cookies_for_threads(str(COOKIES_FILE))
    await update.message.reply_text(
        f"✅ cookies.txt сақталды ({doc.file_size // 1024} КБ)\n"
        "Енді Instagram, Threads, TikTok, Facebook жұмыс істейді!"
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Сәлем! Видео сілтемесін жіберіңіз.\n\n"
        "Қолдайтын сайттар:\n"
        "• YouTube • Instagram • TikTok\n"
        "• Facebook • Threads • Twitter/X\n"
        "• және тағы 1000+ сайт"
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text.strip()

    # Трим күтіп тұрмыз ба? Қолданушы уақыт аралығын жіберуі керек (URL емес)
    if context.user_data.get("awaiting_trim"):
        await _handle_trim_input(update, context, text)
        return

    match = URL_REGEX.search(text)
    if not match:
        await update.message.reply_text(
            "Сілтеме табылмады. http:// немесе https:// басталатын сілтеме жіберіңіз."
        )
        return

    user_id = update.effective_user.id
    if user_id in ACTIVE_USERS:
        await update.message.reply_text("⏳ Алдыңғы жүктеу аяқталмады. Күте тұрыңыз.")
        return

    url = _clean_url(match.group(0))
    context.user_data[USER_URL_KEY] = url
    context.user_data.pop("dl_info", None)

    keyboard = [
        [
            InlineKeyboardButton("🎵 Аудио (MP3)", callback_data="type:audio"),
            InlineKeyboardButton("🎬 Видео (MP4)", callback_data="type:video"),
        ],
        [InlineKeyboardButton("✂️ Кесіп жүктеу", callback_data="type:trim")],
    ]

    # Threads — yt-dlp қолдамайды, тікелей кнопка көрсетеміз
    if _is_threads(url):
        if not COOKIES_FILE.exists():
            await update.message.reply_text(
                "🔐 Threads жүктеу үшін cookies керек.\n\n"
                "/setcookies командасын жіберіңіз."
            )
            return
        msg = await update.message.reply_text("🧵 Threads посты")
        await msg.edit_text("🧵 Threads посты\n\nНе жүктегіңіз келеді?",
                            reply_markup=InlineKeyboardMarkup(keyboard))
        return

    # Плейлист сілтемесі — арнайы өңдейміз (сілтеме түрі бойынша, get_video_info-сіз)
    if _is_playlist_url(url):
        pl_keyboard = [
            [InlineKeyboardButton("📋 Барлығын жүктеу", callback_data="type:playlist")],
            [
                InlineKeyboardButton("🎬 Тек 1-видео", callback_data="type:video"),
                InlineKeyboardButton("🎵 Тек 1-аудио", callback_data="type:audio"),
            ],
        ]
        await update.message.reply_text(
            "📋 Бұл плейлист сілтемесі.\n\n"
            "• «📋 Барлығын жүктеу» — бүкіл плейлисті жүктейді (макс. 25)\n"
            "• «🎬/🎵 Тек 1-…» — тек бірінші видеоны",
            reply_markup=InlineKeyboardMarkup(pl_keyboard),
        )
        return

    msg = await update.message.reply_text("🔍 Сілтеме тексерілуде...")
    try:
        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(None, get_video_info, url)
        context.user_data["dl_info"] = info

        title = info.get("title") or ""
        channel = info.get("channel") or info.get("uploader") or ""
        dur_str = _format_duration(info.get("duration"))
        is_playlist = info.get("_type") == "playlist"

        lines = [f"🎬 <b>{title[:100]}</b>"]
        meta = []
        if dur_str:
            meta.append(f"⏱ {dur_str}")
        if channel:
            meta.append(f"📺 {channel[:40]}")
        if meta:
            lines.append(" | ".join(meta))
        if is_playlist:
            count = len(info.get("entries") or [])
            lines.append(f"📋 Плейлист: {count} видео")
            # Плейлисті толық жүктеу батырмасы
            keyboard = keyboard + [[InlineKeyboardButton(
                f"📋 Барлығын жүктеу ({count})" if count else "📋 Барлығын жүктеу",
                callback_data="type:playlist")]]

        await msg.edit_text(
            "\n".join(lines),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
    except Exception as e:
        logger.warning(f"Preview қатесі: {e}")
        await msg.edit_text("Не жүктегіңіз келеді?", reply_markup=InlineKeyboardMarkup(keyboard))


async def handle_type_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    choice = query.data.split(":")[1]
    url = context.user_data.get(USER_URL_KEY)

    if not url:
        await query.edit_message_text("Сілтеме табылмады. Қайта жіберіңіз.")
        return

    if choice == "audio":
        await query.edit_message_text("⏳ Аудио жүктелуде, күте тұрыңыз...")
        await download_and_send_audio(query, context, url)

    elif choice == "trim":
        context.user_data["awaiting_trim"] = True
        await query.edit_message_text(
            "✂️ Қай аралықты кесейік?\n\n"
            "Уақытты былай жіберіңіз: <b>басы-соңы</b>\n"
            "Мысалы:\n"
            "• <code>1:20-2:45</code>\n"
            "• <code>0:30-1:15</code>\n"
            "• <code>1:02:00-1:05:30</code> (сағат:минут:секунд)",
            parse_mode="HTML",
        )

    elif choice == "playlist":
        await query.edit_message_text("📋 Плейлист дайындалуда...")
        await download_and_send_playlist(query, context, url)

    elif choice == "video":
        # Threads — бірден жүктейміз (бір сапа)
        if _is_threads(url):
            await query.edit_message_text("⏳ Видео жүктелуде, күте тұрыңыз...")
            await download_and_send_video(query, context, url, height=None)
            return

        # TikTok — HD/SD сапасын таңдатамыз
        if _is_tiktok(url):
            kb = [[
                InlineKeyboardButton("📹 HD (жоғары)", callback_data="ttq:hd"),
                InlineKeyboardButton("📱 SD (кішірек)", callback_data="ttq:sd"),
            ]]
            await query.edit_message_text(
                "🎬 TikTok сапасын таңдаңыз:",
                reply_markup=InlineKeyboardMarkup(kb),
            )
            return

        await query.edit_message_text("⏳ Сапалар анықталуда...")
        try:
            loop = asyncio.get_event_loop()
            info = context.user_data.get("dl_info") or await loop.run_in_executor(None, get_video_info, url)
            qualities = get_available_video_qualities(info)
            title = info.get("title") or ""
            context.user_data["dl_title"] = title
            context.user_data["dl_info"] = info

            if not qualities:
                # Сапа таңдау мүмкін емес — тікелей жүктейді
                await query.edit_message_text("⏳ Видео жүктелуде, күте тұрыңыз...")
                await download_and_send_video(query, context, url, height=None)
                return

            keyboard = []
            for q in qualities:
                mb_bytes = q.get("bytes", 0)
                size_str = q.get("size", "")
                if mb_bytes > 0 and mb_bytes > 50 * 1024 * 1024:
                    label = f"📹 {q['label']} — {size_str} ⚠️"
                elif size_str and size_str != "? МБ":
                    label = f"📹 {q['label']} — {size_str}"
                else:
                    label = f"📹 {q['label']}"
                keyboard.append([InlineKeyboardButton(label, callback_data=f"quality:{q['height']}")])
            keyboard.append([InlineKeyboardButton("⭐ Ең жоғары сапа", callback_data="quality:best")])
            keyboard.append([InlineKeyboardButton("❌ Бас тарту", callback_data="quality:cancel")])

            short_title = title[:40] + ("..." if len(title) > 40 else "")
            await query.edit_message_text(
                f"🎬 {short_title}\n\nСапаны таңдаңыз:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        except Exception as e:
            logger.error(f"Info error: {e}", exc_info=True)
            await query.edit_message_text(_format_error(str(e), url))


async def handle_quality_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    value = query.data.split(":")[1]

    if value == "cancel":
        await query.edit_message_text("Жүктеу бас тартылды.")
        return

    url = context.user_data.get(USER_URL_KEY)
    if not url:
        await query.edit_message_text("Сілтеме табылмады. Қайта жіберіңіз.")
        return

    if value == "best":
        await query.edit_message_text("⏳ Ең жоғары сапада жүктелуде...")
        await download_and_send_video(query, context, url, height=None)
    else:
        height = int(value)
        await query.edit_message_text(f"⏳ {height}p сапасында жүктелуде, күте тұрыңыз...")
        await download_and_send_video(query, context, url, height=height)


async def handle_tiktok_quality(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """TikTok HD/SD сапа таңдауын өңдейді."""
    query = update.callback_query
    await query.answer()
    quality = query.data.split(":")[1]  # hd немесе sd
    url = context.user_data.get(USER_URL_KEY)
    if not url:
        await query.edit_message_text("Сілтеме табылмады. Қайта жіберіңіз.")
        return
    context.user_data["tiktok_hd"] = (quality == "hd")
    await query.edit_message_text("⏳ TikTok жүктелуде...")
    await download_and_send_video(query, context, url, height=None)


# ---------------------------------------------------------------------------
# Жүктеу функциялары
# ---------------------------------------------------------------------------

async def download_and_send_audio(query, context, url: str) -> None:
    user_id = query.from_user.id
    if user_id in ACTIVE_USERS:
        await query.edit_message_text("⏳ Алдыңғы жүктеу аяқталмады.")
        return
    ACTIVE_USERS.add(user_id)

    # Threads — видеоны жүктеп, аудио шығарамыз
    if _is_threads(url):
        try:
            loop = asyncio.get_event_loop()
            video_path, title = await loop.run_in_executor(None, _threads_download, url, DOWNLOAD_DIR)
            uid = video_path.stem.split("_")[1]
            # FFmpeg арқылы аудио шығару
            ffmpeg = Path(FFMPEG_DIR) / f"ffmpeg{_EXE}" if FFMPEG_DIR else Path("ffmpeg")
            mp3_path = DOWNLOAD_DIR / f"audio_{uid}.mp3"
            subprocess.run([
                str(ffmpeg), "-y", "-i", str(video_path),
                "-vn", "-acodec", "libmp3lame", "-b:a", "320k", str(mp3_path)
            ], capture_output=True, check=True)
            video_path.unlink(missing_ok=True)
            await query.edit_message_text("📤 Жіберілуде...")
            with open(mp3_path, "rb") as f:
                await context.bot.send_audio(
                    chat_id=query.message.chat_id, audio=f,
                    title=title[:64], filename=f"{_safe_name(title)}.mp3",
                    read_timeout=600, write_timeout=600, connect_timeout=60,
                )
            await query.edit_message_text("✅ Аудио жіберілді!")
            mp3_path.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"Threads audio error: {e}", exc_info=True)
            await query.edit_message_text(f"❌ Threads аудио қатесі:\n{str(e)[:200]}")
        finally:
            ACTIVE_USERS.discard(user_id)
        return

    # TikTok — видеоны tikwm/Playwright арқылы алып, аудио шығарамыз
    if _is_tiktok(url):
        try:
            loop = asyncio.get_event_loop()
            await query.edit_message_text("⏳ TikTok жүктелуде...")
            v_path, tt_title = await loop.run_in_executor(None, _tiktok_download, url, DOWNLOAD_DIR)
            ffmpeg = Path(FFMPEG_DIR) / f"ffmpeg{_EXE}" if FFMPEG_DIR else Path("ffmpeg")
            uid = v_path.stem.split("_")[1] if "_" in v_path.stem else uuid.uuid4().hex[:8]
            mp3_path = DOWNLOAD_DIR / f"audio_{uid}.mp3"
            await query.edit_message_text("⚙️ Аудио шығарылуда...")
            subprocess.run([
                str(ffmpeg), "-y", "-i", str(v_path),
                "-vn", "-acodec", "libmp3lame", "-b:a", "320k", str(mp3_path)
            ], capture_output=True, check=True)
            v_path.unlink(missing_ok=True)
            await query.edit_message_text("📤 Жіберілуде...")
            with open(mp3_path, "rb") as af:
                await context.bot.send_audio(
                    chat_id=query.message.chat_id, audio=af,
                    title=str(tt_title)[:64], filename=f"{_safe_name(str(tt_title))}.mp3",
                    read_timeout=600, write_timeout=600, connect_timeout=60,
                )
            await query.edit_message_text("✅ Аудио жіберілді!")
            mp3_path.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"TikTok audio error: {e}", exc_info=True)
            await query.edit_message_text(f"❌ TikTok аудио жүктелмеді:\n{str(e)[:300]}")
        finally:
            ACTIVE_USERS.discard(user_id)
        return

    uid = uuid.uuid4().hex[:8]
    out_template = str(DOWNLOAD_DIR / f"audio_{uid}.%(ext)s")

    loop = asyncio.get_event_loop()
    opts = _base_ydl_opts(url)
    opts.update({
        "format": "bestaudio/best",
        "outtmpl": out_template,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "320",
        }],
        "progress_hooks": [_make_progress_hook(loop, query.message)],
    })

    try:
        info = await loop.run_in_executor(None, lambda: _ydl_download_with_retry(opts, url))
        title = info.get("title") or "audio"

        mp3_path = DOWNLOAD_DIR / f"audio_{uid}.mp3"
        if not mp3_path.exists():
            found = list(DOWNLOAD_DIR.glob(f"audio_{uid}.*"))
            if not found:
                await query.edit_message_text("Аудио файл жасалмады. Сілтемені тексеріңіз.")
                return
            mp3_path = found[0]

        size_mb = mp3_path.stat().st_size / (1024 * 1024)
        await query.edit_message_text("📤 Жіберілуде...")
        if size_mb > 50 and API_ID and API_HASH:
            await _pyro_send_audio(query.message.chat_id, mp3_path, title, query.message)
        else:
            with open(mp3_path, "rb") as f:
                await context.bot.send_audio(
                    chat_id=query.message.chat_id,
                    audio=f,
                    title=title[:64],
                    filename=f"{_safe_name(title)}.mp3",
                    read_timeout=600,
                    write_timeout=600,
                    connect_timeout=60,
                )
        await query.edit_message_text("✅ Аудио жіберілді!")
        mp3_path.unlink(missing_ok=True)

    except Exception as e:
        # TikTok аудио: API блокталса — браузер арқылы видео алып, аудио шығарамыз
        if _is_tiktok(url):
            try:
                await query.edit_message_text("⏳ TikTok браузер арқылы жүктелуде...")
                v_path, tt_title = await loop.run_in_executor(
                    None, _tiktok_download_playwright, url, DOWNLOAD_DIR
                )
                ffmpeg = Path(FFMPEG_DIR) / f"ffmpeg{_EXE}" if FFMPEG_DIR else Path("ffmpeg")
                mp3_path = DOWNLOAD_DIR / f"audio_{uid}.mp3"
                subprocess.run([
                    str(ffmpeg), "-y", "-i", str(v_path),
                    "-vn", "-acodec", "libmp3lame", "-b:a", "320k", str(mp3_path)
                ], capture_output=True, check=True)
                v_path.unlink(missing_ok=True)
                await query.edit_message_text("📤 Жіберілуде...")
                with open(mp3_path, "rb") as af:
                    await context.bot.send_audio(
                        chat_id=query.message.chat_id, audio=af,
                        title=str(tt_title)[:64], filename=f"{_safe_name(str(tt_title))}.mp3",
                        read_timeout=600, write_timeout=600, connect_timeout=60,
                    )
                await query.edit_message_text("✅ Аудио жіберілді!")
                mp3_path.unlink(missing_ok=True)
                return
            except Exception as e2:
                logger.error(f"TikTok audio fallback error: {e2}", exc_info=True)
        logger.error(f"Audio error: {e}", exc_info=True)
        for f in DOWNLOAD_DIR.glob(f"audio_{uid}.*"):
            f.unlink(missing_ok=True)
        await query.edit_message_text(_format_error(str(e), url))
    finally:
        ACTIVE_USERS.discard(user_id)


async def download_and_send_video(query, context, url: str, height: int | None) -> None:
    user_id = query.from_user.id
    if user_id in ACTIVE_USERS:
        await query.edit_message_text("⏳ Алдыңғы жүктеу аяқталмады.")
        return
    ACTIVE_USERS.add(user_id)

    # Threads — Instagram API арқылы жүктейміз
    if _is_threads(url):
        try:
            loop = asyncio.get_event_loop()
            video_path, title = await loop.run_in_executor(None, _threads_download, url, DOWNLOAD_DIR)
            await query.edit_message_text("⚙️ Telegram үшін өңделуде...")
            uid = video_path.stem.split("_")[1] if "_" in video_path.stem else uuid.uuid4().hex[:8]
            video_path = await loop.run_in_executor(None, lambda: _convert_for_telegram(video_path, uid))
            size_mb = video_path.stat().st_size / (1024 * 1024)
            vw, vh = _get_video_dimensions(video_path)
            await query.edit_message_text(f"📤 Жіберілуде... ({size_mb:.0f} МБ)")
            if size_mb > 50 and API_ID and API_HASH:
                await _pyro_send_video(query.message.chat_id, video_path, title, query.message, vw, vh)
            else:
                with open(video_path, "rb") as f:
                    await context.bot.send_video(
                        chat_id=query.message.chat_id, video=f,
                        caption=f"🎬 {title[:200]}",
                        filename=f"{_safe_name(title)}.mp4",
                        supports_streaming=True,
                        width=vw or None, height=vh or None,
                        read_timeout=600, write_timeout=600, connect_timeout=60,
                    )
            await query.edit_message_text("✅ Видео жіберілді!")
            video_path.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"Threads video error: {e}", exc_info=True)
            await query.edit_message_text(f"❌ Threads қатесі:\n{str(e)[:300]}")
        finally:
            ACTIVE_USERS.discard(user_id)
        return

    # TikTok — арнайы жол: tikwm API → Playwright резерві (Railway-де де істейді,
    # yt-dlp мобильді API датацентр IP-де бұғатталады)
    if _is_tiktok(url):
        try:
            loop = asyncio.get_event_loop()
            hd = context.user_data.get("tiktok_hd", True) if context else True
            await query.edit_message_text("⏳ TikTok жүктелуде...")
            video_path, title = await loop.run_in_executor(
                None, lambda: _tiktok_download(url, DOWNLOAD_DIR, hd))
            await query.edit_message_text("⚙️ Telegram үшін өңделуде...")
            uid = video_path.stem.split("_")[1] if "_" in video_path.stem else uuid.uuid4().hex[:8]
            video_path = await loop.run_in_executor(None, lambda: _convert_for_telegram(video_path, uid))
            size_mb = video_path.stat().st_size / (1024 * 1024)
            vw, vh = _get_video_dimensions(video_path)
            await query.edit_message_text(f"📤 Жіберілуде... ({size_mb:.0f} МБ)")
            if size_mb > 50 and API_ID and API_HASH:
                await _pyro_send_video(query.message.chat_id, video_path, title, query.message, vw, vh)
            else:
                with open(video_path, "rb") as f:
                    await context.bot.send_video(
                        chat_id=query.message.chat_id, video=f,
                        caption=f"🎬 {title[:200]}",
                        filename=f"{_safe_name(title)}.mp4",
                        supports_streaming=True,
                        width=vw or None, height=vh or None,
                        read_timeout=600, write_timeout=600, connect_timeout=60,
                    )
            await query.edit_message_text("✅ Видео жіберілді!")
            video_path.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"TikTok video error: {e}", exc_info=True)
            await query.edit_message_text(f"❌ TikTok жүктелмеді:\n{str(e)[:300]}")
        finally:
            ACTIVE_USERS.discard(user_id)
        return

    uid = uuid.uuid4().hex[:8]
    out_template = str(DOWNLOAD_DIR / f"video_{uid}.%(ext)s")

    # Форматты таңдайды
    # YouTube cloud client-тары (tv_embedded, android_vr) combined форматтарды береді
    if _is_youtube(url):
        if height:
            fmt = f"best[height<={height}]/best[height<={height*2}]/best"
        else:
            fmt = "best"
    elif height:
        fmt = (f"bestvideo[height<={height}][ext=mp4]+bestaudio[ext=m4a]/"
               f"bestvideo[height<={height}]+bestaudio/"
               f"best[height<={height}]/best")
    else:
        fmt = "bestvideo[ext=mp4]+bestaudio[ext=m4a]/bestvideo+bestaudio/best"

    loop = asyncio.get_event_loop()
    opts = _base_ydl_opts(url)
    opts.update({
        "format": fmt,
        "outtmpl": out_template,
        "merge_output_format": "mp4",
        "progress_hooks": [_make_progress_hook(loop, query.message)],
    })

    # Алдын ала алынған info болса — format ID қолмен таңдаймыз
    stored_info = context.user_data.get("dl_info") if context else None
    fmt_id = None
    if stored_info and _is_youtube(url):
        fmt_id = _pick_format_id(stored_info, height)
        opts["format"] = fmt_id
        n_formats = len(stored_info.get("formats", []))
        await query.edit_message_text(f"⏳ Format: {fmt_id} (жалпы: {n_formats}), жүктелуде...")

    try:
        video_path = None
        title = context.user_data.get("dl_title") or "video"

        # Алдымен yt-dlp арқылы жүктеп көреміз
        try:
            if stored_info and _is_youtube(url):
                info = await loop.run_in_executor(
                    None, lambda: _ydl_download_from_info(opts, stored_info)
                )
            else:
                info = await loop.run_in_executor(None, lambda: _ydl_download_with_retry(opts, url))
            title = context.user_data.get("dl_title") or info.get("title") or "video"

            mp4_files = list(DOWNLOAD_DIR.glob(f"video_{uid}.mp4"))
            found = mp4_files or list(DOWNLOAD_DIR.glob(f"video_{uid}.*"))
            if found:
                video_path = found[0]
        except Exception as dl_err:
            # TikTok: мобильді API блокталса (Railway) — браузер арқылы көшеміз
            if _is_tiktok(url):
                logger.warning(f"TikTok yt-dlp сәтсіз, Playwright қолданамыз: {dl_err}")
            else:
                raise

        # TikTok yt-dlp файл бермесе — Playwright резерві
        if video_path is None and _is_tiktok(url):
            await query.edit_message_text("⏳ TikTok браузер арқылы жүктелуде...")
            video_path, tt_title = await loop.run_in_executor(
                None, _tiktok_download_playwright, url, DOWNLOAD_DIR
            )
            title = tt_title or title

        if video_path is None:
            await query.edit_message_text("Видео файл жасалмады. Сілтемені тексеріңіз.")
            return

        # Telegram-ға үйлесімді H.264+AAC форматына конвертациялайды
        await query.edit_message_text("⚙️ Telegram үшін өңделуде...")
        loop = asyncio.get_event_loop()
        video_path = await loop.run_in_executor(
            None, lambda: _convert_for_telegram(video_path, uid)
        )

        size_mb = video_path.stat().st_size / (1024 * 1024)
        vw, vh = _get_video_dimensions(video_path)
        await query.edit_message_text(f"📤 Жіберілуде... ({size_mb:.0f} МБ)")
        if size_mb > 50 and API_ID and API_HASH:
            await _pyro_send_video(query.message.chat_id, video_path, title, query.message, vw, vh)
        else:
            with open(video_path, "rb") as f:
                await context.bot.send_video(
                    chat_id=query.message.chat_id,
                    video=f,
                    caption=f"🎬 {title[:200]}",
                    filename=f"{_safe_name(title)}.mp4",
                    supports_streaming=True,
                    width=vw or None,
                    height=vh or None,
                    read_timeout=600,
                    write_timeout=600,
                    connect_timeout=60,
                )
        await query.edit_message_text("✅ Видео жіберілді!")
        video_path.unlink(missing_ok=True)

    except Exception as e:
        logger.error(f"Video error: {e}", exc_info=True)
        for f in DOWNLOAD_DIR.glob(f"video_{uid}.*"):
            f.unlink(missing_ok=True)
        await query.edit_message_text(_format_error(str(e), url))
    finally:
        ACTIVE_USERS.discard(user_id)


# ---------------------------------------------------------------------------
# Видеоны кесу (трим)
# ---------------------------------------------------------------------------

def _parse_timestamp(s: str) -> int | None:
    """'1:20' / '1:02:03' / '90' → секунд."""
    s = s.strip()
    if not s:
        return None
    try:
        bits = [int(b) for b in s.split(":")]
    except ValueError:
        return None
    if len(bits) == 1:
        return bits[0]
    if len(bits) == 2:
        return bits[0] * 60 + bits[1]
    if len(bits) == 3:
        return bits[0] * 3600 + bits[1] * 60 + bits[2]
    return None


def _parse_time_range(text: str) -> tuple[int, int] | None:
    """'1:20-2:45' / '1:20 - 2:45' / '1:20 to 2:45' → (start_sec, end_sec)."""
    parts = re.split(r"\s*(?:-|–|—|to|до)\s*", text.strip())
    parts = [p for p in parts if p.strip()]
    if len(parts) != 2:
        return None
    s = _parse_timestamp(parts[0])
    e = _parse_timestamp(parts[1])
    if s is None or e is None:
        return None
    return s, e


def _ffmpeg_cut(src: Path, out: Path, start: int, end: int) -> None:
    """Видеоны start-end аралығымен кеседі (H.264/AAC)."""
    ffmpeg = Path(FFMPEG_DIR) / f"ffmpeg{_EXE}" if FFMPEG_DIR else Path("ffmpeg")
    subprocess.run([
        str(ffmpeg), "-y", "-ss", str(start), "-to", str(end), "-i", str(src),
        "-c:v", "libx264", "-c:a", "aac", "-movflags", "+faststart", str(out),
    ], capture_output=True, check=True)


async def _handle_trim_input(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    """Қолданушы жіберген уақыт аралығын өңдейді."""
    context.user_data["awaiting_trim"] = False
    url = context.user_data.get(USER_URL_KEY)
    if not url:
        await update.message.reply_text("Сілтеме табылмады. Видео сілтемесін қайта жіберіңіз.")
        return

    rng = _parse_time_range(text)
    if not rng:
        context.user_data["awaiting_trim"] = True
        await update.message.reply_text(
            "⚠️ Уақыт форматы дұрыс емес. Қайтадан жіберіңіз.\n"
            "Мысалы: 1:20-2:45"
        )
        return

    start, end = rng
    if end <= start:
        context.user_data["awaiting_trim"] = True
        await update.message.reply_text("⚠️ Соңғы уақыт басынан кейін болуы керек. Қайта жіберіңіз.")
        return

    await download_and_send_trimmed(update, context, url, start, end)


async def download_and_send_trimmed(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                    url: str, start: int, end: int) -> None:
    """Видеоның тек [start, end] аралығын жүктеп жібереді."""
    user_id = update.effective_user.id
    if user_id in ACTIVE_USERS:
        await update.message.reply_text("⏳ Алдыңғы жүктеу аяқталмады. Күте тұрыңыз.")
        return
    ACTIVE_USERS.add(user_id)

    chat_id = update.effective_chat.id
    msg = await update.message.reply_text(
        f"✂️ {_format_duration(start)}–{_format_duration(end)} аралығы жүктелуде..."
    )
    loop = asyncio.get_event_loop()
    uid = uuid.uuid4().hex[:8]

    try:
        # TikTok/Threads — толық жүктеп, ffmpeg-пен кесеміз (download_ranges қолдамайды)
        if _is_tiktok(url) or _is_threads(url):
            if _is_tiktok(url):
                full, title = await loop.run_in_executor(None, _tiktok_download, url, DOWNLOAD_DIR)
            else:
                full, title = await loop.run_in_executor(None, _threads_download, url, DOWNLOAD_DIR)
            out = DOWNLOAD_DIR / f"trim_{uid}.mp4"
            await msg.edit_text("✂️ Кесілуде...")
            await loop.run_in_executor(None, lambda: _ffmpeg_cut(Path(full), out, start, end))
            Path(full).unlink(missing_ok=True)
            video_path = out
        else:
            # yt-dlp — тек керекті бөлікті жүктейді (трафикті үнемдейді)
            from yt_dlp.utils import download_range_func
            opts = _base_ydl_opts(url)
            opts.update({
                "outtmpl": str(DOWNLOAD_DIR / f"trim_{uid}.%(ext)s"),
                "format": "best" if _is_youtube(url) else "bestvideo+bestaudio/best",
                "merge_output_format": "mp4",
                "download_ranges": download_range_func(None, [(start, end)]),
                "force_keyframes_at_cuts": True,
                "progress_hooks": [_make_progress_hook(loop, msg)],
            })
            info = await loop.run_in_executor(None, lambda: _ydl_download(opts, url))
            title = info.get("title") or "video"
            found = list(DOWNLOAD_DIR.glob(f"trim_{uid}.*"))
            if not found:
                await msg.edit_text("❌ Кесілген файл жасалмады. Сілтемені тексеріңіз.")
                return
            video_path = found[0]

        # Telegram үшін өңдеу
        await msg.edit_text("⚙️ Telegram үшін өңделуде...")
        video_path = await loop.run_in_executor(None, lambda: _convert_for_telegram(video_path, uid))
        size_mb = video_path.stat().st_size / (1024 * 1024)
        vw, vh = _get_video_dimensions(video_path)
        caption = f"✂️ {title[:170]}\n⏱ {_format_duration(start)}–{_format_duration(end)}"
        await msg.edit_text(f"📤 Жіберілуде... ({size_mb:.0f} МБ)")

        if size_mb > 50 and API_ID and API_HASH:
            await _pyro_send_video(chat_id, video_path, caption, msg, vw, vh)
        else:
            with open(video_path, "rb") as f:
                await context.bot.send_video(
                    chat_id=chat_id, video=f, caption=caption,
                    filename=f"{_safe_name(title)}_cut.mp4",
                    supports_streaming=True,
                    width=vw or None, height=vh or None,
                    read_timeout=600, write_timeout=600, connect_timeout=60,
                )
        await msg.delete()
        video_path.unlink(missing_ok=True)

    except Exception as e:
        logger.error(f"Trim error: {e}", exc_info=True)
        for f in DOWNLOAD_DIR.glob(f"trim_{uid}.*"):
            f.unlink(missing_ok=True)
        await msg.edit_text(f"❌ Кесу қатесі:\n{str(e)[:300]}")
    finally:
        ACTIVE_USERS.discard(user_id)


# ---------------------------------------------------------------------------
# Плейлист толық жүктеу
# ---------------------------------------------------------------------------

MAX_PLAYLIST = 25  # бір реттегі ең көп видео саны (шектен асудан қорғау)


async def download_and_send_playlist(query, context, url: str) -> None:
    """Плейлисттегі видеоларды кезекпен жүктеп жібереді."""
    user_id = query.from_user.id
    if user_id in ACTIVE_USERS:
        await query.edit_message_text("⏳ Алдыңғы жүктеу аяқталмады.")
        return
    ACTIVE_USERS.add(user_id)
    chat_id = query.message.chat_id
    loop = asyncio.get_event_loop()

    try:
        # Плейлист элементтерін жалпақ режимде алу (жылдам)
        await query.edit_message_text("📋 Плейлист оқылуда...")

        def _extract():
            import yt_dlp
            o = _base_ydl_opts(url)
            o["extract_flat"] = "in_playlist"
            o["skip_download"] = True
            with yt_dlp.YoutubeDL(o) as ydl:
                return ydl.extract_info(url, download=False)

        info = await loop.run_in_executor(None, _extract)
        entries = [e for e in (info.get("entries") or []) if e]
        if not entries:
            await query.edit_message_text("❌ Плейлисте видео табылмады.")
            return

        total = len(entries)
        limited = entries[:MAX_PLAYLIST]
        pl_title = info.get("title") or "Плейлист"
        note = ""
        if total > MAX_PLAYLIST:
            note = f"\n⚠️ Алғашқы {MAX_PLAYLIST} видео жүктеледі ({total} ішінен)."
        await query.edit_message_text(
            f"📋 {pl_title}\n{len(limited)} видео жүктеле бастады...{note}"
        )

        ok_count = 0
        for idx, e in enumerate(limited, 1):
            vurl = e.get("url") or e.get("webpage_url") or e.get("id")
            if not vurl:
                continue
            # YouTube id болса толық URL жасаймыз
            if not str(vurl).startswith("http"):
                vurl = f"https://www.youtube.com/watch?v={e.get('id', vurl)}"

            uid = uuid.uuid4().hex[:8]
            status = await context.bot.send_message(
                chat_id, f"⬇️ {idx}/{len(limited)} жүктелуде..."
            )
            try:
                opts = _base_ydl_opts(vurl)
                opts.update({
                    "outtmpl": str(DOWNLOAD_DIR / f"pl_{uid}.%(ext)s"),
                    "format": "best" if _is_youtube(vurl) else "bestvideo+bestaudio/best",
                    "merge_output_format": "mp4",
                    "noplaylist": True,
                })
                vinfo = await loop.run_in_executor(None, lambda o=opts, u=vurl: _ydl_download(o, u))
                vtitle = vinfo.get("title") or f"video_{idx}"
                found = list(DOWNLOAD_DIR.glob(f"pl_{uid}.*"))
                if not found:
                    await status.edit_text(f"⚠️ {idx}/{len(limited)}: жүктелмеді")
                    continue
                vpath = found[0]
                vpath = await loop.run_in_executor(None, lambda p=vpath, u=uid: _convert_for_telegram(p, u))
                size_mb = vpath.stat().st_size / (1024 * 1024)
                vw, vh = _get_video_dimensions(vpath)
                await status.edit_text(f"📤 {idx}/{len(limited)} жіберілуде...")
                if size_mb > 50 and API_ID and API_HASH:
                    await _pyro_send_video(chat_id, vpath, vtitle, status, vw, vh)
                else:
                    with open(vpath, "rb") as f:
                        await context.bot.send_video(
                            chat_id=chat_id, video=f,
                            caption=f"🎬 {idx}/{len(limited)}. {vtitle[:180]}",
                            filename=f"{_safe_name(vtitle)}.mp4",
                            supports_streaming=True,
                            width=vw or None, height=vh or None,
                            read_timeout=600, write_timeout=600, connect_timeout=60,
                        )
                await status.delete()
                vpath.unlink(missing_ok=True)
                ok_count += 1
            except Exception as e2:
                logger.error(f"Playlist item {idx} error: {e2}", exc_info=True)
                try:
                    await status.edit_text(f"⚠️ {idx}/{len(limited)}: қате — {str(e2)[:80]}")
                except Exception:
                    pass
                for f in DOWNLOAD_DIR.glob(f"pl_{uid}.*"):
                    f.unlink(missing_ok=True)

        await context.bot.send_message(
            chat_id, f"✅ Дайын! {ok_count}/{len(limited)} видео жіберілді."
        )

    except Exception as e:
        logger.error(f"Playlist error: {e}", exc_info=True)
        await query.edit_message_text(f"❌ Плейлист қатесі:\n{str(e)[:300]}")
    finally:
        ACTIVE_USERS.discard(user_id)


# ---------------------------------------------------------------------------
# Көмекші функциялар
# ---------------------------------------------------------------------------

COOKIES_INSTRUCTION = (
    "📋 Не істеу керек:\n"
    "1. Chrome-ға «Get cookies.txt LOCALLY» extension орнатыңыз\n"
    "2. Сол сайтқа кіріңіз (Instagram / TikTok / Facebook / Threads)\n"
    "3. Extension-ды ашып → Export → cookies.txt сақтаңыз\n"
    f"4. Файлды осы жерге қойыңыз:\n"
    f"   {Path('cookies.txt').resolve()}\n"
    "5. Ботты қайта іске қосыңыз"
)


def _format_error(error: str, url: str = "") -> str:
    """Қатені пайдаланушыға түсінікті хабарламаға айналдырады."""
    err = error.lower()
    url_lower = url.lower()

    is_youtube = any(d in url_lower for d in ("youtube.com", "youtu.be"))
    is_tiktok = "tiktok.com" in url_lower
    is_vk = any(d in url_lower for d in ("vk.com", "vk.ru", "vkvideo.ru"))
    is_threads = any(d in url_lower for d in ("threads.net", "threads.com"))
    is_instagram = "instagram.com" in url_lower
    no_cookies = not COOKIES_FILE.exists()

    # YouTube датацентр IP блогы
    if is_youtube and ("sign in" in err or "bot" in err or "confirm your age" in err
                       or ("login" in err and "cookies" not in err)):
        return "❌ YouTube серверден жүктеуді блоктады. Сілтемені қайта жіберіп көріңіз."

    if is_threads and no_cookies:
        return "🔐 Threads жүктеу үшін cookies керек.\n\n" + COOKIES_INSTRUCTION

    # Instagram — cookie қажет
    if is_instagram and (no_cookies or "login" in err or "invalid_post" in err):
        if no_cookies:
            return (
                "🔐 Instagram жүктеу үшін аккаунтқа кіру керек.\n\n"
                + COOKIES_INSTRUCTION
            )
        return "❌ Instagram бұл постты жүктеуге рұқсат бермеді."

    if is_tiktok and ("blocked" in err or "ip address" in err):
        if no_cookies:
            return (
                "🔐 TikTok IP блоктады — cookie керек.\n\n"
                + COOKIES_INSTRUCTION
            )
        return "❌ TikTok бұл контентті жүктеуге рұқсат бермеді."
    if is_vk and no_cookies and ("login" in err or "auth" in err or "private" in err):
        return (
            "🔐 Бұл VK видеосы кіруді қажет етеді.\n\n"
            + COOKIES_INSTRUCTION
        )
    if "registered users" in err or "authentication" in err or ("login" in err and not is_youtube) or "cookies" in err:
        return (
            "🔐 Бұл контент кіруді қажет етеді.\n\n"
            + COOKIES_INSTRUCTION
        )
    if "invalid_post" in err:
        return (
            "🔐 Бұл контент кіруді қажет етеді.\n\n"
            + COOKIES_INSTRUCTION
        )
    if "unsupported url" in err:
        return "❌ Бұл сайт қолдамайды немесе сілтеме дұрыс емес."
    if "copyright" in err or "removed at the request" in err:
        return "⛔ Бұл видео авторлық құқық иесінің сұрауы бойынша жойылған. Жүктеу мүмкін емес."
    if "private" in err or "only available" in err:
        return "🔒 Бұл жабық (private) контент. Жүктеу мүмкін емес."
    if "not available" in err or "no video formats" in err:
        return f"❌ Debug:\n{error[:500]}"
    return f"❌ Қате болды:\n{error[:300]}"


async def _pyro_send_video(chat_id: int, path: Path, title: str, progress_msg,
                           width: int = 0, height: int = 0) -> None:
    """Pyrogram арқылы кез келген өлшемдегі видеоны жібереді (2 ГБ-қа дейін)."""
    async with Client(
        "bot_session",
        api_id=int(API_ID),
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
    ) as app:
        last = [0]
        async def progress(current, total):
            pct = int(current * 100 / total)
            if pct - last[0] >= 10:
                last[0] = pct
                try:
                    await progress_msg.edit_text(f"📤 Жіберілуде... {pct}%")
                except Exception:
                    pass

        kwargs = dict(
            chat_id=chat_id,
            video=str(path),
            caption=f"🎬 {title[:1000]}",
            supports_streaming=True,
            progress=progress,
        )
        if width and height:
            kwargs["width"] = width
            kwargs["height"] = height
        await app.send_video(**kwargs)


async def _pyro_send_audio(chat_id: int, path: Path, title: str, progress_msg) -> None:
    """Pyrogram арқылы кез келген өлшемдегі аудионы жібереді."""
    async with Client(
        "bot_session",
        api_id=int(API_ID),
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
    ) as app:
        last = [0]
        async def progress(current, total):
            pct = int(current * 100 / total)
            if pct - last[0] >= 10:
                last[0] = pct
                try:
                    await progress_msg.edit_text(f"📤 Жіберілуде... {pct}%")
                except Exception:
                    pass

        await app.send_audio(
            chat_id=chat_id,
            audio=str(path),
            title=title[:64],
            progress=progress,
        )


def _get_video_dimensions(path: Path) -> tuple[int, int]:
    """FFprobe арқылы видео ені мен биіктігін қайтарады."""
    ffprobe = Path(FFMPEG_DIR) / f"ffprobe{_EXE}" if FFMPEG_DIR else "ffprobe"
    r = subprocess.run(
        [str(ffprobe), "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=width,height",
         "-of", "csv=p=0", str(path)],
        capture_output=True, text=True
    )
    try:
        w, h = r.stdout.strip().split(",")
        return int(w), int(h)
    except Exception:
        return 0, 0


def _convert_for_telegram(src: Path, uid: str) -> Path:
    """
    Telegram үшін H.264+AAC MP4-ке дайындайды.
    H.264 болса — faststart ғана (секундтар). Басқа кодек болса — ultrafast encode.
    """
    ffmpeg = Path(FFMPEG_DIR) / f"ffmpeg{_EXE}" if FFMPEG_DIR else "ffmpeg"
    ffprobe = Path(FFMPEG_DIR) / f"ffprobe{_EXE}" if FFMPEG_DIR else "ffprobe"

    # Видео және аудио кодектерін анықтайды
    probe = subprocess.run(
        [str(ffprobe), "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=codec_name",
         "-of", "default=noprint_wrappers=1:nokey=1", str(src)],
        capture_output=True, text=True
    )
    vcodec = probe.stdout.strip().lower()

    probe_a = subprocess.run(
        [str(ffprobe), "-v", "error", "-select_streams", "a:0",
         "-show_entries", "stream=codec_name",
         "-of", "default=noprint_wrappers=1:nokey=1", str(src)],
        capture_output=True, text=True
    )
    acodec = probe_a.stdout.strip().lower()

    is_h264 = vcodec in ("h264", "avc", "avc1")
    is_aac  = acodec in ("aac", "mp4a")

    out = DOWNLOAD_DIR / f"video_{uid}_tg.mp4"

    if is_h264 and is_aac:
        # Ең жылдам — тек контейнер + faststart, кодтамайды
        cmd = [
            str(ffmpeg), "-y", "-i", str(src),
            "-c", "copy", "-movflags", "+faststart",
            str(out)
        ]
    elif is_h264:
        # Видео дайын, тек аудио AAC-ке
        cmd = [
            str(ffmpeg), "-y", "-i", str(src),
            "-c:v", "copy", "-c:a", "aac", "-b:a", "192k",
            "-movflags", "+faststart",
            str(out)
        ]
    else:
        # Толық encode — ultrafast ең жылдам нұсқа
        cmd = [
            str(ffmpeg), "-y", "-i", str(src),
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
            "-c:a", "aac", "-b:a", "192k",
            "-movflags", "+faststart",
            str(out)
        ]

    result = subprocess.run(cmd, capture_output=True)
    if result.returncode == 0 and out.exists():
        src.unlink(missing_ok=True)
        return out
    # Конвертация сәтсіз болса — бастапқы файлды қайтарады
    out.unlink(missing_ok=True)
    return src


def _safe_name(title: str) -> str:
    """Файл аты үшін арнайы символдарды алып тастайды."""
    return re.sub(r'[\\/*?:"<>|]', "", title)[:50] or "file"


def _ydl_download(opts: dict, url: str) -> dict:
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=True)


def _ydl_download_from_info(opts: dict, info: dict) -> dict:
    """Format URL-ін тікелей жүктейді — YouTube-ке қайта сұраныс жібермейді."""
    import urllib.request

    formats = info.get("formats", [])
    fmt_id = opts.get("format", "best")
    outtmpl = opts.get("outtmpl", "downloads/video.%(ext)s")
    ffmpeg = Path(FFMPEG_DIR) / "ffmpeg" if FFMPEG_DIR else Path("ffmpeg")

    def pick(fmts, fid):
        for f in fmts:
            if f.get("format_id") == fid:
                return f
        return None

    headers = {
        "User-Agent": "com.google.ios.youtube/19.29.1 (iPhone16,2; U; CPU iOS 17_5_1 like Mac OS X;)",
        "Accept-Language": "en-US,en;q=0.9",
    }

    if "+" in fmt_id:
        vid_id, aud_id = fmt_id.split("+", 1)
        vf = pick(formats, vid_id)
        af = pick(formats, aud_id)
        if vf and af and vf.get("url") and af.get("url"):
            ext = vf.get("ext", "mp4")
            base = outtmpl.replace("%(ext)s", ext).replace(".mp4", "")
            vtmp = base + "_v.mp4"
            atmp = base + "_a.m4a"
            req_v = urllib.request.Request(vf["url"], headers=headers)
            req_a = urllib.request.Request(af["url"], headers=headers)
            with urllib.request.urlopen(req_v, timeout=300) as r:
                Path(vtmp).write_bytes(r.read())
            with urllib.request.urlopen(req_a, timeout=300) as r:
                Path(atmp).write_bytes(r.read())
            out = outtmpl.replace("%(ext)s", "mp4")
            subprocess.run([str(ffmpeg), "-y", "-i", vtmp, "-i", atmp,
                            "-c", "copy", out], capture_output=True, check=True)
            Path(vtmp).unlink(missing_ok=True)
            Path(atmp).unlink(missing_ok=True)
            return info
    else:
        cf = pick(formats, fmt_id)
        if cf and cf.get("url"):
            ext = cf.get("ext", "mp4")
            out = outtmpl.replace("%(ext)s", ext)
            req = urllib.request.Request(cf["url"], headers=headers)
            with urllib.request.urlopen(req, timeout=300) as r:
                Path(out).write_bytes(r.read())
            return info

    # Fallback: yt-dlp арқылы жүктейді
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.process_ie_result(dict(info), download=True)


def _pick_format_id(info: dict, height: int | None) -> str:
    """Formats тізімінен ең жақсы format ID-ін қолмен таңдайды."""
    formats = info.get("formats", [])
    if not formats:
        return "best"

    # Видео + аудио бірге (combined)
    combined = [f for f in formats
                if f.get("vcodec", "none") != "none"
                and f.get("acodec", "none") != "none"
                and f.get("format_id")]
    if combined:
        pool = [f for f in combined if not height or (f.get("height") or 0) <= height] or combined
        best = max(pool, key=lambda f: f.get("height") or 0)
        return best["format_id"]

    # Бөлек видео + аудио
    vids = [f for f in formats if f.get("vcodec", "none") != "none"
            and f.get("acodec", "none") == "none" and f.get("format_id")]
    auds = [f for f in formats if f.get("vcodec", "none") == "none"
            and f.get("acodec", "none") != "none" and f.get("format_id")]
    if vids and auds:
        vpool = [f for f in vids if not height or (f.get("height") or 0) <= height] or vids
        best_v = max(vpool, key=lambda f: f.get("height") or 0)
        best_a = max(auds, key=lambda f: f.get("abr") or 0)
        return f"{best_v['format_id']}+{best_a['format_id']}"

    return formats[-1]["format_id"] if formats else "best"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN .env файлында жоқ!")
    if not FFMPEG_DIR:
        logger.warning("FFmpeg табылмады! Аудио/видео merge жұмыс істемеуі мүмкін.")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .read_timeout(300)
        .write_timeout(300)
        .connect_timeout(60)
        .pool_timeout(300)
        .build()
    )
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("debug", cmd_debug))
    app.add_handler(CommandHandler("setcookies", cmd_setcookies))
    app.add_handler(CommandHandler("getcookies", cmd_getcookies))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_type_choice, pattern=r"^type:"))
    app.add_handler(CallbackQueryHandler(handle_quality_choice, pattern=r"^quality:"))
    app.add_handler(CallbackQueryHandler(handle_tiktok_quality, pattern=r"^ttq:"))

    logger.info("Video Downloader Bot іске қосылды...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
