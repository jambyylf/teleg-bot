import asyncio
import logging
import os
import re
import shutil
import subprocess
import uuid
from pathlib import Path

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
from yt_dlp.networking.impersonate import ImpersonateTarget
from hydrogram import Client

import base64

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID    = os.getenv("API_ID")
API_HASH  = os.getenv("API_HASH")
DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

# Railway env var арқылы cookies жүктеу (base64 форматында)
COOKIES_FILE = Path("cookies.txt")
_cookies_b64 = os.getenv("COOKIES_CONTENT")
if _cookies_b64 and not COOKIES_FILE.exists():
    try:
        COOKIES_FILE.write_bytes(base64.b64decode(_cookies_b64))
    except Exception:
        pass

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
    # threads.com → threads.net (yt-dlp тек threads.net қолдайды)
    url = url.replace("www.threads.com", "www.threads.net").replace("threads.com", "threads.net")
    parsed = urlparse(url)
    if any(d in parsed.netloc for d in CLEAN_URL_DOMAINS):
        return urlunparse(parsed._replace(query="", fragment=""))
    return url


# ---------------------------------------------------------------------------
# FFmpeg жолы
# ---------------------------------------------------------------------------

def _find_ffmpeg() -> str:
    exe = shutil.which("ffmpeg")
    if exe:
        return str(Path(exe).parent)
    winget = Path.home() / "AppData/Local/Microsoft/WinGet/Packages"
    for p in sorted(winget.glob("Gyan.FFmpeg_*/ffmpeg-*/bin"), reverse=True):
        if (p / "ffmpeg.exe").exists():
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


def _base_ydl_opts(url: str = "") -> dict:
    opts: dict = {
        "ffmpeg_location": FFMPEG_DIR,
        "quiet": True,
        "no_warnings": True,
        "nocheckcertificate": True,
        "legacyserverconnect": True,
    }
    if _is_youtube(url):
        if COOKIES_FILE.exists():
            # Cookies бар болса — web client жақсы жұмыс істейді
            opts["extractor_args"] = {
                "youtube": {"player_client": ["web", "tv_embedded"]}
            }
        else:
            opts["extractor_args"] = {
                "youtube": {"player_client": ["android_vr", "tv_embedded", "android", "ios"]}
            }
        opts["socket_timeout"] = 30
    if _is_tiktok(url):
        opts["impersonate"] = ImpersonateTarget("chrome")
        opts["extractor_args"] = {
            "tiktok": {"api_hostname": "api16-normal-c-useast1a.tiktokv.com"}
        }
    if COOKIES_FILE.exists():
        opts["cookiefile"] = str(COOKIES_FILE)
    return opts


def _ydl_download_with_retry(opts: dict, url: str) -> dict:
    """Алдымен қалыпты жүктейді, сәтсіз болса баламалы параметрлермен қайталайды."""
    try:
        return _ydl_download(opts, url)
    except Exception as first_err:
        err_str = str(first_err).lower()
        # YouTube блогы — басқа client-пен retry
        if _is_youtube(url) and any(k in err_str for k in ("sign in", "login", "bot", "confirm",
                                                             "not available", "format")):
            clients = [["web"], ["tv_embedded"], ["android"], ["ios"]] if COOKIES_FILE.exists() \
                      else [["android_vr"], ["android"], ["ios"], ["mweb"]]
            for client in clients:
                retry_opts = dict(opts)
                retry_opts["extractor_args"] = {"youtube": {"player_client": client}}
                try:
                    return _ydl_download(retry_opts, url)
                except Exception:
                    continue
        # Auth/block қатесі болса — cookie немесе басқа параметрлермен retry
        if any(k in err_str for k in ("blocked", "login", "authentication",
                                       "registered", "cookies", "private")):
            retry_opts = dict(opts)
            # TikTok үшін басқа API hostname-мен қайталайды
            if "tiktok" in url.lower():
                for hostname in [
                    "api19-normal-c-useast1a.tiktokv.com",
                    "api22-normal-c-useast1a.tiktokv.com",
                ]:
                    retry_opts["extractor_args"] = {
                        "tiktok": {"api_hostname": hostname}
                    }
                    try:
                        return _ydl_download(retry_opts, url)
                    except Exception:
                        continue
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
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)
    except Exception as e:
        err = str(e).lower()
        # YouTube датацентр блогы — басқа client-пен retry
        if _is_youtube(url) and any(k in err for k in ("sign in", "login", "bot", "confirm",
                                                         "not available", "format")):
            clients = [["web"], ["tv_embedded"], ["android"], ["ios"]] if COOKIES_FILE.exists() \
                      else [["android_vr"], ["android"], ["ios"], ["mweb"]]
            for client in clients:
                retry = dict(opts)
                retry["extractor_args"] = {"youtube": {"player_client": client}}
                try:
                    with yt_dlp.YoutubeDL(retry) as ydl:
                        return ydl.extract_info(url, download=False)
                except Exception:
                    continue
        if "tiktok" in url.lower() and any(k in err for k in ("blocked", "login")):
            for hostname in [
                "api19-normal-c-useast1a.tiktokv.com",
                "api22-normal-c-useast1a.tiktokv.com",
            ]:
                retry = dict(opts)
                retry["extractor_args"] = {"tiktok": {"api_hostname": hostname}}
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
    match = URL_REGEX.search(text)
    if not match:
        await update.message.reply_text(
            "Сілтеме табылмады. http:// немесе https:// басталатын сілтеме жіберіңіз."
        )
        return

    url = match.group(0)
    context.user_data[USER_URL_KEY] = _clean_url(url)

    keyboard = [[
        InlineKeyboardButton("🎵 Аудио (MP3)", callback_data="type:audio"),
        InlineKeyboardButton("🎬 Видео (MP4)", callback_data="type:video"),
    ]]
    await update.message.reply_text(
        "Не жүктегіңіз келеді?",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


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

    elif choice == "video":
        await query.edit_message_text("⏳ Сапалар анықталуда...")
        try:
            loop = asyncio.get_event_loop()
            info = await loop.run_in_executor(None, get_video_info, url)
            qualities = get_available_video_qualities(info)
            title = info.get("title") or ""
            context.user_data["dl_title"] = title

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


# ---------------------------------------------------------------------------
# Жүктеу функциялары
# ---------------------------------------------------------------------------

async def download_and_send_audio(query, context, url: str) -> None:
    uid = uuid.uuid4().hex[:8]
    out_template = str(DOWNLOAD_DIR / f"audio_{uid}.%(ext)s")

    opts = _base_ydl_opts(url)
    opts.update({
        "format": "bestaudio/best",
        "outtmpl": out_template,
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": "mp3",
            "preferredquality": "320",
        }],
    })

    try:
        loop = asyncio.get_event_loop()
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
        logger.error(f"Audio error: {e}", exc_info=True)
        for f in DOWNLOAD_DIR.glob(f"audio_{uid}.*"):
            f.unlink(missing_ok=True)
        await query.edit_message_text(_format_error(str(e), url))


async def download_and_send_video(query, context, url: str, height: int | None) -> None:
    uid = uuid.uuid4().hex[:8]
    out_template = str(DOWNLOAD_DIR / f"video_{uid}.%(ext)s")

    # Форматты таңдайды — алдымен AVC, содан кейін кез келген
    if height:
        fmt = (
            f"bestvideo[height<={height}][vcodec^=avc1]+bestaudio[ext=m4a]/"
            f"bestvideo[height<={height}][vcodec^=avc]+bestaudio/"
            f"bestvideo[height<={height}]+bestaudio/"
            f"best[height<={height}]/best"
        )
    else:
        fmt = (
            "bestvideo[vcodec^=avc1]+bestaudio[ext=m4a]/"
            "bestvideo[vcodec^=avc]+bestaudio/"
            "bestvideo+bestaudio/best"
        )

    opts = _base_ydl_opts(url)
    opts.update({
        "format": fmt,
        "outtmpl": out_template,
        "merge_output_format": "mp4",
    })

    try:
        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(None, lambda: _ydl_download_with_retry(opts, url))
        title = context.user_data.get("dl_title") or info.get("title") or "video"

        mp4_files = list(DOWNLOAD_DIR.glob(f"video_{uid}.mp4"))
        found = mp4_files or list(DOWNLOAD_DIR.glob(f"video_{uid}.*"))
        if not found:
            await query.edit_message_text("Видео файл жасалмады. Сілтемені тексеріңіз.")
            return
        video_path = found[0]

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
    no_cookies = not COOKIES_FILE.exists()

    # YouTube датацентр IP блогы — cookies емес, серверден жүктеу мәселесі
    if is_youtube and ("sign in" in err or "bot" in err or "confirm your age" in err
                       or ("login" in err and "cookies" not in err)):
        return "❌ YouTube серверден жүктеуді блоктады. Сілтемені қайта жіберіп көріңіз."
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
    if "unsupported url" in err or "invalid_post" in err:
        return "❌ Бұл сайт қолдамайды немесе сілтеме дұрыс емес."
    if "copyright" in err or "removed at the request" in err:
        return "⛔ Бұл видео авторлық құқық иесінің сұрауы бойынша жойылған. Жүктеу мүмкін емес."
    if "private" in err or "only available" in err:
        return "🔒 Бұл жабық (private) контент. Жүктеу мүмкін емес."
    if "not available" in err or "no video formats" in err:
        return "❌ Бұл сілтемеде жүктелетін видео табылмады."
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
    ffprobe = Path(FFMPEG_DIR) / "ffprobe.exe" if FFMPEG_DIR else "ffprobe"
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
    ffmpeg = Path(FFMPEG_DIR) / "ffmpeg.exe" if FFMPEG_DIR else "ffmpeg"
    ffprobe = Path(FFMPEG_DIR) / "ffprobe.exe" if FFMPEG_DIR else "ffprobe"

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
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_type_choice, pattern=r"^type:"))
    app.add_handler(CallbackQueryHandler(handle_quality_choice, pattern=r"^quality:"))

    logger.info("Video Downloader Bot іске қосылды...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
