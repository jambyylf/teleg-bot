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

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID    = os.getenv("API_ID")
API_HASH  = os.getenv("API_HASH")
DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

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

# cookies.txt файлының жолы (Netscape форматы)
COOKIES_FILE = Path("cookies.txt")

USER_URL_KEY = "dl_url"


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
        opts["extractor_args"] = {"youtube": {"player_client": ["tv_embedded"]}}
    if _is_tiktok(url):
        # Браузер TLS fingerprint-ін имитациялайды — IP блокын айналып өтеді
        opts["impersonate"] = ImpersonateTarget("chrome")
    if COOKIES_FILE.exists():
        opts["cookiefile"] = str(COOKIES_FILE)
        logger.info("cookies.txt пайдаланылады")
    return opts


# ---------------------------------------------------------------------------
# Видео форматтарын анықтайды
# ---------------------------------------------------------------------------

def get_video_info(url: str) -> dict:
    opts = _base_ydl_opts(url)
    opts["skip_download"] = True
    with yt_dlp.YoutubeDL(opts) as ydl:
        return ydl.extract_info(url, download=False)


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
    context.user_data[USER_URL_KEY] = url

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
        info = await loop.run_in_executor(None, lambda: _ydl_download(opts, url))
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

    # H.264 кодекін басымдықта алады — Telegram-да дұрыс ойнайды
    if height:
        fmt = (
            f"bestvideo[height<={height}][vcodec^=avc]+bestaudio/bestaudio"
            f"/bestvideo[height<={height}]+bestaudio"
            f"/best[height<={height}]"
        )
    else:
        fmt = "bestvideo[vcodec^=avc]+bestaudio/bestaudio/bestvideo+bestaudio/best"

    opts = _base_ydl_opts(url)
    opts.update({
        "format": fmt,
        "outtmpl": out_template,
        "merge_output_format": "mp4",
    })

    try:
        loop = asyncio.get_event_loop()
        info = await loop.run_in_executor(None, lambda: _ydl_download(opts, url))
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
        await query.edit_message_text(f"📤 Жіберілуде... ({size_mb:.0f} МБ)")
        if size_mb > 50 and API_ID and API_HASH:
            await _pyro_send_video(query.message.chat_id, video_path, title, query.message)
        else:
            with open(video_path, "rb") as f:
                await context.bot.send_video(
                    chat_id=query.message.chat_id,
                    video=f,
                    caption=f"🎬 {title[:200]}",
                    filename=f"{_safe_name(title)}.mp4",
                    supports_streaming=True,
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

    is_threads = "threads" in url_lower
    is_tiktok = "tiktok.com" in url_lower
    is_vk = any(d in url_lower for d in ("vk.com", "vk.ru", "vkvideo.ru"))
    no_cookies = not COOKIES_FILE.exists()

    if is_tiktok and ("blocked" in err or "ip address" in err):
        if no_cookies:
            return (
                "🔐 TikTok IP блоктады — cookie керек.\n\n"
                + COOKIES_INSTRUCTION
            )
        return "❌ TikTok бұл контентті жүктеуге рұқсат бермеді."
    if is_threads and no_cookies:
        return (
            "🔐 Threads cookies.txt-сіз жұмыс істемейді.\n\n"
            + COOKIES_INSTRUCTION
        )
    if is_vk and no_cookies and ("login" in err or "auth" in err or "private" in err):
        return (
            "🔐 Бұл VK видеосы кіруді қажет етеді.\n\n"
            + COOKIES_INSTRUCTION
        )
    if "registered users" in err or "authentication" in err or "login" in err or "cookies" in err:
        return (
            "🔐 Бұл контент кіруді қажет етеді.\n\n"
            + COOKIES_INSTRUCTION
        )
    if "unsupported url" in err or "invalid_post" in err:
        if is_threads:
            return (
                "❌ Threads сілтемесі жарамсыз немесе кіру қажет.\n\n"
                + COOKIES_INSTRUCTION
            )
        return "❌ Бұл сайт қолдамайды немесе сілтеме дұрыс емес."
    if "copyright" in err or "removed at the request" in err:
        return "⛔ Бұл видео авторлық құқық иесінің сұрауы бойынша жойылған. Жүктеу мүмкін емес."
    if "private" in err or "only available" in err:
        return "🔒 Бұл жабық (private) контент. Жүктеу мүмкін емес."
    if "not available" in err or "no video formats" in err:
        return "❌ Бұл сілтемеде жүктелетін видео табылмады."
    return f"❌ Қате болды:\n{error[:300]}"


async def _pyro_send_video(chat_id: int, path: Path, title: str, progress_msg) -> None:
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

        await app.send_video(
            chat_id=chat_id,
            video=str(path),
            caption=f"🎬 {title[:1000]}",
            supports_streaming=True,
            progress=progress,
        )


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


def _convert_for_telegram(src: Path, uid: str) -> Path:
    """
    Видеоны Telegram-ға үйлесімді H.264+AAC MP4-ке конвертациялайды.
    Егер бұрын H.264 болса — тек faststart қосады (жылдам).
    """
    ffmpeg = Path(FFMPEG_DIR) / "ffmpeg.exe" if FFMPEG_DIR else "ffmpeg"
    ffprobe = Path(FFMPEG_DIR) / "ffprobe.exe" if FFMPEG_DIR else "ffprobe"

    # Кодекті анықтайды
    probe = subprocess.run(
        [str(ffprobe), "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=codec_name",
         "-of", "default=noprint_wrappers=1:nokey=1", str(src)],
        capture_output=True, text=True
    )
    vcodec = probe.stdout.strip().lower()

    out = DOWNLOAD_DIR / f"video_{uid}_tg.mp4"

    if vcodec in ("h264", "avc"):
        # Тек контейнер + faststart — жылдам, сапа жоғалмайды
        cmd = [
            str(ffmpeg), "-y", "-i", str(src),
            "-c", "copy",
            "-movflags", "+faststart",
            str(out)
        ]
    else:
        # H.264-ке қайта кодтайды (AV1, VP9, HEVC т.б.)
        cmd = [
            str(ffmpeg), "-y", "-i", str(src),
            "-c:v", "libx264", "-preset", "fast", "-crf", "22",
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

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(handle_type_choice, pattern=r"^type:"))
    app.add_handler(CallbackQueryHandler(handle_quality_choice, pattern=r"^quality:"))

    logger.info("Video Downloader Bot іске қосылды...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
