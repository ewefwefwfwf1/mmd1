import asyncio
import json
import os
import html
import hashlib
import secrets
import uuid
import time
import re
import base64
import sqlite3
import socket
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
from collections import deque, defaultdict

from fastapi import FastAPI, Request, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.responses import Response, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import uvicorn
import httpx
import logging
import psutil

try:
    import telebot
    from telebot.async_telebot import AsyncTeleBot
    from telebot import types
    TELEBOT_AVAILABLE = True
except ImportError:
    TELEBOT_AVAILABLE = False
    print("WARNING: Please install pyTelegramBotAPI to enable the Telegram Bot: pip install pyTelegramBotAPI")

log_queue = deque(maxlen=150)

class QueueHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            log_queue.append(msg)
        except Exception:
            pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("mmd-Gateway")

q_handler = QueueHandler()
q_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(q_handler)
logging.getLogger("uvicorn.error").addHandler(q_handler)
logging.getLogger("uvicorn.access").addHandler(q_handler)

app = FastAPI(title="エムエムディー Panel", docs_url=None, redoc_url=None)

# Bump this on every release so the dashboard can notify already-open sessions
# that a new version is available / was just applied.
PANEL_VERSION = "1.1.0"

# GitHub repo checked for update notifications
GITHUB_REPO = "luffy-sh-op/mmd_PANEL"

async def check_github_latest(force: bool = False) -> dict:
    """Fetches the latest release tag from GitHub, caches in SQLite.
    Only actually calls the API if force=True or no cached data exists."""
    conn = get_db()
    try:
        cur = conn.execute("SELECT latest_tag, latest_url, checked_at FROM github_cache WHERE id = 1")
        row = cur.fetchone()
    finally:
        conn.close()

    now = time.time()
    cached_tag = row["latest_tag"] if row else None
    cached_url = row["latest_url"] if row else None
    cached_at = row["checked_at"] if row else 0

    if not force and cached_tag and (now - cached_at) < 60:
        return {"tag": cached_tag, "url": cached_url, "checked_at": cached_at}

    global http_client
    if http_client is None:
        return {"tag": cached_tag, "url": cached_url, "checked_at": cached_at}

    new_tag = cached_tag
    new_url = cached_url
    try:
        r = await http_client.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest",
            headers={"Accept": "application/vnd.github+json"},
        )
        if r.status_code == 200:
            data = r.json()
            new_tag = data.get("tag_name") or data.get("name")
            new_url = data.get("html_url")
        else:
            r2 = await http_client.get(f"https://api.github.com/repos/{GITHUB_REPO}/commits/main")
            if r2.status_code == 200:
                data2 = r2.json()
                sha = data2.get("sha") or ""
                new_tag = sha[:7] if sha else cached_tag
                new_url = f"https://github.com/{GITHUB_REPO}/commit/{sha}" if sha else cached_url
    except Exception as e:
        logger.warning(f"GitHub version check failed: {e}")

    conn = get_db()
    try:
        conn.execute("INSERT OR REPLACE INTO github_cache (id, latest_tag, latest_url, checked_at) VALUES (1, ?, ?, ?)",
                     (new_tag, new_url, now))
        conn.commit()
    finally:
        conn.close()

    # Create notification if a new version is detected
    if new_tag and new_tag != cached_tag and cached_tag:
        await create_notification(
            type="update",
            title=f"New version: {new_tag}",
            message=f"Panel version {cached_tag} → {new_tag} is available on GitHub.",
            link=new_url,
        )

    return {"tag": new_tag, "url": new_url, "checked_at": now}


async def github_check_loop():
    """Background task: check GitHub every 60 seconds for new releases."""
    await asyncio.sleep(10)  # initial delay
    while True:
        try:
            await check_github_latest(force=True)
        except Exception as e:
            logger.warning(f"GitHub periodic check error: {e}")
        await asyncio.sleep(60)


# ── Notifications ────────────────────────────────────────────────────────

async def create_notification(type: str, title: str, message: str, link: str | None = None):
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO notifications (type, title, message, link, created_at) VALUES (?, ?, ?, ?, ?)",
            (type, title, message, link, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Error creating notification: {e}")
    finally:
        conn.close()

async def get_unread_notification_count() -> int:
    conn = get_db()
    try:
        cur = conn.execute("SELECT COUNT(*) as cnt FROM notifications WHERE seen = 0")
        row = cur.fetchone()
        return row["cnt"] if row else 0
    finally:
        conn.close()

async def get_notifications(limit: int = 50) -> list:
    conn = get_db()
    try:
        cur = conn.execute(
            "SELECT id, type, title, message, link, seen, created_at FROM notifications ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

def _get_or_create_secret() -> str:
    """Returns a stable secret key across restarts.

    Previously this fell back to secrets.token_urlsafe(32) on every process
    start when SECRET_KEY wasn't set, which changed the key each restart.
    Since password hashes are salted with this secret, that made the stored
    admin password hash (and every changed password) unverifiable after any
    restart, effectively locking everyone out. We now persist a generated
    secret to a local file so it stays constant across restarts.
    """
    env_secret = os.environ.get("SECRET_KEY")
    if env_secret:
        return env_secret
    secret_file = "/data/secret.key" if os.path.isdir("/data") else "secret.key"
    try:
        if os.path.exists(secret_file):
            with open(secret_file, "r", encoding="utf-8") as f:
                existing = f.read().strip()
                if existing:
                    return existing
    except Exception:
        pass
    new_secret = secrets.token_urlsafe(32)
    try:
        with open(secret_file, "w", encoding="utf-8") as f:
            f.write(new_secret)
    except Exception as e:
        logger.warning(f"Could not persist secret.key, sessions/passwords will reset on restart: {e}")
    return new_secret

CONFIG = {
    "port": int(os.environ.get("PORT", 8000)),
    "secret": _get_or_create_secret(),
    "telegram_token": "",
    "telegram_admin_id": "",
    "bot_lang": "en",
    "railway_token": "",
    "notify_connections": "0",
}

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.mount("/client", StaticFiles(directory="client"), name="client")

connections: dict = {}
connections_lock = asyncio.Lock()
connection_sockets: dict = {}
link_ip_map: dict = defaultdict(set)
stats = {"total_bytes": 0, "total_requests": 0, "total_errors": 0, "start_time": time.time()}
error_logs: deque = deque(maxlen=50)
hourly_traffic: dict = defaultdict(int)
daily_traffic: dict = defaultdict(int)
http_client: httpx.AsyncClient | None = None

LINKS: dict = {}
LINKS_LOCK = asyncio.Lock()
CUSTOM_ADDRESSES: list = []
CUSTOM_ADDRESSES_LOCK = asyncio.Lock()

notified_uids = set()

SESSION_COOKIE = "ren_session"
SESSION_TTL = 60 * 60 * 24 * 7
UNLIMITED_QUOTA_BYTES = 53687091200000
# پورت همیشه ثابت روی 443 است — دیگه قابل تغییر توسط کاربر نیست
DEFAULT_PORT = 443
MIN_PORT, MAX_PORT = 1, 65535

# نوع پروتکل (auth scheme) و ترابرد به‌صورت دو بُعد جدا از هم هستن؛ کاربر برای هر
# کانفیگ هرکدوم رو مستقل از اون یکی انتخاب می‌کنه (مثلاً Trojan + XHTTP stream-up
# یا VLESS + WebSocket و ...). مقدار ذخیره‌شده‌ی نهایی همیشه "{auth}-{transport}"ه.
AUTH_TYPES = ("vless", "trojan")
DEFAULT_AUTH = "vless"

TRANSPORTS = ("ws", "xhttp-packet-up", "xhttp-stream-up")
DEFAULT_TRANSPORT = "ws"

PROTOCOLS = tuple(f"{a}-{t}" for a in AUTH_TYPES for t in TRANSPORTS)
DEFAULT_PROTOCOL = f"{DEFAULT_AUTH}-{DEFAULT_TRANSPORT}"

def split_protocol(protocol: str) -> tuple[str, str]:
    """مقدار ذخیره‌شده‌ی protocol ("auth-transport") رو به دو بخش auth/transport می‌شکونه."""
    protocol = normalize_protocol(protocol)
    auth, transport = protocol.split("-", 1)
    return auth, transport

def normalize_protocol(value: str | None) -> str:
    """قدیم‌ترها مقدار protocol فقط ترابرد بود (مثلاً 'xhttp-packet-up' بدون
    پیشوند auth) چون auth همیشه vless بود. این تابع مقادیر قدیمی رو به فرمت
    جدید 'auth-transport' تبدیل می‌کنه تا کانفیگ‌های قبلی خراب نشن."""
    value = (value or "").strip().lower()
    if value in PROTOCOLS:
        return value
    if value in TRANSPORTS:  # legacy value با auth ضمنی vless
        return f"vless-{value}"
    return DEFAULT_PROTOCOL

# Fingerprint (uTLS) های قابل انتخاب برای هر کانفیگ — مستقل برای هر پروتکل انتخاب می‌شه
FINGERPRINTS = ("chrome", "firefox", "safari", "ios", "android", "edge", "360", "qq", "random", "randomized")
DEFAULT_FINGERPRINT = "chrome"

# لیست بسته‌ی ALPNهای قابل‌انتخاب (دیگه فیلد آزاد نیست) — مستقل برای هر پروتکل انتخاب می‌شه
ALPN_OPTIONS = ("h3", "h2", "http/1.1", "h3,h2,http/1.1", "h3,h2", "h2,http/1.1")

# پیش‌فرض ALPN بر اساس نوع ترابرد، وقتی کاربر مقدار انتخاب نکرده (auth روی این تاثیری نداره)
DEFAULT_ALPN_BY_PROTOCOL = {}
for _auth in AUTH_TYPES:
    DEFAULT_ALPN_BY_PROTOCOL[f"{_auth}-ws"] = "http/1.1"
    DEFAULT_ALPN_BY_PROTOCOL[f"{_auth}-xhttp-packet-up"] = "h2,http/1.1"
    DEFAULT_ALPN_BY_PROTOCOL[f"{_auth}-xhttp-stream-up"] = "h2,http/1.1"
del _auth

# ═══════════════════ ساختار «variants» — هر لینک می‌تونه هم‌زمان هم VLESS هم Trojan ═══════════════════
# هر لینک به‌جای یک protocol واحد، یک variant مستقل برای هر auth type داره:
#   link["variants"] = {
#       "vless":  {"enabled": bool, "transport": ..., "fingerprint": ..., "alpn": ...},
#       "trojan": {"enabled": bool, "transport": ..., "fingerprint": ..., "alpn": ...},
#   }
# حداقل یکی از دو تا باید enabled باشه.

def default_variants() -> dict:
    return {
        "vless": {"enabled": True, "transport": DEFAULT_TRANSPORT, "fingerprint": DEFAULT_FINGERPRINT, "alpn": DEFAULT_ALPN_BY_PROTOCOL["vless-ws"]},
        "trojan": {"enabled": False, "transport": DEFAULT_TRANSPORT, "fingerprint": DEFAULT_FINGERPRINT, "alpn": DEFAULT_ALPN_BY_PROTOCOL["trojan-ws"]},
    }

def sanitize_variant(v: dict | None, auth: str) -> dict:
    v = v or {}
    transport = str(v.get("transport") or DEFAULT_TRANSPORT).strip().lower()
    if transport not in TRANSPORTS:
        transport = DEFAULT_TRANSPORT
    fp = str(v.get("fingerprint") or DEFAULT_FINGERPRINT).strip().lower()
    if fp not in FINGERPRINTS:
        fp = DEFAULT_FINGERPRINT
    alpn = str(v.get("alpn") or "").strip()
    if alpn not in ALPN_OPTIONS:
        alpn = DEFAULT_ALPN_BY_PROTOCOL.get(f"{auth}-{transport}", "http/1.1")
    return {"enabled": bool(v.get("enabled", False)), "transport": transport, "fingerprint": fp, "alpn": alpn}

def sanitize_variants(variants: dict | None) -> dict:
    variants = variants or {}
    result = {auth: sanitize_variant(variants.get(auth), auth) for auth in AUTH_TYPES}
    if not any(result[a]["enabled"] for a in AUTH_TYPES):
        result["vless"]["enabled"] = True  # حداقل یکی باید فعال بمونه
    return result

def variants_from_legacy(protocol: str, fingerprint: str, alpn: str) -> dict:
    """کانفیگ‌های قدیمی که فقط یک protocol/fingerprint/alpn ستونی داشتن رو به فرمت جدید تبدیل می‌کنه."""
    auth, transport = split_protocol(protocol)
    variants = default_variants()
    for a in AUTH_TYPES:
        variants[a]["enabled"] = False
    variants[auth] = {
        "enabled": True, "transport": transport,
        "fingerprint": fingerprint or DEFAULT_FINGERPRINT,
        "alpn": alpn or DEFAULT_ALPN_BY_PROTOCOL.get(f"{auth}-{transport}", "http/1.1"),
    }
    return variants

def variants_to_legacy(variants: dict) -> tuple[str, str, str]:
    """برای پرشدن ستون‌های قدیمی protocol/fingerprint/alpn (صرفاً برای سازگاری با ابزارهای بیرونی)."""
    for auth in AUTH_TYPES:
        v = (variants or {}).get(auth, {})
        if v.get("enabled"):
            return f"{auth}-{v.get('transport', DEFAULT_TRANSPORT)}", v.get("fingerprint", DEFAULT_FINGERPRINT), v.get("alpn", "")
    return DEFAULT_PROTOCOL, DEFAULT_FINGERPRINT, ""

def variants_from_body(body: dict, base: dict | None = None) -> dict:
    """بدنه‌ی JSON درخواست (فیلدهای vless_enabled/vless_transport/... و trojan_*) رو
    به ساختار variants تبدیل می‌کنه. base مقادیر پیش‌فرض/موجود رو برای فیلدهایی که
    توی body نیومدن فراهم می‌کنه (برای PATCH جزئی)."""
    base = base or default_variants()
    result = {}
    for auth in AUTH_TYPES:
        cur = dict(base.get(auth, {}))
        if f"{auth}_enabled" in body:
            cur["enabled"] = bool(body.get(f"{auth}_enabled"))
        if f"{auth}_transport" in body:
            cur["transport"] = body.get(f"{auth}_transport")
        if f"{auth}_fingerprint" in body:
            cur["fingerprint"] = body.get(f"{auth}_fingerprint")
        if f"{auth}_alpn" in body:
            cur["alpn"] = body.get(f"{auth}_alpn")
        result[auth] = cur
    return sanitize_variants(result)

DB_FILE = "/data/panel.db" if os.path.isdir("/data") else "panel.db"
if os.path.isdir("/data"):
    logger.warning(f"[STARTUP] Persistent volume detected at /data -> using {DB_FILE} (data survives restarts/deploys)")
else:
    logger.warning(f"[STARTUP] NO persistent volume found at /data -> using EPHEMERAL {DB_FILE} (ALL links/data will be LOST on next restart/deploy!)")
DB_LOCK = asyncio.Lock()
bot = None
bot_polling_task: asyncio.Task | None = None

BOT_I18N = {
    "en": {
        "btn_stats": "📊 Stats",
        "btn_users": "👥 Users",
        "btn_top": "🔝 Top Users",
        "btn_create": "➕ Create User",
        "btn_addip": "🌐 Add Clean IP",
        "btn_lang": "فارسی",
        "welcome": "👑 <b>Welcome to エムエムディー Panel!</b>\nManage your VLESS inbounds.",
        "lang_switched": "🌐 Language switched to <b>English</b>.",
        "stats": (
            "<b>📊 Server Status Dashboard</b>\n\n"
            "🌐 <b>Domain:</b> <code>{domain}</code>\n"
            "🔋 <b>CPU:</b> <code>{cpu:.1f}%</code>\n"
            "💾 <b>Memory:</b> <code>{mem:.1f}%</code>\n"
            "⏱ <b>Uptime:</b> <code>{uptime}</code>\n"
            "👥 <b>Active Connections:</b> <code>{active}</code>\n"
            "📈 <b>Total Traffic:</b> <code>{traffic} MB</code>\n"
            "🔑 <b>Total Inbounds:</b> <code>{links}</code>"
        ),
        "users_title": "<b>👥 Users List & Usage:</b>\n",
        "users_line": "• <b>{label}</b>: {used} / {limit} (⌛ {exp}) | {status}",
        "no_inbounds": "No inbounds found.",
        "status_on": "🟢 On",
        "status_off": "🔴 Off",
        "top_title": "<b>🔝 Top 5 Users by Usage:</b>\n",
        "top_line": "{i}. <b>{label}</b>: Used {used} of {limit}",
        "create_format": (
            "❌ <b>Invalid format.</b>\n"
            "Format: <code>/create [name] [limit_GB] [days]</code>\n"
            "Example: <code>/create Ali 15 30</code>"
        ),
        "create_bad_name": "❌ <b>Name must contain only English letters and numbers.</b>",
        "create_bad_limit": "❌ <b>Traffic limit must be a number.</b>",
        "create_bad_days": "❌ <b>Days valid must be an integer.</b>",
        "create_exists": "❌ <b>An inbound with the name '{label}' already exists.</b>",
        "create_success": (
            "✅ <b>Inbound Created Successfully!</b>\n\n"
            "👤 <b>Name:</b> <code>{label}</code>\n"
            "📊 <b>Quota:</b> <code>{quota}</code>\n"
            "⌛ <b>Expiry:</b> <code>{expiry}</code>\n\n"
            "🔗 <b>VLESS Link:</b>\n<code>{vless}</code>\n\n"
            "🌐 <b>Subscription URL:</b>\n<code>{sub}</code>"
        ),
        "unlimited": "Unlimited",
        "days_fmt": "{days} days",
        "addaddr_format": "❌ Format: <code>/addaddr [ip_or_domain]</code>",
        "addaddr_invalid": "❌ Invalid address format.",
        "addaddr_exists": "⚠️ Address '{addr}' is already in the list.",
        "addaddr_success": "✅ Clean IP/Domain <code>{addr}</code> successfully added.",
        "toggle_format": "❌ Format: <code>/{action} [username]</code>",
        "not_found": "❌ User '{name}' not found.",
        "toggle_success": "✅ User <code>{name}</code> successfully <b>{state}</b>.",
        "state_enabled": "Enabled",
        "state_disabled": "Disabled",
        "reset_format": "❌ Format: <code>/reset [username]</code>",
        "reset_success": "🔄 Usage reset to 0 for user <code>{name}</code>.",
        "create_guide": (
            "➕ <b>How to create a user:</b>\n\n"
            "Use the <code>/create</code> command. Format:\n"
            "<code>/create [name] [limit_GB] [days]</code>\n\n"
            "<b>Examples:</b>\n"
            "• <code>/create Ali 15 30</code> (15GB limit, 30 days validity)\n"
            "• <code>/create Reza 0 0</code> (Unlimited, No Expiry)"
        ),
        "addip_guide": (
            "🌐 <b>How to add Clean IP:</b>\n\n"
            "Use the <code>/addaddr</code> command. Format:\n"
            "<code>/addaddr [ip_or_domain]</code>\n\n"
            "<b>Example:</b>\n"
            "• <code>/addaddr cf.example.com</code>\n"
            "• <code>/addaddr 1.1.1.1</code>"
        ),
        "quota_alert": (
            "⚠️ <b>Quota Alert!</b>\n"
            "User: <code>{label}</code> has reached their limit.\n"
            "Usage: <code>{used} / {limit}</code>"
        ),
        "expiry_alert": (
            "⏰ <b>Expiry Alert!</b>\n"
            "User: <code>{label}</code> has expired.\n"
            "Expiry date: <code>{exp}</code>"
        ),
    },
    "fa": {
        "btn_stats": "📊 آمار",
        "btn_users": "👥 کاربران",
        "btn_top": "🔝 پرمصرف‌ترین‌ها",
        "btn_create": "➕ ساخت کاربر",
        "btn_addip": "🌐 افزودن آی‌پی تمیز",
        "btn_lang": "English",
        "welcome": "👑 <b>به پنل 에ممدی خوش اومدی!</b>\nاینباندهای VLESS رو مستقیم از تلگرام مدیریت کن.",
        "lang_switched": "🌐 زبان به <b>فارسی</b> تغییر یافت.",
        "stats": (
            "<b>📊 وضعیت سرور</b>\n\n"
            "🌐 <b>دامنه:</b> <code>{domain}</code>\n"
            "🔋 <b>پردازنده:</b> <code>{cpu:.1f}%</code>\n"
            "💾 <b>رم:</b> <code>{mem:.1f}%</code>\n"
            "⏱ <b>آپ‌تایم:</b> <code>{uptime}</code>\n"
            "👥 <b>اتصالات فعال:</b> <code>{active}</code>\n"
            "📈 <b>ترافیک کل:</b> <code>{traffic} MB</code>\n"
            "🔑 <b>تعداد کاربران:</b> <code>{links}</code>"
        ),
        "users_title": "<b>👥 لیست کاربران و میزان مصرف:</b>\n",
        "users_line": "• <b>{label}</b>: {used} / {limit} (⌛ {exp}) | {status}",
        "no_inbounds": "هیچ کاربری یافت نشد.",
        "status_on": "🟢 فعال",
        "status_off": "🔴 غیرفعال",
        "top_title": "<b>🔝 ۵ کاربر پرمصرف:</b>\n",
        "top_line": "{i}. <b>{label}</b>: مصرف {used} از {limit}",
        "create_format": (
            "❌ <b>فرمت اشتباه است.</b>\n"
            "فرمت: <code>/create [نام] [حجم_GB] [روز]</code>\n"
            "مثال: <code>/create Ali 15 30</code>"
        ),
        "create_bad_name": "❌ <b>نام فقط باید شامل حروف انگلیسی و عدد باشد.</b>",
        "create_bad_limit": "❌ <b>حجم ترافیک باید عدد باشد.</b>",
        "create_bad_days": "❌ <b>تعداد روز باید عدد صحیح باشد.</b>",
        "create_exists": "❌ <b>کاربری با نام «{label}» از قبل وجود دارد.</b>",
        "create_success": (
            "✅ <b>کاربر با موفقیت ساخته شد!</b>\n\n"
            "👤 <b>نام:</b> <code>{label}</code>\n"
            "📊 <b>حجم:</b> <code>{quota}</code>\n"
            "⌛ <b>انقضا:</b> <code>{expiry}</code>\n\n"
            "🔗 <b>لینک VLESS:</b>\n<code>{vless}</code>\n\n"
            "🌐 <b>آدرس اشتراک:</b>\n<code>{sub}</code>"
        ),
        "unlimited": "نامحدود",
        "days_fmt": "{days} روز",
        "addaddr_format": "❌ فرمت: <code>/addaddr [آی‌پی_یا_دامنه]</code>",
        "addaddr_invalid": "❌ فرمت آدرس نامعتبر است.",
        "addaddr_exists": "⚠️ آدرس «{addr}» قبلاً در لیست موجود است.",
        "addaddr_success": "✅ آی‌پی/دامنه‌ی <code>{addr}</code> با موفقیت اضافه شد.",
        "toggle_format": "❌ فرمت: <code>/{action} [نام‌کاربری]</code>",
        "not_found": "❌ کاربر «{name}» پیدا نشد.",
        "toggle_success": "✅ کاربر <code>{name}</code> با موفقیت <b>{state}</b> شد.",
        "state_enabled": "فعال",
        "state_disabled": "غیرفعال",
        "reset_format": "❌ فرمت: <code>/reset [نام‌کاربری]</code>",
        "reset_success": "🔄 مصرف کاربر <code>{name}</code> به صفر بازنشانی شد.",
        "create_guide": (
            "➕ <b>راهنمای ساخت کاربر:</b>\n\n"
            "از دستور <code>/create</code> استفاده کن. فرمت:\n"
            "<code>/create [نام] [حجم_GB] [روز]</code>\n\n"
            "<b>مثال‌ها:</b>\n"
            "• <code>/create Ali 15 30</code> (۱۵ گیگ، ۳۰ روز اعتبار)\n"
            "• <code>/create Reza 0 0</code> (نامحدود، بدون انقضا)"
        ),
        "addip_guide": (
            "🌐 <b>راهنمای افزودن آی‌پی تمیز:</b>\n\n"
            "از دستور <code>/addaddr</code> استفاده کن. فرمت:\n"
            "<code>/addaddr [آی‌پی_یا_دامنه]</code>\n\n"
            "<b>مثال:</b>\n"
            "• <code>/addaddr cf.example.com</code>\n"
            "• <code>/addaddr 1.1.1.1</code>"
        ),
        "quota_alert": (
            "⚠️ <b>هشدار اتمام حجم!</b>\n"
            "کاربر: <code>{label}</code> به سقف مصرف رسید.\n"
            "مصرف: <code>{used} / {limit}</code>"
        ),
        "expiry_alert": (
            "⏰ <b>هشدار انقضا!</b>\n"
            "کاربر: <code>{label}</code> منقضی شد.\n"
            "تاریخ انقضا: <code>{exp}</code>"
        ),
    },
}

def bot_lang() -> str:
    return CONFIG.get("bot_lang") if CONFIG.get("bot_lang") in ("en", "fa") else "en"

def L(key: str, **kwargs) -> str:
    lang = bot_lang()
    template = BOT_I18N.get(lang, BOT_I18N["en"]).get(key) or BOT_I18N["en"].get(key, key)
    try:
        return template.format(**kwargs)
    except Exception:
        return template

def build_main_keyboard():
    if not TELEBOT_AVAILABLE:
        return None
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(
        types.InlineKeyboardButton(L("btn_stats"), callback_data="tg_stats"),
        types.InlineKeyboardButton(L("btn_users"), callback_data="tg_users"),
        types.InlineKeyboardButton(L("btn_top"), callback_data="tg_top"),
        types.InlineKeyboardButton(L("btn_create"), callback_data="tg_create_guide"),
        types.InlineKeyboardButton(L("btn_addip"), callback_data="tg_add_ip_guide"),
        types.InlineKeyboardButton(L("btn_lang"), callback_data="tg_lang_toggle"),
    )
    return kb

# ── SQLite Database ──────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS links (
            uuid TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            limit_bytes INTEGER DEFAULT 0,
            used_bytes INTEGER DEFAULT 0,
            max_connections INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            expires_at TEXT,
            protocol TEXT DEFAULT 'vless-ws',
            fingerprint TEXT DEFAULT 'chrome',
            alpn TEXT DEFAULT '',
            port INTEGER DEFAULT 443
        );
        CREATE TABLE IF NOT EXISTS custom_addresses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL UNIQUE
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            expires_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS auth (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            password_hash TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            link TEXT,
            seen INTEGER DEFAULT 0,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS github_cache (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            latest_tag TEXT,
            latest_url TEXT,
            checked_at REAL
        );
    """)
    conn.commit()
    # Migrate older DBs created before protocol/fingerprint/alpn/port existed
    existing_cols = {row["name"] for row in conn.execute("PRAGMA table_info(links)").fetchall()}
    for col, ddl in (
        ("protocol", "ALTER TABLE links ADD COLUMN protocol TEXT DEFAULT 'vless-ws'"),
        ("fingerprint", "ALTER TABLE links ADD COLUMN fingerprint TEXT DEFAULT 'chrome'"),
        ("alpn", "ALTER TABLE links ADD COLUMN alpn TEXT DEFAULT ''"),
        ("port", "ALTER TABLE links ADD COLUMN port INTEGER DEFAULT 443"),
        ("variants_json", "ALTER TABLE links ADD COLUMN variants_json TEXT DEFAULT ''"),
    ):
        if col not in existing_cols:
            conn.execute(ddl)
    conn.commit()
    # Ensure default auth row
    cur = conn.execute("SELECT password_hash FROM auth WHERE id = 1")
    row = cur.fetchone()
    if row is None:
        conn.execute("INSERT INTO auth (id, password_hash) VALUES (1, ?)", (AUTH["password_hash"],))
        conn.commit()
    else:
        AUTH["password_hash"] = row["password_hash"]
    conn.close()
    migrate_json_to_sqlite()

def migrate_json_to_sqlite():
    json_file = "panel_db.json"
    if not os.path.exists(json_file):
        return
    conn = get_db()
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Migrate auth
        pw = data.get("auth_hash")
        if pw:
            conn.execute("INSERT OR REPLACE INTO auth (id, password_hash) VALUES (1, ?)", (pw,))
            AUTH["password_hash"] = pw
        # Migrate links
        links = data.get("links", {})
        for uid, link in links.items():
            variants = variants_from_legacy(link.get("protocol", DEFAULT_PROTOCOL), link.get("fingerprint", DEFAULT_FINGERPRINT), link.get("alpn", ""))
            legacy_protocol, legacy_fp, legacy_alpn = variants_to_legacy(variants)
            conn.execute("""
                INSERT OR REPLACE INTO links (uuid, label, limit_bytes, used_bytes, max_connections, created_at, active, expires_at, protocol, fingerprint, alpn, port, variants_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (uid, link.get("label", uid), link.get("limit_bytes", 0), link.get("used_bytes", 0),
                  link.get("max_connections", 0), link.get("created_at", datetime.now(timezone.utc).isoformat()),
                  1 if link.get("active", True) else 0, link.get("expires_at"),
                  legacy_protocol, legacy_fp, legacy_alpn, link.get("port", DEFAULT_PORT),
                  json.dumps(variants)))
            LINKS[uid] = dict(link)
            LINKS[uid]["variants"] = variants
        # Migrate addresses
        addresses = data.get("custom_addresses", [])
        CUSTOM_ADDRESSES.clear()
        for addr in addresses:
            conn.execute("INSERT OR IGNORE INTO custom_addresses (address) VALUES (?)", (addr,))
            CUSTOM_ADDRESSES.append(addr)
        # Migrate settings
        for key in ("telegram_token", "telegram_admin_id", "bot_lang"):
            val = data.get(key)
            if val:
                conn.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(val)))
                CONFIG[key] = val
        conn.commit()
        # Backup and remove old JSON
        os.rename(json_file, json_file + ".bak")
        logger.info(f"Migrated from {json_file} to SQLite database.")
    except Exception as e:
        logger.error(f"Migration error: {e}")
    finally:
        conn.close()

async def save_db():
    conn = get_db()
    try:
        async with DB_LOCK:
            # Save auth
            conn.execute("INSERT OR REPLACE INTO auth (id, password_hash) VALUES (1, ?)", (AUTH["password_hash"],))
            # Save links
            async with LINKS_LOCK:
                for uid, link in list(LINKS.items()):
                    variants = sanitize_variants(link.get("variants"))
                    legacy_protocol, legacy_fp, legacy_alpn = variants_to_legacy(variants)
                    conn.execute("""
                        INSERT OR REPLACE INTO links (uuid, label, limit_bytes, used_bytes, max_connections, created_at, active, expires_at, protocol, fingerprint, alpn, port, variants_json)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (uid, link["label"], link["limit_bytes"], link["used_bytes"],
                          link.get("max_connections", 0), link["created_at"],
                          1 if link.get("active", True) else 0, link.get("expires_at"),
                          legacy_protocol, legacy_fp, legacy_alpn, link.get("port", DEFAULT_PORT),
                          json.dumps(variants)))
            # Save addresses
            async with CUSTOM_ADDRESSES_LOCK:
                conn.execute("DELETE FROM custom_addresses")
                for ad...(truncated 174411 characters)...
etch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:pw})});
    if(r.ok){$m('login-pw').value='';showDashboard()}
    else $m('login-err').style.display='block';
  }catch(e){$m('login-err').style.display='block'}
}

async function doLogout(){
  await fetch('/api/logout',{method:'POST'});
  showLogin();
}

document.querySelectorAll('.nav-item[data-page]').forEach(el=>{
  el.addEventListener('click',()=>switchPage(el.dataset.page));
});

function switchPage(id){
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  const target=$m('page-'+id);
  if(target)target.classList.add('active');
  document.querySelectorAll('.nav-item').forEach(n=>n.classList.toggle('active',n.dataset.page===id));
}

function toast(msg,err=false){
  const t=$m('toast');
  t.textContent=msg;
  t.className='toast'+(err?' err':'')+' show';
  clearTimeout(t._hide);
  t._hide=setTimeout(()=>t.classList.remove('show'),3000);
}

function fmtB(b){
  if(!b||b===0)return'0 B';
  return b>=1073741824?(b/1073741824).toFixed(2)+' GB':
         b>=1048576?(b/1048576).toFixed(2)+' MB':(b/1024).toFixed(1)+' KB';
}
function fmtLim(b){
  if(!b||b===0)return'∞';
  const g=b/1073741824;
  return(g%1===0?g.toFixed(0):g.toFixed(1))+' GB';
}
function fmtExp(ea){
  if(!ea||ea===0)return'∞';
  const d=new Date(ea)-new Date();
  if(d<=0)return'Expired';
  const days=Math.floor(d/86400000);
  if(days>0)return days+'d';
  const hours=Math.floor(d/3600000);
  if(hours>0)return hours+'h';
  return Math.floor(d/60000)+'m';
}

function setFilter(filter,el){
  cf=filter;
  document.querySelectorAll('.chip').forEach(c=>c.classList.remove('active'));
  if(el)el.classList.add('active');
  filterLinks();
}

function filterLinks(){
  const q=($m('srch')?.value||'').toLowerCase();
  let r=allLinks;
  if(cf==='active')r=r.filter(l=>l.active);
  else if(cf==='off')r=r.filter(l=>!l.active);
  if(q)r=r.filter(l=>l.label.toLowerCase().includes(q)||l.uuid.toLowerCase().includes(q));
  renderLinks(r);
}

function processAlertsAndCharts(){
  const alertsList=$m('alerts-list');
  const alertsBox=$m('alerts-box');
  alertsList.innerHTML='';
  let alertCount=0;

  allLinks.forEach(l=>{
    const u=l.used_bytes||0;
    const lim=l.limit_bytes||0;
    const pct=lim>0?(u/lim)*100:0;
    if(lim>0&&pct>=90){
      alertCount++;
      alertsList.innerHTML+=`<div class="alert-item"><span style="font-weight:600">🔴 '${esc(l.label)}' near limit:</span><span>${pct.toFixed(1)}% Used</span></div>`;
    }
    if(l.expires_at){
      const diff=new Date(l.expires_at)-new Date();
      const days=diff/86400000;
      if(days>0&&days<=3){
        alertCount++;
        alertsList.innerHTML+=`<div class="alert-item"><span style="font-weight:600">🟡 '${esc(l.label)}' expiring soon:</span><span>${days.toFixed(1)} Days</span></div>`;
      }
    }
  });
  alertsBox.style.display=alertCount>0?'block':'none';

  if(iChart){
    const sorted=[...allLinks].sort((a,b)=>(b.used_bytes||0)-(a.used_bytes||0)).slice(0,8);
    iChart.data.labels=sorted.map(x=>x.label);
    iChart.data.datasets[0].data=sorted.map(x=>Math.round((x.used_bytes||0)/(1024*1024)));
    iChart.data.datasets[0].backgroundColor=genDistinctColors(sorted.length);
    iChart.update();
  }
}

function renderLinks(links){
  const tb=$m('ltb');
  const em=$m('lempty');
  const mc=$m('mcards');
  if(!links||!links.length){
    tb.innerHTML='';mc.innerHTML='';em.style.display='block';
    em.textContent=em.getAttribute('data-'+lang)||'No inbounds found';
    return;
  }
  em.style.display='none';
  let idx=links.length;
  const rows=links.map(l=>{
    const u=l.used_bytes||0;
    const lim=l.limit_bytes||0;
    const pct=lim>0?Math.min(100,(u/lim)*100):0;
    const col=pct>90?'var(--red)':pct>70?'var(--yellow)':'var(--gold)';
    const ex=fmtExp(l.expires_at);
    const ec=ex==='Expired'?'var(--red)':ex==='∞'?'var(--text3)':'var(--text2)';
    const i=idx--;
    const cc=l.current_connections||0;
    const mc2=l.max_connections||0;
    return{l,pct,col,ex,ec,i,cc,mc2,u,lim};
  });

  const editText=tr('edit');
  const copyText=tr('copy');
  const subText=tr('sub');
  const qrText=tr('qr');
  const delText=tr('del');

  tb.innerHTML=rows.map(r=>`<tr>
    <td style="color:var(--text3);font-size:10.5px">${r.i}</td>
    <td style="font-weight:600">${esc(r.l.label)}</td>
    <td><span class="tag tag-vless">${protoBadge(r.l.variants)}</span></td>
    <td><div class="pill"><span class="pill-used">${fmtB(r.u)}</span><div class="pill-bar"><div class="pill-fill" style="width:${r.pct}%;background:${r.col}"></div></div><span class="pill-lim">${fmtLim(r.lim)}</span></div></td>
    <td style="font-size:11px;font-weight:600;color:${r.mc2>0&&r.cc>=r.mc2?'var(--red)':'var(--text2)'}">${r.cc}/${r.mc2||'∞'}</td>
    <td style="font-size:10.5px;font-weight:700;color:${r.ec}">${r.ex}</td>
    <td><div style="display:flex;gap:3px;align-items:center;flex-wrap:wrap">
      <button class="toggle ${r.l.active?'on':''}" data-uid="${r.l.uuid}" onclick="togLink(this)"></button>
      <button class="act-btn act-edit" onclick="showEditMo('${r.l.uuid}')">${editText}</button>
      <button class="act-btn act-copy" onclick="cpLink('${esc((r.l.vless_links||[]).join(String.fromCharCode(10)))}')">${copyText}</button>
      <button class="act-btn act-sub" onclick="cpSub('${r.l.uuid}')">${subText}</button>
      <button class="act-btn act-qr" onclick="showQR('${esc((r.l.vless_links||[])[0]||'')}')">${qrText}</button>
      <button class="act-btn act-del" onclick="delLink('${r.l.uuid}')">${delText}</button>
    </div></td>
  </tr>`).join('');

  mc.innerHTML=rows.map(r=>`<div class="m-card">
    <div class="m-card-hd">
      <div style="display:flex;align-items:center;gap:7px">
        <span style="font-size:11px;color:var(--text3)">#${r.i}</span>
        <span style="font-weight:600;font-size:14px">${esc(r.l.label)}</span>
        <span class="tag tag-vless">${protoBadge(r.l.variants)}</span>
      </div>
      <button class="toggle ${r.l.active?'on':''}" data-uid="${r.l.uuid}" onclick="togLink(this)"></button>
    </div>
    <div class="pill"><span class="pill-used">${fmtB(r.u)}</span><div class="pill-bar"><div class="pill-fill" style="width:${r.pct}%;background:${r.col}"></div></div><span class="pill-lim">${fmtLim(r.lim)}</span></div>
    <div style="font-size:11.5px;color:${r.ec};margin-top:6px;font-weight:600">⏳ ${r.ex} · ${r.cc}/${r.mc2||'∞'} IPs</div>
    <div style="m-card-acts">
      <button class="act-btn act-edit" onclick="showEditMo('${r.l.uuid}')">${editText}</button>
      <button class="act-btn act-copy" onclick="cpLink('${esc((r.l.vless_links||[]).join(String.fromCharCode(10)))}')">${copyText}</button>
      <button class="act-btn act-sub" onclick="cpSub('${r.l.uuid}')">${subText}</button>
      <button class="act-btn act-qr" onclick="showQR('${esc((r.l.vless_links||[])[0]||'')}')">${qrText}</button>
      <button class="act-btn act-del" onclick="delLink('${r.l.uuid}')">${delText}</button>
    </div>
  </div>`).join('');
  
  processAlertsAndCharts();
}

async function togLink(el){
  const uid=el.dataset.uid;
  const l=allLinks.find(x=>x.uuid===uid);
  if(!l)return;
  const na=!l.active;
  try{
    const r=await fetch('/api/links/'+uid,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({active:na})});
    if(!r.ok)throw new Error();
    l.active=na;filterLinks();loadStats();
  }catch(e){toast('Failed to toggle',true)}
}

function showAddMo(){$m('mo-add').classList.add('show')}

// وقتی transport یک بلاک (vless یا trojan) عوض شد، ALPN همون بلاک رو به پیش‌فرضش ببر
const ALPN_DEFAULTS={
  'vless-ws':'http/1.1','vless-xhttp-packet-up':'h2,http/1.1','vless-xhttp-stream-up':'h2,http/1.1',
  'trojan-ws':'http/1.1','trojan-xhttp-packet-up':'h2,http/1.1','trojan-xhttp-stream-up':'h2,http/1.1',
};
function syncAlpnDefault(auth,transportId,alpnId){
  const key=auth+'-'+$m(transportId).value;
  $m(alpnId).value=ALPN_DEFAULTS[key]||'http/1.1';
}
function toggleVariantBox(prefix,auth){
  $m(prefix+'_'+auth+'_box').style.display=$m(prefix+'_'+auth+'_enabled').checked?'':'none';
}
function readVariantFields(prefix,auth){
  return {
    [auth+'_enabled']: $m(prefix+'_'+auth+'_enabled').checked,
    [auth+'_transport']: $m(prefix+'_'+auth+'_transport').value,
    [auth+'_fingerprint']: $m(prefix+'_'+auth+'_fp').value,
    [auth+'_alpn']: $m(prefix+'_'+auth+'_alpn').value,
  };
}
function fillVariantFields(prefix,auth,variant){
  $m(prefix+'_'+auth+'_enabled').checked=!!(variant&&variant.enabled);
  $m(prefix+'_'+auth+'_transport').value=(variant&&variant.transport)||'ws';
  $m(prefix+'_'+auth+'_fp').value=(variant&&variant.fingerprint)||'chrome';
  $m(prefix+'_'+auth+'_alpn').value=(variant&&variant.alpn)||ALPN_DEFAULTS[auth+'-ws'];
  toggleVariantBox(prefix,auth);
}

async function createLink(){
  const label=$m('nl').value.trim()||'New Link';
  if(!/^[a-zA-Z0-9\-_. ]+$/.test(label)){toast('Only English letters allowed',true);return}
  if(!$m('n_vless_enabled').checked && !$m('n_trojan_enabled').checked){toast('Enable at least one protocol (VLESS or Trojan)',true);return}
  const v=parseFloat($m('nv').value)||0;
  const mc=parseInt($m('nc').value)||0;
  const days=parseInt($m('nd').value)||0;
  const body=Object.assign({label,limit_value:v,limit_unit:'GB',max_connections:mc,days_valid:days},readVariantFields('n','vless'),readVariantFields('n','trojan'));
  try{
    const r=await fetch('/api/links',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!r.ok)throw new Error();
    toast('Created');
    $m('nl').value='';$m('nv').value='';$m('nc').value='';$m('nd').value='';
    $m('mo-add').classList.remove('show');
    await loadLinks();await loadStats();
  }catch(e){toast('Error creating link',true)}
}

function showEditMo(uid){
  const l=allLinks.find(x=>x.uuid===uid);
  if(!l)return;
  $m('eu').value=uid;
  $m('en2').value=l.label;
  $m('el').value=l.limit_bytes>0?(l.limit_bytes/1073741824):'';
  $m('ec').value=l.max_connections>0?l.max_connections:'';
  $m('ed').value='';
  const variants=l.variants||{};
  fillVariantFields('e','vless',variants.vless);
  fillVariantFields('e','trojan',variants.trojan);
  $m('et').textContent=(lang==='fa'?'ویرایش: ':'EDIT: ')+l.label;
  $m('mo-edit').classList.add('show');
}

async function saveEdit(){
  const uid=$m('eu').value;
  if(!$m('e_vless_enabled').checked && !$m('e_trojan_enabled').checked){toast('Enable at least one protocol (VLESS or Trojan)',true);return}
  const v=parseFloat($m('el').value)||0;
  const mc=parseInt($m('ec').value)||0;
  const days=parseInt($m('ed').value)||0;
  const body=Object.assign({limit_value:v,limit_unit:'GB',max_connections:mc},readVariantFields('e','vless'),readVariantFields('e','trojan'));
  if(days>0)body.days_valid=days;
  try{
    const r=await fetch('/api/links/'+uid,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    if(!r.ok)throw new Error();
    toast('Updated');$m('mo-edit').classList.remove('show');await loadLinks();
  }catch(e){toast('Error updating',true)}
}

async function resetTraf(){
  const uid=$m('eu').value;
  if(!confirm('Reset traffic for this inbound?'))return;
  try{
    const r=await fetch('/api/links/'+uid,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({reset_usage:true})});
    if(!r.ok)throw new Error();
    toast('Traffic reset');await loadLinks();
  }catch(e){toast('Error resetting',true)}
}

async function delLink(uid){
  if(!confirm('Delete this inbound?'))return;
  try{
    const r=await fetch('/api/links/'+uid,{method:'DELETE'});
    if(!r.ok)throw new Error();
    toast('Deleted');await loadLinks();await loadStats();
  }catch(e){toast('Error deleting',true)}
}

function cpLink(txt){
  if(!txt){toast('No link to copy',true);return}
  navigator.clipboard.writeText(txt).then(()=>toast('Copied!')).catch(()=>toast('Failed to copy',true));
}

async function cpSub(uid){
  try{
    await navigator.clipboard.writeText('https://'+location.host+'/sub/'+uid);
    toast('Sub URL copied!');
  }catch(e){toast('Failed to copy',true)}
}

function showQR(txt){
  if(!txt){toast('No QR data',true);return}
  $m('qr-img').src='https://api.qrserver.com/v1/create-qr-code/?size=280x280&data='+encodeURIComponent(txt);
  $m('mo-qr').classList.add('show');
}

function dlQR(){
  const a=document.createElement('a');
  a.href=$m('qr-img').src;a.download='mmd-qr.png';a.click();
}

async function loadSettings(){
  try{
    const r=await fetch('/api/settings');
    if(r.ok){const d=await r.json();
      $m('tg-token').value=d.telegram_token||'';
      $m('tg-admin-id').value=d.telegram_admin_id||'';
      if($m('rw-tg-token'))$m('rw-tg-token').value=d.telegram_token||'';
      if($m('rw-tg-admin'))$m('rw-tg-admin').value=d.telegram_admin_id||'';
      if($m('rw-token'))$m('rw-token').value=d.railway_token||'';
      if($m('rw-tg-notify-conn'))$m('rw-tg-notify-conn').checked=!!d.notify_connections;
    }
  }catch(e){}
}

async function saveSettings(){
  const tok=$m('tg-token').value.trim();
  const adm=$m('tg-admin-id').value.trim();
  try{
    const r=await fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({telegram_token:tok,telegram_admin_id:adm})});
    if(r.ok)toast('Bot settings saved & restarted');
    else toast('Failed to save settings',true);
  }catch(e){toast('Error saving settings',true)}
}

async function saveAllSettings(){
  const tok=($m('rw-tg-token')?.value||'').trim();
  const adm=($m('rw-tg-admin')?.value||'').trim();
  const rwt=($m('rw-token')?.value||'').trim();
  const notifyConn=!!($m('rw-tg-notify-conn')?.checked);
  try{
    const r=await fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({telegram_token:tok,telegram_admin_id:adm,railway_token:rwt,notify_connections:notifyConn})});
    if(r.ok)toast('All settings saved');
    else toast('Failed to save settings',true);
  }catch(e){toast('Error saving settings',true)}
}

// ── Railway / Permanent Database ──────────────────────────────────────────

async function fetchRailwayProjects(){
  const token=$m('rw-token').value.trim();
  if(!token){toast('Enter your Railway token first',true);return}
  const btn=$m('rw-fetch-btn');
  const sel=$m('rw-project');
  btn.disabled=true;btn.textContent='Loading...';
  sel.disabled=true;sel.innerHTML='<option>Loading...</option>';
  $m('rw-volume-info').style.display='none';
  try{
    const r=await fetch('/api/railway/projects',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token})});
    if(!r.ok)throw new Error((await r.json()).detail||'Error');
    const d=await r.json();
    sel.innerHTML='<option value="">-- Select a project --</option>'+d.projects.map(p=>`<option value="${p.id}">${esc(p.name)}</option>`).join('');
    sel.disabled=false;
    toast('Found '+d.projects.length+' project(s)');
  }catch(e){toast(e.message||'Failed to fetch projects',true);sel.innerHTML='<option value="">Error loading</option>'}
  finally{btn.disabled=false;btn.textContent=btn.getAttribute('data-'+lang)||'Fetch'}
}

async function checkRailwayVolume(){
  const token=$m('rw-token').value.trim();
  const pid=$m('rw-project').value;
  if(!token||!pid){toast('Select a project first',true);return}
  const info=$m('rw-volume-info');
  const icon=$m('rw-volume-icon');
  const title=$m('rw-volume-title');
  const desc=$m('rw-volume-desc');
  const cbtn=$m('rw-create-btn');
  info.style.display='';icon.textContent='⏳';title.textContent='Checking...';desc.textContent='';cbtn.style.display='none';
  try{
    const r=await fetch('/api/railway/volume-status',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token,project_id:pid})});
    if(!r.ok)throw new Error((await r.json()).detail||'Error');
    const d=await r.json();
    const hasData=d.has_data_volume;
    if(hasData){
      icon.textContent='✅';icon.style.color='var(--green)';
      title.textContent='Volume at /data exists!';
      const v=d.volumes.find(x=>x.path==='data'||x.path==='/data')||d.volumes[0];
      desc.textContent=(v?'ID: '+v.id+' | Name: '+v.name+' | State: '+v.state:'');
      cbtn.style.display='none';
      $m('rdb-status').textContent='✅ Active';$m('rdb-status').style.color='var(--green)';
    }else{
      // No volume found - create it automatically, no manual click needed.
      icon.textContent='⏳';title.textContent='No volume found, creating one automatically...';desc.textContent='';
      $m('rdb-status').textContent='⏳ Creating...';$m('rdb-status').style.color='var(--gold)';
      await createRailwayVolume(true);
    }
  }catch(e){toast(e.message||'Failed to check',true);info.style.display='none'}
}

async function createRailwayVolume(silent){
  const token=$m('rw-token').value.trim();
  const pid=$m('rw-project').value;
  if(!token||!pid){toast('Select a project first',true);return}
  const icon=$m('rw-volume-icon');
  const title=$m('rw-volume-title');
  const desc=$m('rw-volume-desc');
  const cbtn=$m('rw-create-btn');
  cbtn.disabled=true;cbtn.textContent='Creating...';
  try{
    const r=await fetch('/api/railway/create-volume',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token,project_id:pid})});
    if(!r.ok)throw new Error((await r.json()).detail||'Error');
    if(!silent)toast('Volume created successfully!');
    else toast('/data volume created automatically');
    icon.textContent='✅';icon.style.color='var(--green)';
    title.textContent='Volume at /data created!';
    desc.textContent='It may take a few seconds to finish provisioning.';
    cbtn.style.display='none';
    $m('rdb-status').textContent='✅ Active';$m('rdb-status').style.color='var(--green)';
  }catch(e){
    icon.textContent='❌';icon.style.color='var(--red)';
    title.textContent='No volume at /data found';
    desc.textContent=e.message||'Failed to auto-create volume. Click below to retry.';
    cbtn.style.display='';
    $m('rdb-status').textContent='❌ Missing';$m('rdb-status').style.color='var(--red)';
    toast(e.message||'Failed to create volume',true);
  }
  finally{cbtn.disabled=false;cbtn.textContent=cbtn.getAttribute('data-'+lang)||'Create Volume'}
}

// Auto-check volume when project selection changes
document.addEventListener('change',function(e){
  if(e.target.id==='rw-project'&&e.target.value){
    checkRailwayVolume();
  }
});

async function loadStats(){
  try{
    const r=await fetch('/stats');
    if(r.status===401){showLogin();return}
    if(!r.ok)throw new Error();
    sData=await r.json();
    $m('sv-traffic').innerHTML=(sData.total_traffic_mb||0)+'<span class="stat-unit"> MB</span>';
    $m('sv-links').textContent=sData.links_count||0;
    $m('sv-uptime').textContent=sData.uptime||'-';
    $m('sv-domain').textContent=sData.domain||'-';
    $m('nb').textContent=sData.links_count||0;
    $m('last-up').textContent='Updated '+new Date().toLocaleTimeString();
    if($m('t-tr'))$m('t-tr').textContent=(sData.total_traffic_mb||0)+' MB';
    if($m('t-rq'))$m('t-rq').textContent=(sData.total_requests||0).toLocaleString();
    if($m('t-up'))$m('t-up').textContent=sData.uptime||'-';
    if(sData.cpu_percent!==undefined){
      const c=sData.cpu_percent;
      const cc=c>80?'var(--red)':c>50?'var(--yellow)':'var(--gold)';
      $m('cpu-v').textContent=c.toFixed(1)+'%';$m('cpu-v').style.color=cc;
      $m('cpu-b').style.width=c+'%';$m('cpu-b').style.background=cc;
    }
    if(sData.memory_percent!==undefined){
      const m=sData.memory_percent;
      const mc=m>80?'var(--red)':m>50?'var(--yellow)':'var(--green)';
      $m('mem-v').textContent=m.toFixed(1)+'%';$m('mem-v').style.color=mc;
      $m('mem-b').style.width=m+'%';$m('mem-b').style.background=mc;
    }
    updChart();
  }catch(e){}
}

async function loadLinks(){
  try{
    const r=await fetch('/api/links');
    if(r.status===401){showLogin();return}
    if(!r.ok)throw new Error();
    const d=await r.json();
    allLinks=d.links||[];filterLinks();
  }catch(e){}
}

async function chgPw(){
  const cur=$m('cpw').value;const nw=$m('npw').value;
  if(!cur||!nw){toast('Fill all fields',true);return}
  if(nw.length<4){toast('Password must be at least 4 characters',true);return}
  try{
    const r=await fetch('/api/change-password',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({current_password:cur,new_password:nw})});
    if(!r.ok){const d=await r.json().catch(()=>({}));throw new Error(d.detail||'Error')}
    toast('Password updated');$m('cpw').value='';$m('npw').value='';
  }catch(e){toast(e.message,true)}
}

function initChart(){
  const ctx=$m('tc');
  if(!ctx||tChart)return;
  tChart=new Chart(ctx,{
    type:'bar',
    data:{labels:[],datasets:[{label:'MB',data:[],backgroundColor:'rgba(59,130,246,0.45)',borderColor:'#3b82f6',borderWidth:1,borderRadius:4}]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false}},
      scales:{
        x:{grid:{display:false},ticks:{color:'rgba(59,130,246,0.35)',font:{size:10}}},
        y:{grid:{color:'rgba(59,130,246,0.06)'},ticks:{color:'rgba(59,130,246,0.35)',font:{size:10},callback:v=>v+' MB'},beginAtZero:true}
      }
    }
  });

  const ctx2=$m('inbound-chart');
  if(ctx2&&!iChart){
    iChart=new Chart(ctx2,{
      type:'doughnut',
      data:{labels:[],datasets:[{data:[],
        backgroundColor:[],
        borderWidth:0}]},
      options:{responsive:true,maintainAspectRatio:false,
        plugins:{legend:{display:true,position:'right',labels:{color:'rgba(255,255,255,0.6)',font:{size:10}}}}}
    }
  }
  updChartColors();
}

function updChartColors(){
  if(!tChart)return;
  const col=theme==='light'?'rgba(0,0,0,0.4)':'rgba(59,130,246,0.35)';
  const gridCol=theme==='light'?'rgba(0,0,0,0.06)':'rgba(59,130,246,0.06)';
  tChart.options.scales.x.ticks.color=col;
  tChart.options.scales.y.ticks.color=col;
  tChart.options.scales.y.grid.color=gridCol;
  tChart.update();
}

function updChart(){
  if(!tChart||!sData.hourly_traffic)return;
  const entries=Object.entries(sData.hourly_traffic).sort((a,b)=>a[0].localeCompare(b[0])).slice(-12);
  tChart.data.labels=entries.map(x=>{const p=x[0].split(' ');return p.length>1?p[1]:p[0]});
  tChart.data.datasets[0].data=entries.map(x=>Math.round(x[1]/1048576));
  tChart.update();
}

async function loadAddrs(){
  try{
    const r=await fetch('/api/addresses');
    if(!r.ok)throw new Error();
    const d=await r.json();allAddrs=d.addresses||[];renderAddrs();
  }catch(e){}
}

function renderAddrs(){
  const el=$m('addr-list');
  if(!el)return;
  if(!allAddrs||!allAddrs.length){el.innerHTML='<div style="color:var(--text3);font-size:12px">No addresses added</div>';return}
  el.innerHTML=allAddrs.map((a,i)=>`<div style="display:flex;align-items:center;justify-content:space-between;padding:12px 14px;background:var(--surface3);border:1px solid var(--border);border-radius:10px;margin-bottom:8px">
    <div style="display:flex;align-items:center;gap:10px">
      <span style="color:var(--gold);font-size:16px">🌐</span>
      <div><div style="font-size:14px;font-weight:600">${esc(a)}</div><div style="font-size:11px;color:var(--text3);margin-top:2px">Address #${i+1}</div></div>
    </div>
    <button class="act-btn act-del" onclick="delAddr(${i})">${tr('del')}</button>
  </div>`).join('');
}

function showAddAddrMo(){$m('na').value='';$m('mo-addr').classList.add('show')}

// ── Notifications ────────────────────────────────────────────────────────
const NOTIF_ICONS = {update:'🔔',quota:'⚠️',expiry:'⏰',info:'ℹ️'};

async function loadNotifs(){
  try{
    const r=await fetch('/api/notifications');
    if(r.status===401)return;
    if(!r.ok)return;
    const d=await r.json();
    renderNotifs(d.notifications||[]);
  }catch(e){}
}

function renderNotifs(notifs){
  const el=$m('notif-list');
  if(!el)return;
  if(!notifs||!notifs.length){
    el.innerHTML='<div class="empty" style="padding:32px">'+(lang==='fa'?'هیچ اعلانی وجود ندارد':'No notifications')+'</div>';
    return;
  }
  el.innerHTML=notifs.map(n=>{
    const icon=NOTIF_ICONS[n.type]||'ℹ️';
    const cls=n.seen?'':'unseen';
    const time=new Date(n.created_at).toLocaleString();
    const linkHtml=n.link?`<a href="${esc(n.link)}" target="_blank" class="notif-link">${tr('gh')} ↗</a>`:'';
    return `<div class="notif-item ${cls}" onclick="markSeen(${n.id})">
      <div class="notif-icon ${n.type}">${icon}</div>
      <div class="notif-body">
        <div class="notif-title">${esc(n.title)}</div>
        <div class="notif-msg">${esc(n.message)}</div>
        <div class="notif-time">${time}</div>
        ${linkHtml}
      </div>
      ${n.seen?'':'<div class="notif-dot"></div>'}
    </div>`;
  }).join('');
}

async function markSeen(id){
  await fetch('/api/notifications/'+id+'/seen',{method:'POST'});
  await loadNotifs();
  await updateNotifBadge();
}

async function markAllSeen(){
  await fetch('/api/notifications/seen-all',{method:'POST'});
  await loadNotifs();
  await updateNotifBadge();
}

async function clearNotifs(){
  if(!confirm(lang==='fa'?'حذف همه اعلانات؟':'Clear all notifications?'))return;
  await fetch('/api/notifications',{method:'DELETE'});
  await loadNotifs();
  await updateNotifBadge();
}

async function updateNotifBadge(){
  try{
    const r=await fetch('/api/notifications/count');
    if(!r.ok)return;
    const d=await r.json();
    const badge=$m('notif-badge');
    if(badge){
      if(d.count>0){badge.style.display='';badge.textContent=d.count}
      else{badge.style.display='none'}
    }
  }catch(e){}
}

async function addAddrs(){
  const lines=($m('na').value||'').trim().split('\n').map(l=>l.trim()).filter(l=>l);
  let ok=0,fail=0;
  for(const a of lines){
    if(!/^[a-zA-Z0-9\-_. ]+$/.test(a)){fail++;continue}
    try{
      const r=await fetch('/api/addresses',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({address:a})});
      if(r.ok)ok++;else fail++;
    }catch(e){fail++}
  }
  if(ok)toast('Added '+ok);
  if(fail)toast(fail+' failed',true);
  if(ok){$m('mo-addr').classList.remove('show');await loadAddrs()}
}

async function delAddr(i){
  if(!confirm('Delete this address?'))return;
  try{
    const r=await fetch('/api/addresses/'+i,{method:'DELETE'});
    if(!r.ok)throw new Error();
    toast('Deleted');await loadAddrs();
  }catch(e){toast('Error deleting',true)}
}

async function delAllAddrs(){
  if(!allAddrs||!allAddrs.length){toast('No addresses to delete',true);return}
  if(!confirm('Delete ALL clean IP addresses?'))return;
  try{
    const r=await fetch('/api/addresses',{method:'DELETE'});
    if(!r.ok)throw new Error();
    toast('All addresses deleted');await loadAddrs();
  }catch(e){toast('Error deleting',true)}
}

// همه‌ی آی‌پی‌های railway_ips.txt رو یکجا (یک درخواست، بدون تاخیر
// به‌ازای هر آی‌پی) به لیست Clean IP اضافه می‌کنه.
async function importAddrs(source){
  try{
    const r=await fetch('/api/addresses/import/'+source,{method:'POST'});
    const d=await r.json().catch(()=>null);
    if(!r.ok){toast((d&&d.detail)||'Error importing',true);return}
    toast((d.added||0)+' address(es) added, '+((d.total_in_file||0)-(d.added||0))+' already existed');
    await loadAddrs();
  }catch(e){toast('Error importing',true)}
}

setTheme(theme);
setLang('fa');

function toggleSidebar(){
  const sb=document.getElementById('sb');
  if(!sb)return;
  sb.classList.toggle('collapsed');
  document.body.classList.toggle('sb-collapsed', sb.classList.contains('collapsed'));
  localStorage.setItem('sb_collapsed', sb.classList.contains('collapsed') ? '1' : '0');
}
(function(){
  if(localStorage.getItem('sb_collapsed')==='1'){
    const sb=document.getElementById('sb');
    if(sb){sb.classList.add('collapsed');document.body.classList.add('sb-collapsed')}
  }
})();
checkAuth();
let statsInterval=null;
function startPolling(){
  if(statsInterval)clearInterval(statsInterval);
  statsInterval=setInterval(()=>{if(isAuthenticated){loadStats();loadLinks();updateNotifBadge()}},12000);
}
startPolling();

// ── Panel update notifications (checks GitHub for new releases) ────────
const PANEL_VERSION_KEY='mmd_panel_last_version';
const PANEL_GH_NOTIFIED_KEY='mmd_panel_last_notified_gh';
let loadedPanelVersion=null;

async function checkPanelVersion(isPeriodic){
  try{
    const r=await fetch('/api/version');
    if(!r.ok)return;
    const d=await r.json();
    const serverVersion=d.version;

    // Detect that this panel instance was updated since the last time we visited
    if(!loadedPanelVersion){
      loadedPanelVersion=serverVersion;
      const lastSeen=localStorage.getItem(PANEL_VERSION_KEY);
      if(lastSeen&&lastSeen!==serverVersion){
        toast('✅ Panel updated successfully to v'+serverVersion);
      }
      localStorage.setItem(PANEL_VERSION_KEY,serverVersion);
    }

    // Detect that GitHub has a newer release than what's currently running
    if(d.update_available&&d.latest_github_version){
      const alreadyNotified=localStorage.getItem(PANEL_GH_NOTIFIED_KEY);
      if(alreadyNotified!==d.latest_github_version){
        toast('🚀 New version available on GitHub: '+d.latest_github_version+' - pull the latest update');
        localStorage.setItem(PANEL_GH_NOTIFIED_KEY,d.latest_github_version);
      }
    }
  }catch(e){}
}
checkPanelVersion(false);
setInterval(()=>checkPanelVersion(true),5*60*1000);

// ====================== دکمه خروج جدید و حرفه‌ای (رنگ آبی-سفید، متن بزرگ، افکت حرفه‌ای) ======================
mo_close_html = '''
<button class="mo-close" onclick="if(confirm('آیا مطمئن هستید؟')) doLogout()" 
        style="
          position: absolute; top: 15px; right: 15px;
          background: linear-gradient(135deg, #00b4ff, #ffffff);
          color: #0a1f3d; font-size: 20px; font-weight: 700;
          width: 38px; height: 38px; border-radius: 50%;
          display: flex; align-items: center; justify-content: center;
          box-shadow: 0 5px 20px rgba(0, 180, 255, 0.5);
          transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
          z-index: 9999;
          border: none; cursor: pointer;
        ">
  ✕
</button>
'''

PANEL_HTML = PANEL_HTML.replace('</body>', mo_close_html + '</body>')

# استایل جدید برای دکمه خروج (برای اطمینان از نمایش درست)
mo_close_css = '''
/* ====================== دکمه خروج جدید ====================== */
.mo-close {
  position: absolute !important;
  top: 15px !important;
  right: 15px !important;
  background: linear-gradient(135deg, #00b4ff, #ffffff) !important;
  color: #0a1f3d !important;
  font-size: 20px !important;
  font-weight: 700 !important;
  width: 38px !important;
  height: 38px !important;
  border-radius: 50% !important;
  display: flex !important;
  align-items: center !important;
  justify-content: center !important;
  box-shadow: 0 5px 20px rgba(0, 180, 255, 0.5) !important;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
  z-index: 9999 !important;
  border: none !important;
  cursor: pointer !important;
}

.mo-close:hover {
  transform: scale(1.12) rotate(90deg) !important;
  box-shadow: 0 8px 25px rgba(0, 180, 255, 0.7) !important;
  background: linear-gradient(135deg, #ffffff, #00b4ff) !important;
}

.mo-close:active {
  transform: scale(0.95) !important;
}
'''

PANEL_HTML = PANEL_HTML.replace('</style>', mo_close_css + '</style>')

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return HTMLResponse(content=PANEL_HTML)

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    return 
