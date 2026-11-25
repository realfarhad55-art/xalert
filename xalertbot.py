
# ═══════════════════════════════════════════════════════════════════════════
# xalertbot - WizardScan X-alert POSTING bot (third split bot)
# Built by build_xalertbot.py from the Nov 23 GitHub-synced main-bot base
# copies (x_alert-3aa26821.py, core-fadf8802.py, leaderboard-28ae16b7.py,
# commands-91a03413.py). Posting pipeline functions below are extracted
# VERBATIM; the xalertbot-specific parts are marked XALERTBOT.
#
# What it does: reads monbot's queue (/opt/monbot/data/monbot_milestones.json)
# every 1s, re-checks the GUARD live, seals via the main bot (outbox +
# ack, see below), and posts the X alert with video + premium emojis +
# forwards/DMs exactly like the main bot used to.
#
# OWNERSHIP (Model B): xalertbot NEVER writes the main bot's data files.
# All bookkeeping (seals, milestone_posts, points, milestone_times, alert
# counts, hot-streak state) goes to /opt/bot/data/xalert_outbox/ as small
# JSON files; the main bot's xalert_outbox_job applies them (it stays the
# SOLE writer of its data files). xalertbot only writes its OWN files
# under /opt/xalertbot/data/ (claims, intake offset, pending-media list).
#
# Posting path: xalertbot has its OWN Telethon userbot session (a third
# Telegram account - see xalertbot-deploy.md). The main bot keeps @yowziuh
# for dip / leaderboard / Most Shared / Binance; the dropbot keeps
# @Wizard_Scan. A session allows only ONE live connection, so sharing
# @yowziuh between two processes would disconnect the main bot (the Nov 24
# saga) - that is why xalertbot does NOT use it. Without a session
# configured, xalertbot still posts via the Bot API (video captions lose
# premium emojis; text alerts keep them) and logs a loud warning.
#
# The bot token is the SAME @WIZARD_SCAN_BOT token as the main bot, used
# ONLY for direct Bot API calls (send/forward/get_me). xalertbot NEVER
# polls get_updates (that would steal updates from the main bot).
# ═══════════════════════════════════════════════════════════════════════════

import asyncio
import contextvars as _contextvars
import hashlib
import html
import json
import logging
import os
import random
import re
import requests
import sys
import threading as _threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta

from telegram import Bot as _TG_Bot
from telegram import InlineKeyboardButton, InlineKeyboardMarkup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("xalertbot")

# --- env / paths -------------------------------------------------------------
def _env(name, default=""):
    v = os.environ.get(name, default)
    return v.strip() if isinstance(v, str) else v


MAIN_DATA_DIR = _env("XALERT_MAIN_DATA_DIR", "/opt/bot/data")
XALERT_DATA_DIR = _env("XALERT_DATA_DIR", "/opt/xalertbot/data")
MONBOT_QUEUE_FILE = _env("MONBOT_QUEUE_FILE", "/opt/monbot/data/monbot_milestones.json")
XALERT_OUTBOX_DIR = os.path.join(MAIN_DATA_DIR, "xalert_outbox")
XALERT_INTAKE_FILE = os.path.join(XALERT_DATA_DIR, "intake.jsonl")
XALERT_STATE_FILE = os.path.join(XALERT_DATA_DIR, "xalert_state.json")
XALERT_PENDING_FILE = os.path.join(XALERT_DATA_DIR, "pending_media_alerts.json")

OWNER_ID = int(_env("OWNER_ID", "0") or 0)
OWNER_ID_2 = int(_env("OWNER_ID_2", "0") or 0)
TARGET_CHANNEL = "@WizardScan"

XALERTBOT_TOKEN = _env("XALERTBOT_TOKEN", "")
XALERTBOT_SESSION = _env("XALERTBOT_SESSION", "")
XALERTBOT_API_ID = int(_env("XALERTBOT_API_ID", "0") or 0)
XALERTBOT_API_HASH = _env("XALERTBOT_API_HASH", "")
# True when NO userbot session is configured at all (Bot API fallback mode).
XALERT_NO_USERBOT = not bool(XALERTBOT_SESSION)

# --- shared in-memory state (loaded read-only from the main bot's files) ---
tracked_calls = {}
sent_milestones = defaultdict(set)
milestone_posts = {}
milestone_times = {}
pending_media_alerts = defaultdict(set)
userbot_client = None
userbot_client2 = None
seen_message_ids = defaultdict(set)


def _dp(name):
    # _dp resolves against the MAIN bot's data dir (shared reads: config,
    # dm prefs, hot-streak state). xalertbot's own files use XALERT_DATA_DIR.
    return os.path.join(MAIN_DATA_DIR, name)


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d if d is not None else default
    except Exception:
        return default


def save_json(path, obj):
    # Atomic write (temp + os.replace) so a crash never leaves a half file.
    tmp = f"{path}.tmp{os.getpid()}"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


def _atomic_write_json(path, obj):
    save_json(path, obj)


def _read_json_file(path, default):
    return load_json(path, default)


# --- names the extracted pipeline needs (not safe literals in the base) ----
# Data-file paths (READ-ONLY use in xalertbot; the main bot owns the files).
DATA_DIR = MAIN_DATA_DIR
BOT_CONFIG_FILE = _dp("bot_config.json")
CALL_ARCHIVE_FILE = _dp("call_archive.json")
CHANNELS_FILE = _dp("channels.json")
CHANNEL_POINTS_FILE = _dp("channel_points.json")
CHANNEL_SUBS_FILE = _dp("channel_subs.json")
HOT_STREAK_STATE_FILE = _dp("hot_streak_state.json")
KOL_OWNERS_FILE = _dp("kol_owners.json")
LINKED_CHANNELS_FILE = _dp("linked_channels.json")
REMOVED_CHANNELS_FILE = _dp("removed_channels.json")
SUBSCRIPTIONS_FILE = _dp("subscriptions.json")
X_ACCOUNTS_FILE = _dp("x_accounts.json")

BIRDEYE_API_KEY = _env("BIRDEYE_API_KEY", "")
MORALIS_API_KEY = _env("MORALIS_API_KEY", "")
CMC_API_KEY = _env("CMC_API_KEY", "")
EXTRA_SOURCES_ENABLED = os.environ.get("EXTRA_SOURCES", "1").strip().lower() not in ("0", "false", "off")
PROMO_EMOJI_ID = 5816433179703124832

# DexScreener/Birdeye fetch state (verbatim defaults from core.py).
_DEX_CACHE_TTL = float(os.environ.get("DEX_CACHE_TTL_SECONDS", "2.5") or 2.5)
_DEX_429_COOLDOWN = int(os.environ.get("DEX_429_COOLDOWN_SECONDS", "20") or 20)
_DEX_RATE_LOCK = _threading.Lock()
_DEX_RATE_TIMESTAMPS = deque()
_DEX_MAX_REQUESTS_PER_MINUTE = max(1, int(os.environ.get("DEX_MAX_REQUESTS_PER_MINUTE", "260") or 260))
_DEX_STATS_AT = time.time()
_DEX_EFF_BUDGET = float(_DEX_MAX_REQUESTS_PER_MINUTE)
_DEX_LAST_ADJ_AT = time.time()
_DEX_ADAPTIVE = (os.environ.get("DEX_ADAPTIVE_BUDGET", "0") or "0").strip() == "1"
_DEX_LANE_CV = _contextvars.ContextVar("dex_lane", default="single")
_DEX_LANE_TS = {"bulk": deque(), "single": deque(), "mon": deque()}
_DEX_MON_SHARE = float(os.environ.get("DEX_MON_SHARE", "0.25") or 0.25)
_BIRDEYE_RATE_LOCK = _threading.Lock()
_BIRDEYE_RATE_TIMESTAMPS = deque()
_BIRDEYE_MAX_REQUESTS_PER_MINUTE = max(1, int(os.environ.get("BIRDEYE_MAX_REQUESTS_PER_MINUTE", "50") or 50))
_dex_session = requests.Session()
_MC_VALUE_BEFORE_RE = re.compile(r'(?:^|[\s>])\$?\d[\d,]*\.?\d*\s*[KMBkmb]?$')
_X_LINK_LINE_RE = re.compile(r"\n?\s*🔮\s*<a href=\"\{(?:mae|kol|bot)_link\}\">[^<]*</a>")
_MEDIA_CACHE_DIR = os.path.join(XALERT_DATA_DIR, "media_cache")
_userbot_edit_lock = asyncio.Lock()


def load_config():
    return load_json(BOT_CONFIG_FILE, {})


def record_xray_milestone(channel, x_val, row):
    return  # X-Ray feature permanently removed upstream (core.py:1092)


def _register_xalert_details(channel, ca, x_val):
    # XALERTBOT: the Details deep-link registry lives in the MAIN bot (its
    # /start handler reads _xalert_details_map()). xalertbot cannot write
    # the main bot's live map, so it queues a registration outbox record;
    # the main bot's outbox job inserts it (same sid) and saves
    # xalert_details.json. The sid is deterministic (sha256 of the call
    # key, like _register_drop_details) so the link is stable. Gap: a user
    # tapping Details within ~1s of the post (before the outbox job runs)
    # gets "no longer available" - same as a restart gap, accepted.
    call_key = f"{channel}_{ca}"
    sid = hashlib.sha256(call_key.encode("utf-8")).hexdigest()[:12]
    _xalert_outbox_write({"type": "register_details", "channel": channel,
                          "ca": ca, "x_val": x_val, "sid": sid,
                          "call_key": call_key})
    return sid



_RT_QUEUE     = None
_RT_WORKER    = None
_RT_INFLIGHT  = set()
_RT_SEQ       = 0
RT_POST_GAP   = float(os.environ.get("RT_POST_GAP", "0.12") or 0.12)
RT_MILESTONE_GAP = float(os.environ.get("RT_MILESTONE_GAP", "0.3") or 0.3)
RT_PRIO_X     = 0
RT_PRIO_DROP  = 5
RT_MAX_QUEUE_AGE_SECONDS = int(os.environ.get("RT_MAX_QUEUE_AGE_SECONDS", "600") or 600)
RT_SEND_TIMEOUT_SECONDS = int(os.environ.get("RT_SEND_TIMEOUT_SECONDS", "20") or 20)
USERBOT_SEND_TIMEOUT_SECONDS = float(os.environ.get("USERBOT_SEND_TIMEOUT_SECONDS", "6") or 6)
USERBOT_MEDIA_TIMEOUT_SECONDS = float(os.environ.get("USERBOT_MEDIA_TIMEOUT_SECONDS", "30") or 30)
XALERT_REQUIRE_MEDIA_AND_EMOJI = os.environ.get("XALERT_REQUIRE_MEDIA_AND_EMOJI", "1") not in ("0", "", "false", "False")
RT_SEND_TIMEOUT_SECONDS = max(RT_SEND_TIMEOUT_SECONDS, int(USERBOT_MEDIA_TIMEOUT_SECONDS) + 130)
EMOJI_FIX_ATTEMPTS = max(1, int(os.environ.get("EMOJI_FIX_ATTEMPTS", "3") or 3))
RT_WORKER_COUNT = max(1, int(os.environ.get("RT_WORKER_COUNT", "3") or 3))
RT_MAX_SEND_RETRIES = max(0, int(os.environ.get("RT_MAX_SEND_RETRIES", "2") or 2))
RT_GLOBAL_MIN_GAP    = float(os.environ.get("RT_GLOBAL_MIN_GAP", "3.0") or 3.0)
_RT_GLOBAL_POST_LOCK = asyncio.Lock()
_RT_LAST_POST_TS     = [0.0]
RT_MILESTONE_GLOBAL_GAP = float(os.environ.get("RT_MILESTONE_GLOBAL_GAP", "0.5") or 0.5)
RT_ORDER_WAIT_SECONDS = float(os.environ.get("RT_ORDER_WAIT_SECONDS", "30") or 25)


# --- extracted: _BADGE_CACHE ---
_BADGE_CACHE = {}

# --- extracted: _cached_badge ---
def _cached_badge(channel):
    try:
        import time as _t
        _now = _t.time()
        _key = (channel or "").lower()
        _ent = _BADGE_CACHE.get(_key)
        if _ent is not None and (_now - _ent[0]) < 60:
            return _ent[1]
        _bdg = _get_kol_badge(channel)
        _BADGE_CACHE[_key] = (_now, _bdg)
        for _k in [k for k, v in _BADGE_CACHE.items() if (_now - v[0]) > 300]:
            _BADGE_CACHE.pop(_k, None)
        return _bdg
    except Exception:
        try:
            return _get_kol_badge(channel)
        except Exception:
            return None

# --- extracted: DEFAULT_TEMPLATE ---
DEFAULT_TEMPLATE = (
    "<b>🔮 @{channel} KOL Hit {x}X+</b>\n"
    "<b>🔮 {entry}    🔮    {current}</b>\n\n"
    "Ca: <code>{ca}</code>\n\n"
    '🔮<a href="{mae_link}">MAE</a>\n'
    '🔮<a href="{kol_link}">KOL</a>\n'
    '🔮<a href="{bot_link}">BOT</a>'
)

# --- extracted: DEFAULT_X_TEMPLATE ---
DEFAULT_X_TEMPLATE = (
    "${symbol} {chain} play called at {entry} market cap. "
    "Current Market Cap stands at {current}. "
    "Clean execution. Tracking for the next move."
)

# --- extracted: X_TIER_PARAGRAPHS ---
X_TIER_PARAGRAPHS = [
    (2, 99,
        "${symbol} {chain} play called at {entry} market cap. "
        "Current Market Cap stands at {current}. "
        "Clean execution. Tracking for the next move."
    ),
    (100, 499,
        "@{channel} delivered {x}X. ${symbol} was called at a {entry}. "
        "Current MC stands at {current}. "
        "Massive move delivered. Eyes on the next milestone."
    ),
    (500, 999,
        "@{channel} printed {x}X. ${symbol} continues to exceed expectations, "
        "climbing from {entry} to {current}. "
        "Another milestone secured. Based KOL of Wizard Scan."
    ),
    (1000, 1999,
        "@{channel} nailed {x}X. ${symbol} was called at a {entry} and has "
        "now climbed to {current}. "
        "A truly elite performance with exceptional returns."
    ),
    (2000, 2999,
        "@{channel} crushed {x}X. ${symbol} was called at a {entry} and has "
        "now reached {current}. "
        "An extraordinary run that stands among the rarest performances."
    ),
    (3000, 4999,
        "@{channel} smashed {x}X. From {entry} to {current}, ${symbol} "
        "delivered a historic performance. "
        "One of the biggest moves ever tracked."
    ),
    (5000, 9999,
        "@{channel} hit {x}X. The results speak for themselves. ${symbol} "
        "climbed from {entry} to {current}, once again delivering "
        "exceptional returns. Another historic call added to the record."
    ),
    (10000, 999999999,
        "@{channel} printed {x}X. ${symbol} on {chain}, from {entry} to "
        "{current}. This is the rarest of rare calls. "
        "This is what legends are made of."
    ),
]

# --- extracted: _get_x_tier_paragraph ---
def _get_x_tier_paragraph(x_val):
    """X alert ka paragraph us X range ke hisab se (2-99X, 100-499X, ...)."""
    _exact = CHAMP_X_TEXTS.get(int(x_val))
    if _exact:
        return _exact[1]
    for lo, hi, p in X_TIER_PARAGRAPHS:
        if lo <= x_val <= hi:
            return p
    return DEFAULT_X_TEMPLATE

# --- extracted: TIERED_TEMPLATES ---
TIERED_TEMPLATES = [
    (2, 99,
        "<b>🔮 @{channel} KOL Hit {x}X+</b>\n\n"
        "${symbol} {chain} play called at {entry} market cap. "
        "Current Market Cap stands at {current}. "
        "Clean execution. Tracking for the next move.\n\n"
        "<b>🔮 {entry}    🔮    {current}</b>\n\n"
        "Ca: <code>{ca}</code>\n\n"
        '🔮<a href="{mae_link}">MAE</a>\n'
        '🔮<a href="{kol_link}">KOL</a>\n'
        '🔮<a href="{bot_link}">BOT</a>'
    ),
    (100, 499,
        "<b>🔮 SOLID KOL @{channel}</b>\n\n"
        "@{channel} delivered {x}X. ${symbol} was called at a {entry}. Current MC stands at {current}. "
        "Massive move delivered. Eyes on the next milestone.\n\n"
        "<b>🔮 {entry}  🔮  {current}</b>\n\n"
        "CA: <code>{ca}</code>\n\n"
        '🔮<a href="{mae_link}">MAE</a>\n'
        '🔮<a href="{kol_link}">KOL</a>\n'
        '🔮<a href="{bot_link}">BOT</a>'
    ),
    (500, 999,
        "<b>🔮 Apex KOL @{channel}</b>\n\n"
        "@{channel} printed {x}X. ${symbol} continues to exceed expectations, climbing from {entry} to {current}. "
        "Another milestone secured. Based KOL of Wizard Scan.\n\n"
        "<b>🔮 {entry}  🔮  {current}</b>\n\n"
        "CA: <code>{ca}</code>\n\n"
        '🔮<a href="{mae_link}">MAE</a>\n'
        '🔮<a href="{kol_link}">KOL</a>\n'
        '🔮<a href="{bot_link}">BOT</a>'
    ),
    (1000, 1999,
        "<b>🔮 ELITE KOL @{channel}</b>\n\n"
        "@{channel} nailed {x}X. ${symbol} was called at a {entry} and has now climbed to {current}. "
        "A truly elite performance with exceptional returns.\n\n"
        "<b>🔮 {entry}  🔮  {current}</b>\n\n"
        "CA: <code>{ca}</code>\n\n"
        '🔮<a href="{mae_link}">MAE</a>\n'
        '🔮<a href="{kol_link}">KOL</a>\n'
        '🔮<a href="{bot_link}">BOT</a>'
    ),
    (2000, 2999,
        "<b>🔮 RARE KOL @{channel}</b>\n\n"
        "@{channel} crushed {x}X. ${symbol} was called at a {entry} and has now reached {current}. "
        "An extraordinary run that stands among the rarest performances.\n\n"
        "<b>🔮 {entry}  🔮  {current}</b>\n\n"
        "CA: <code>{ca}</code>\n\n"
        '🔮<a href="{mae_link}">MAE</a>\n'
        '🔮<a href="{kol_link}">KOL</a>\n'
        '🔮<a href="{bot_link}">BOT</a>'
    ),
    (3000, 4999,
        "<b>🔮 EPIC KOL @{channel}</b>\n\n"
        "@{channel} smashed {x}X. From {entry} to {current}, ${symbol} delivered a historic performance. "
        "One of the biggest moves ever tracked.\n\n"
        "<b>🔮 {entry}  🔮  {current}</b>\n\n"
        "CA: <code>{ca}</code>\n\n"
        '🔮<a href="{mae_link}">MAE</a>\n'
        '🔮<a href="{kol_link}">KOL</a>\n'
        '🔮<a href="{bot_link}">BOT</a>'
    ),
    (5000, 9999,
        "<b>🔮 LEGENDARY KOL @{channel}</b>\n\n"
        "@{channel} hit {x}X. The results speak for themselves. ${symbol} climbed from {entry} to {current}, "
        "once again delivering exceptional returns. Another historic call added to the record.\n\n"
        "<b>🔮 {entry}  🔮  {current}</b>\n\n"
        "CA: <code>{ca}</code>\n\n"
        '🔮<a href="{mae_link}">MAE</a>\n'
        '🔮<a href="{kol_link}">KOL</a>\n'
        '🔮<a href="{bot_link}">BOT</a>'
    ),
]

# --- extracted: CHAMPION_TEMPLATE ---
CHAMPION_TEMPLATE = (
    "<b>🔮 HALL OF FAME @{channel}</b>\n\n"
    "@{channel} printed {x}X. ${symbol} on {chain}, from {entry} to {current}. "
    "This is the rarest of rare calls.\n\n"
    "@{channel} didn't just call it. They owned it. A masterclass in execution.\n\n"
    "This is what legends are made of. Welcome to the Hall of Fame.\n\n"
    "<b>🔮 {entry}  🔮  {current}</b>\n\n"
    "CA: <code>{ca}</code>\n\n"
    '🔮<a href="{mae_link}">MAE</a>\n'
    '🔮<a href="{kol_link}">KOL</a>\n'
    '🔮<a href="{bot_link}">BOT</a>'
)

# --- extracted: _pack_with_chart ---
def _pack_with_chart(pack):
    """Ensure the pack has a 'chart' key for the renamed 'Chart Link' text.
    The built-in packs (core.py) and the owner's saved custom packs were all
    created before the 'chart' key existed, so pack.get('chart') is None for
    them and the Chart Link emoji glitches (falls through to positional).
    'Chart Link' must show the DEX emoji, so default 'chart' to 'dex'."""
    if not pack:
        return pack
    if pack.get("chart"):
        return pack
    pack = dict(pack)
    pack["chart"] = pack.get("dex") or 0
    return pack

# --- extracted: _get_alert_pack ---
def _get_alert_pack():
    """Return current EMOJI_PACK.
    If the owner has locked a specific pack (via /setemojipack), always use that —
    this avoids the pack silently jumping around whenever channel_post_count
    changes (e.g. after a config reset). Otherwise falls back to auto-rotation
    every 10 posts based on post count."""
    config = load_config()
    locked = config.get("locked_emoji_pack")
    if locked:
        for pack in EMOJI_PACKS:
            if pack["name"] == locked:
                return _pack_with_chart(pack)
    count = config.get("channel_post_count", 0)
    return _pack_with_chart(EMOJI_PACKS[(count // 10) % len(EMOJI_PACKS)])

# --- extracted: _pack_by_name ---
def _pack_by_name(name):
    for pack in EMOJI_PACKS:
        if pack["name"] == name:
            return pack
    if name == X76_PACK["name"]:
        return X76_PACK
    return None

# --- extracted: _pack_teer ---
def _pack_teer(pack):
    """Pack ki apni teer emoji, warna locked default."""
    try:
        return int((pack or {}).get("teer") or TEER_EMOJI_ID)
    except Exception:
        return TEER_EMOJI_ID

# --- extracted: _pack_crystal ---
def _pack_crystal(pack):
    """Pack ki apni crystal-ball (entry MC) emoji, warna locked default."""
    try:
        return int((pack or {}).get("crystal") or MC_ENTRY_EMOJI_ID)
    except Exception:
        return MC_ENTRY_EMOJI_ID

# --- extracted: _custom_x_pack ---
def _custom_x_pack(x_val):
    """Owner ki khud set ki hui x-range pack (config["x_emoji_packs"]).
    If more than one saved range covers x_val (e.g. an old wide 11-99 range
    plus a newer, more specific 51-75), the NARROWEST one wins — otherwise
    whichever range was saved first always shadows a more specific one added
    later, which is why a newly-confirmed-saved pack could still never
    actually show up on real alerts."""
    try:
        best = None
        for r in load_config().get("x_emoji_packs", []):
            low, high = int(r["low"]), int(r["high"])
            if low <= int(x_val) <= high:
                if best is None or (high - low) < (int(best["high"]) - int(best["low"])):
                    best = r
        if best:
            pack = dict(best.get("pack") or {})
            if pack:
                return pack
    except Exception as _e:
        logger.warning(f"custom x emoji pack read failed: {_e}")
    return None

# --- extracted: _get_alert_pack_for_x ---
def _get_alert_pack_for_x(x_val):
    """X value ke hisaab se emoji pack.
    Priority: owner ki custom range -> built-in X_PACK_RULES (2-10 red,
    11+ blue) -> /setemojipack lock -> pehla pack."""
    try:
        xv = int(x_val)
    except Exception:
        return _get_alert_pack()
    custom = _custom_x_pack(xv)
    if custom:
        return _pack_with_chart(custom)
    for lo, hi, name in X_PACK_RULES:
        if lo <= xv <= hi:
            pack = _pack_by_name(name)
            if pack:
                return _pack_with_chart(pack)
    return _get_alert_pack()

# --- extracted: _get_alert_emoji_ids ---
def _get_alert_emoji_ids(x_val, chain, pack=None):
    """Build ordered emoji_ids list for _build_premium_entities injection.
    Order matches 🔮 placeholder positions in each template tier:
      ≤99x   : [crystal, teer, kol, bot]
      100-999x: [chain, crystal, teer, kol, bot]
      1000x+  : [chain, crystal(champ), crystal(entry), teer, kol, bot]
    Extra post links (if any) append their emoji IDs at the end.
    RH chain emoji is read from config (set via /setrobinemoji).
    """
    if pack is None:
        pack = _get_alert_pack_for_x(x_val)
    chain_key = (chain or "").upper()

    # ── Robinhood chain: config override first, then hardcoded pack emoji ─
    if chain_key == "RH":
        rh_emojis   = load_config().get("rh_chain_emojis", {})
        rh_emoji_id = rh_emojis.get(pack["name"], 0)
        # Priority: /setrobinemoji override → hardcoded pack["chain"]["RH"] → BASE fallback
        chain_emoji = (rh_emoji_id
                       or pack["chain"].get("RH")
                       or pack["chain"].get("BASE")
                       or pack["chain"].get("SOL"))
    elif chain_key == "EVM":
        # FIX: EVM (unknown EVM chain) → ETH emoji, not SOL
        chain_emoji = pack["chain"].get("ETH") or pack["chain"].get("SOL")
    else:
        chain_emoji = pack["chain"].get(chain_key) or pack["chain"].get("ETH") or pack["chain"].get("SOL")

    crystal    = _pack_crystal(pack)   # pack ki apni crystal ball (blue/red/custom)
    teer       = _pack_teer(pack)       # pack ki apni teer
    # NOTE: kol and bot_em are intentionally excluded — _build_premium_entities
    # special-cases 🔮KOL / 🔮BOT and injects pack.kol / pack.bot directly.
    # Including them here would shift badge emoji IDs into the wrong positions.
    #
    # ALL tiers use exactly 3 regular 🔮 placeholders:
    #   🔮 (header) → chain_emoji
    #   🔮 (entry MC) → crystal
    #   🔮 (separator between MCs) → teer  ← always TEER_EMOJI_ID (5346105514575025401)
    #                                         even if template is edited by owner
    # Previously 1000x+ had [chain, crystal, crystal, teer] (4 items) which caused
    # the badge 🔮 to consume teer instead of lb_star — breaking LEADERBOARD KOL label.
    base = [chain_emoji, crystal, teer]
    # NOTE: the promo-link 🔮 is NOT listed here. It is matched by text
    # (promo text follows it) inside _build_premium_entities and always gets
    # PROMO_EMOJI_ID, so positional drift can never give it the wrong emoji.

    # Append emoji IDs for extra post links (owner-configured via /addpostlink)
    extra_links = load_config().get("extra_post_links", [])
    for lnk in extra_links:
        eid = lnk.get("emoji_id")
        if eid:
            base.append(int(eid))
    return base

# --- extracted: _get_tiered_template ---
def _get_tiered_template(x_val):
    """Return tiered template string for x_val range, or None."""
    if x_val >= 10000:
        return CHAMPION_TEMPLATE
    for lo, hi, tmpl in TIERED_TEMPLATES:
        if lo <= x_val <= hi:
            return tmpl
    return None

# --- extracted: _get_range_template ---
def _get_range_template(x_val, config=None):
    """Return owner-defined custom range template for x_val, or None.
    Ranges are set via /setrangetemplate LOW HIGH and stored in config['range_templates']
    as a list of {"low":..,"high":..,"template":..}. Checked before the hardcoded
    TIERED_TEMPLATES so the owner's own per-range text always wins.
    If more than one saved range covers x_val (e.g. an old 2-9999 catch-all
    plus a newer, more specific 100-499), the NARROWEST one wins — otherwise
    whichever range happened to be saved first would always shadow a more
    specific one added later, showing its emojis/text instead."""
    config = config or load_config()
    best = None
    for r in config.get("range_templates", []):
        try:
            low, high = int(r["low"]), int(r["high"])
            if low <= x_val <= high:
                if best is None or (high - low) < (int(best["high"]) - int(best["low"])):
                    best = r
        except (KeyError, ValueError, TypeError):
            continue
    return best["template"] if best else None

# --- extracted: _get_kol_range_template ---
def _get_kol_range_template(channel, x_val, config=None):
    """Return this KOL-group's own custom range template for x_val, or None.
    Set via /setkoltemplate CHANNEL LOW HIGH. If CHANNEL is language-tagged
    (see /setkollang), the template is saved/read from the SHARED per-language
    store (config['lang_range_templates'][lang]) so setting it once applies to
    every KOL tagged with that language — not just this one channel. Untagged
    (English) channels keep the old per-channel store
    (config['channel_range_templates'][channel]) for backward compatibility.
    Checked BEFORE the global /setrangetemplate so a KOL-specific template
    always wins over the global one."""
    config = config or load_config()
    ch = (channel or "").lstrip("@").strip().lower()
    lang = (config.get("channel_language", {}) or {}).get(ch)
    if lang and lang != "english":
        source = config.get("lang_range_templates", {}).get(lang, [])
    else:
        source = config.get("channel_range_templates", {}).get(ch, [])
    best = None
    for r in source:
        try:
            low, high = int(r["low"]), int(r["high"])
            if low <= x_val <= high:
                if best is None or (high - low) < (int(best["high"]) - int(best["low"])):
                    best = r
        except (KeyError, ValueError, TypeError):
            continue
    return best["template"] if best else None

# --- extracted: _is_real_template ---
def _is_real_template(t) -> bool:
    """A saved template is only usable when it actually looks like a template.

    BUG FIX (900X): /setmedia 900 ke saath agar caption me sirf "900" ya koi
    chhota word bheja gaya to woh milestone_templates["900"] me save ho jata
    tha aur alert ka pura text sirf "900" ban jata tha. Ab aise degenerate
    entries ignore hoti hain aur bot range/tier template use karta hai."""
    if not isinstance(t, str):
        return False
    t = t.strip()
    if not t:
        return False
    if "{" in t and "}" in t:      # has real placeholders → genuine template
        return True
    if t.replace("X", "").replace("x", "").replace(",", "").strip().isdigit():
        return False               # just a number like "900" / "900X"
    return len(t) >= 40

# --- extracted: _clean_x_template ---
def _clean_x_template(tmpl):
    """Owner ke purane x_alert_template me se MAE/KOL/BOT link lines hata do —
    ab wo 6 inline buttons ban chuke hain (Trade / Caller / Details)."""
    if not tmpl:
        return None
    cleaned = _X_LINK_LINE_RE.sub("", tmpl).strip()
    return cleaned or None

# --- extracted: CHAMP_X_RANK_WORDS ---
CHAMP_X_RANK_WORDS = {
    "english":    ["One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine", "Ten"],
    "chinese":    ["一", "二", "三", "四", "五", "六", "七", "八", "九", "十"],
    "vietnamese": ["một", "hai", "ba", "bốn", "năm", "sáu", "bảy", "tám", "chín", "mười"],
    "turkish":    ["birinci", "ikinci", "üçüncü", "dördüncü", "beşinci",
                   "altıncı", "yedinci", "sekizinci", "dokuzuncu", "onuncu"],
    "russian":    ["первое", "второе", "третье", "четвёртое", "пятое",
                   "шестое", "седьмое", "восьмое", "девятое", "десятое"],
}

# --- extracted: CHAMP_X_PARAGRAPHS ---
CHAMP_X_PARAGRAPHS = {
    "english": (
        "${symbol} {chain} called at {entry}. Now at {current}. "
        "Clean Execution. Tracking For The Next Move."
    ),
    "chinese": (
        "${symbol} {chain} 在 {entry} 市值时喊单。现价市值 {current}。"
        "操作漂亮！该 KOL 在冠军 KOL 中排名第{rank}。"
    ),
    "vietnamese": (
        "${symbol} {chain} Được gọi ở vốn hóa {entry}. Hiện tại {current}. "
        "Thực hiện xuất sắc! KOL này xếp hạng số {rank} trong các KOL Champion."
    ),
    "turkish": (
        "${symbol} {chain} {entry} piyasa değerinde çağrıldı. Şu an {current}. "
        "Mükemmel uygulama! Bu KOL, Şampiyon KOL'lar arasında {rank} sırada."
    ),
    "russian": (
        "${symbol} {chain} Вызов сделан на капитализации {entry}. Сейчас {current}. "
        "Чистое исполнение! Этот KOL занимает {rank} место среди KOL-чемпионов."
    ),
}

# --- extracted: CHAMP_X_TEXTS ---
CHAMP_X_TEXTS = {
    2: ("Milestone Unlocked",
        "${symbol} is now above 2X from @{channel}'s original call."),
    5: ("@{channel} Signal Reached 5X",
        "One sharp call from @{channel}, one major move: ${symbol} climbed from {entry} to {current}."),
    10: ("@{channel} Call Smashed 10X+",
        "@{channel} called ${symbol} at {entry}. The chart now shows {current}."),
    15: ("The Call That Went 15X",
        "One call. One wild run. ${symbol} climbed from {entry} to {current} after @{channel} spotted it."),
    20: ("One Sharp Eye. One Big Move",
        "@{channel} caught ${symbol} at {entry}. It's now at {current}. Delivered a 20x profit."),
    25: ("@{channel} Hit a 25X Run",
        "${symbol} reached {current} from {entry}. The vision was real, @{channel}."),
    30: ("@{channel} Delivered 30X",
        "A bold call, a bigger run. @{channel} spotted ${symbol} at {entry}, now it's at {current}."),
    35: ("@{channel} Took It to 35X",
        "${symbol} turned @{channel}'s early call into a major move, from {entry} to {current}."),
    40: ("The 40X Run Is Real",
        "${symbol} made its way from {entry} to {current}. @{channel}, the vision was real."),
    45: ("@{channel} Just Unlocked 45X",
        "${symbol} moved from {entry} to {current}. @{channel}, take your victory lap."),
    50: ("50X on the Board. Who Called It?",
        "@{channel} did. ${symbol} moved from {entry} to {current}. That's what an early call looks like."),
    100: ("100X Conquered. Wizard Mode",
        "From @{channel}'s call at {entry} to ${symbol} reaching {current}, that's a full 100X run. The bar is set. Who's ready to raise it?"),
}

# --- extracted: _champion_x_paragraph ---
def _champion_x_paragraph(channel, badge, x_val, config=None):
    """Paragraph for a Champion KOL's X alert (2X–99X), else None."""
    try:
        if not badge or badge.get("type") != "champions":
            return None
        if not (2 <= int(x_val) <= 99):
            return None
        # Champion KOL X alerts are ALWAYS English (owner rule, Nov 11 2026).
        # The KOL's /setkollang tag is ignored here - it once rendered the
        # rejected Chinese paragraph on champion alerts (e.g. $COUSINS 2x).
        lang = "english"
        if lang == "english":
            _per_x = CHAMP_X_TEXTS.get(int(x_val))
            if _per_x:
                return _per_x[1]
        words = CHAMP_X_RANK_WORDS[lang]
        try:
            r = int(badge.get("rank") or 0)
        except (TypeError, ValueError):
            r = 0
        rank_txt = words[r - 1] if 1 <= r <= len(words) else (str(r) if r else "")
        return CHAMP_X_PARAGRAPHS[lang].replace("{rank}", rank_txt)
    except Exception:
        return None

# --- extracted: _mc_whole ---
def _mc_whole(s):
    """MC string as a whole number, no decimals: '196.4K' -> '196K',
    '224.5K' -> '224K', '1.5M' -> '1M'. Used in the Champion KOL paragraph
    only (the 🔮 entry/current line below it keeps the decimals)."""
    if not isinstance(s, str):
        return s
    import re as _re
    m = _re.match(r"^(\d+)\.\d+([KMGB]?)$", s.strip())
    if m:
        return m.group(1) + m.group(2)
    return s

# --- extracted: build_alert ---
def build_alert(channel, msg_id, x_val, chain, entry_fmt, current_fmt, ca, symbol, badge=None, x_style=False):
    kol_link = f"https://t.me/{channel}/{msg_id}" if msg_id else f"https://t.me/{channel}"
    config   = load_config()
    # Priority: 1) specific milestone template  2) KOL-specific range template
    #           3) owner-defined global range template  4) global custom (owner set via /settemplate)
    #           5) hardcoded tiered range fallback (incl. champion at 10000X)  6) default
    # NOTE: global custom (alert_template) intentionally ranks ABOVE tiered so that
    # /settemplate always overrides the built-in 100x-9999x tiered texts.
    _ms_tmpl = config.get("milestone_templates", {}).get(str(x_val))
    if not _is_real_template(_ms_tmpl):
        if _ms_tmpl:
            logger.warning(f"Ignoring broken {x_val}X template ({_ms_tmpl!r}) — using range/tier template")
        _ms_tmpl = None

    # Badge resolved up-front — used both as the {badge} template placeholder
    # (full custom templates) and for the legacy auto-wrap path below.
    if badge is None:
        badge = _cached_badge(channel)  # BADGE_FRESH_NOV18
    badge_html = ""
    _champ_badge_line = ""
    _lead_badge_line = ""
    if badge:
        if badge["type"] == "leaderboard":
            if x_style:
                _lead_badge_line = f'🔮 <b><a href="https://t.me/WizardScan/136">LEAD KOL</a></b> - Rank # 🔮'
            else:
                badge_html = f' <b><a href="https://t.me/WizardScan/136">LEAD KOL</a></b> - Rank # 🔮'
        else:
            # Champion KOL: X alert me badge ab post ke END me alag line par jata hai
            # (links ke neeche 1 khali line, phir "🔮 CHAMP KOL - Rank # 🔮").
            _cw = "https://t.me/WizardScan/134"   # Champion KOL badge link (website link hata diya)
            if x_style:
                _champ_badge_line = f'🔮 <b><a href="{_cw}">CHAMP KOL</a></b> - Rank # 🔮'
            else:
                badge_html = f' <b><a href="{_cw}">CHAMP KOL</a></b> - Rank # 🔮'

    _is_full = False
    if x_style:
        # X alert (buttons wali post). Owner ka template ab DO tarah ka ho sakta hai:
        #   a) FULL template — agar usme {caller_link}/{twitter_link}/{trade_link}/
        #      {dex_link}/{details_link}/{dev_link} me se koi placeholder ho, to
        #      poora post (header/badge/entry-current/links samet) wahi template
        #      decide karta hai — kuch bhi auto-add/wrap nahi hota.
        #   b) sirf paragraph (purana/legacy style) — is case me header, entry/current
        #      line, aur 6-link block neeche automatically wrap ho jate hain.
        # Priority: milestone template > KOL-specific range (/setkoltemplate) >
        #           /setrangetemplate (global per-range) > /editxtemplate (global) >
        #           hardcoded tier paragraph.
        _champ_para = _champion_x_paragraph(channel, badge, x_val, config)
        template = (_champ_para
                    or _clean_x_template(_ms_tmpl)
                    or _clean_x_template(_get_kol_range_template(channel, x_val, config))
                    or _clean_x_template(_get_range_template(x_val, config))
                    or _clean_x_template(config.get("x_alert_template")))
        if _champ_para:
            _is_full = False        # champion template = paragraph only, wrapper adds the rest
        elif template:
            _is_full = any(ph in template for ph in
                           ("{caller_link}", "{twitter_link}", "{trade_link}",
                            "{dex_link}", "{details_link}", "{dev_link}"))
        else:
            template = _get_x_tier_paragraph(x_val)
    else:
        template = (_ms_tmpl
                    or _get_kol_range_template(channel, x_val, config)
                    or _get_range_template(x_val, config)
                    or config.get("alert_template")
                    or _get_tiered_template(x_val)
                    or DEFAULT_TEMPLATE)
    mae_link     = f"{MAESTRO_REFLINK_BASE}{ca}{MAESTRO_REF_SUFFIX}" if ca else f"{MAESTRO_REFLINK_BASE}r-wizard_scan"
    caller_link  = _x_alert_caller_link(channel, msg_id) or f"https://t.me/{channel}"
    twitter_link = _x_alert_twitter_link(channel)
    trade_link   = _x_alert_trade_link(ca)
    dex_link     = _x_alert_dex_link(ca, chain) or "https://dexscreener.com"
    details_link = f"{BOT_LINK}?start=details_{_register_xalert_details(channel, ca, x_val)}" if ca else f"{BOT_LINK}?start=details"
    dev_link     = WIZARD_DEV_LINK
    kwargs = dict(channel=_display_handle(channel), x=x_val, symbol=(symbol or "TOKEN").upper(), chain=str(chain or "SOL").upper(),
                  entry=_mc_no_label(entry_fmt), current=_mc_no_label(current_fmt), ca=ca,
                  kol_link=kol_link, tg_link=TG_CHANNEL_LINK, bot_link=BOT_LINK,
                  mae_link=mae_link, badge=badge_html,
                  caller_link=caller_link, twitter_link=twitter_link, trade_link=trade_link,
                  dex_link=dex_link, details_link=details_link, dev_link=dev_link)
    # Champion KOL paragraph: MC as whole numbers only (196K not 196.4K),
    # and NO $ sign on MC values in the paragraph (user rule).
    # The 🔮 entry/current line below keeps the decimals (and its $).
    if _champ_para:
        kwargs["entry"] = _mc_whole(_mc_no_label(entry_fmt)).replace("$", "")
        kwargs["current"] = _mc_whole(_mc_no_label(current_fmt)).replace("$", "")
    kwargs["chain_l"] = str(chain or "SOL").upper()
    # NAMECAP_ALL_NOV17: header channel always first-letter capital.
    _cap = lambda h: (h[:1].upper() + h[1:]) if h else h
    _hdr_ch = _cap(str(channel))
    if x_style and badge and badge.get("type") != "leaderboard":
        kwargs["channel"] = _cap(str(kwargs["channel"]))
    try:
        text = safe_format(template, **kwargs)
    except Exception:
        text = safe_format(DEFAULT_X_TEMPLATE if x_style else DEFAULT_TEMPLATE, **kwargs)

    if x_style:
        if _is_full:
            # Owner's own full template already has header/badge/entry-current/links —
            # use exactly as typed, nothing auto-added or re-appended.
            pass
        else:
            # Legacy paragraph-only template → auto-wrap with the fixed structure
            #   1) header:  🔮 @channel KOL Hit xX+
            #   2) paragraph + badge inline
            #   3) 🔮 entry    🔮    current
            #   4) links: Champion KOL = 3 links only (Caller, Dexscreener,
            #      Details) on one line; everyone else = 6 links in 2 rows.
            if _champ_para:
                links_block = (
                    f'🔮<a href="{caller_link}">Caller</a>  '
                    f'🔮<a href="{dex_link}">Chart Link</a>  '
                    f'🔮<a href="{details_link}">Details</a>'
                )
            else:
                links_block = (
                    f'🔮<a href="{caller_link}">Caller</a>    '
                    f'🔮<a href="{twitter_link}">Twitter</a>  '
                    f'🔮<a href="{trade_link}">Trade</a>\n'
                    f'🔮<a href="{dex_link}">DEX</a>      '
                    f'🔮<a href="{details_link}">Details</a>   '
                    f'🔮<a href="{dev_link}">DEV</a>'
                )
            _t = CHAMP_X_TEXTS.get(int(x_val))
            if _t:
                # Header starts with a 🔮 placeholder so the emoji pack maps it
                # to the CHAIN emoji (position 1). Without it, the entry-MC 🔮
                # below wrongly takes the chain emoji and the crystal is lost.
                _hdr = f"<b>🔮 {_t[0].replace('@{channel}', '@' + str(_hdr_ch))}</b>"
            else:
                _hdr = f"<b>🔮 @{_hdr_ch} KOL Hit {x_val}X+</b>"
            text = (_hdr + "\n\n"
                    f"{text}{badge_html}\n\n"
                    f"<b>🔮{'  ' if _champ_para else ' '}{_mc_no_label(entry_fmt)}    🔮    {_mc_no_label(current_fmt)}</b>\n\n"
                    f"{links_block}")
        # 🏆 Champion KOL badge — post ke aakhir me (links ke neeche 2 khali
        # lines, phir 3rd line par badge). LEAD KOL badge stays 1 blank line.
        if _champ_badge_line and "CHAMP KOL" not in text:
            text += "\n\n\n" + _champ_badge_line
        elif _lead_badge_line and "LEAD KOL" not in text:
            text += "\n\n" + _lead_badge_line
    elif badge_html:
        text += badge_html

    # Append owner-configured extra post links
    extra_links = config.get("extra_post_links", [])
    for lnk in extra_links:
        lnk_text = html.escape(lnk.get("text", ""))
        lnk_url  = lnk.get("url", "")
        if lnk_text and lnk_url:
            text += f'\n🔮<a href="{lnk_url}">{lnk_text}</a>'

    # ── 48-hour promo link — ALWAYS the very last line ───────────────────────
    # Champion KOL / Leaderboard KOL badge aur extra links ke BAAD, ek khali
    # line chhod kar, taake post saaf aur piyari lage.
    if 2 <= x_val <= 50:
        promo = config.get("promo_link")
        if promo and promo.get("url") and promo.get("text") and promo.get("set_at"):
            try:
                if datetime.utcnow() - datetime.fromisoformat(promo["set_at"]) < timedelta(hours=48):
                    text += f'\n\n🔮 <a href="{promo["url"]}">{html.escape(promo["text"])}</a>'
            except Exception:
                pass

    return text

# --- extracted: build_x_alert ---
def build_x_alert(channel, msg_id, x_val, chain, entry_fmt, current_fmt, ca, symbol):
    """Build alert text for the X/Twitter channel (@WizardscanX)."""
    kol_link = f"https://t.me/{channel}/{msg_id}" if msg_id else f"https://t.me/{channel}"
    config   = load_config()
    template = config.get("x_alert_template") or DEFAULT_X_TEMPLATE
    mae_link = f"{MAESTRO_REFLINK_BASE}{ca}{MAESTRO_REF_SUFFIX}" if ca else f"{MAESTRO_REFLINK_BASE}r-wizard_scan"
    kwargs = dict(channel=_display_handle(channel), x=x_val, symbol=(symbol or "TOKEN").upper(), chain=chain,
                  entry=_mc_no_label(entry_fmt), current=_mc_no_label(current_fmt), ca=ca,
                  kol_link=kol_link, tg_link=TG_CHANNEL_LINK, bot_link=BOT_LINK,
                  mae_link=mae_link)
    try:
        return safe_format(template, **kwargs)
    except Exception:
        return safe_format(DEFAULT_X_TEMPLATE, **kwargs)

# --- extracted: _x_alert_caller_link ---
def _x_alert_caller_link(channel, msg_id):
    """Us post ka direct link jo track ki gayi thi."""
    ch = (channel or "").lstrip("@").strip()
    if not ch:
        return None
    if msg_id:
        return f"https://t.me/{ch}/{msg_id}"
    return f"https://t.me/{ch}"

# --- extracted: _x_alert_twitter_link ---
def _x_alert_twitter_link(channel):
    """KOL ka X link (agar /addx se set hai), warna WizardScan ka X."""
    try:
        handle = _get_channel_x_handle(channel)
    except Exception:
        handle = None
    if handle:
        return f"https://x.com/{handle.lstrip('@')}"
    return WIZARD_X_FALLBACK_LINK

# --- extracted: _x_alert_trade_link ---
def _x_alert_trade_link(ca):
    if ca:
        return f"{MAESTRO_REFLINK_BASE}{ca}{MAESTRO_REF_SUFFIX}"
    return MAESTRO_PLAIN_REFLINK

# --- extracted: _x_alert_dex_link ---
def _x_alert_dex_link(ca, chain):
    if not ca:
        return None
    path = CHAIN_TO_DEXPATH.get((chain or "").upper(), "solana")
    return f"https://dexscreener.com/{path}/{ca}"

# --- extracted: build_x_alert_keyboard ---
def build_x_alert_keyboard(channel, msg_id, ca, chain, x_val=None):
    """Caller/Twitter/Trade/DEX/Details/DEV ab inline KEYBOARD buttons nahi hain —
    ye 6 links ab build_alert() ke andar hi post TEXT me 🔮 emoji ke saath render
    hote hain (Telegram button labels premium emoji support nahi karte, is liye
    text hi sahi jagah hai). Ye function ab sirf owner ke /addbutton wale
    milestone-specific extra buttons (agar hon) return karta hai."""
    rows = []
    if x_val is not None:
        extra = load_config().get("milestone_buttons", {}).get(str(x_val), [])
        for b in extra:
            if b.get("text") and b.get("url"):
                rows.append([InlineKeyboardButton(b["text"], url=b["url"])])
    return InlineKeyboardMarkup(rows) if rows else None

# --- extracted: _BOT_CHANNEL_FORBIDDEN ---
_BOT_CHANNEL_FORBIDDEN = {"v": False}

# --- extracted: _ptb_keyboard_to_tl ---
def _ptb_keyboard_to_tl(keyboard):
    """PTB InlineKeyboardMarkup -> Telethon buttons (url buttons only)."""
    if not keyboard:
        return None
    try:
        from telethon.tl.types import ReplyInlineMarkup, KeyboardButtonRow, KeyboardButtonUrl
        rows = []
        for row in keyboard.inline_keyboard:
            btns = [KeyboardButtonUrl(text=b.text, url=b.url) for b in row if b.url]
            if btns:
                rows.append(KeyboardButtonRow(buttons=btns))
        return ReplyInlineMarkup(rows=rows) if rows else None
    except Exception:
        return None

# --- extracted: _fix_alert_emojis_then_forward ---
async def _fix_alert_emojis_then_forward(bot, channel, x_val, post_id, text, emoji_id,
                                         pack, keyboard):
    """Post Bot API se (bina premium emoji) gayi thi -> userbot usay EDIT karke
    premium emojis lagata hai. Edit ho jane ke BAAD bot (userbot nahi) post ko
    channel owner / subscribers ko forward karta hai. Edit fail ho to bhi forward
    hota hai taake koi alert miss na ho."""
    fixed = False
    try:
        from telethon.extensions.html import parse as tl_html_parse
        _txt, _explicit = prepare_owner_emojis(text)
        plain_text, base_ents = tl_html_parse(_txt)
        all_ents = _build_premium_entities(plain_text, base_ents, emoji_id,
                                           forced_pack=pack, explicit=_explicit)
        tl_buttons = _ptb_keyboard_to_tl(keyboard)

        # Kuch send paths mein bot ka message_id 0 report hota hai (logs: msg_id=0)
        # -> channel ki recent posts mein se wahi post dhoondo (text match).
        if not post_id and userbot_client:
            try:
                recent = await asyncio.wait_for(
                    userbot_client.get_messages(TARGET_CHANNEL, limit=10), timeout=15)
                for m in recent or []:
                    if (getattr(m, "message", "") or "").strip() == plain_text.strip():
                        post_id = m.id
                        logger.info(f"🔎 Emoji-fix: bot post id resolved via userbot: {post_id}")
                        break
            except Exception as e_find:
                logger.warning(f"Emoji-fix: post id lookup failed: {e_find}")

        if post_id and userbot_client:
            for attempt in range(1, EMOJI_FIX_ATTEMPTS + 1):
                try:
                    await asyncio.wait_for(
                        _locked_userbot_edit(
                            TARGET_CHANNEL, int(post_id), plain_text,
                            formatting_entities=all_ents,
                            buttons=tl_buttons, link_preview=False),
                        timeout=20)
                    fixed = True
                    logger.info(f"✅ Emoji-fix: premium emojis added by userbot edit "
                                f"(post {post_id}, {x_val}x @{channel}, attempt {attempt})")
                    break
                except Exception as e_edit:
                    # "message not modified" = emojis pehle se lage hain
                    if "not modified" in str(e_edit).lower():
                        fixed = True
                        break
                    logger.warning(f"Emoji-fix edit attempt {attempt}/{EMOJI_FIX_ATTEMPTS} "
                                   f"failed (post {post_id}): {type(e_edit).__name__}: {e_edit}")
                    await asyncio.sleep(2 * attempt)
        elif not post_id:
            logger.warning(f"Emoji-fix skipped: post id unknown ({x_val}x @{channel})")
    except Exception as e:
        logger.error(f"Emoji-fix failed unexpectedly: {e}")

    if not fixed:
        logger.error(f"❌ Emoji-fix could not add premium emojis to post {post_id} "
                     f"({x_val}x @{channel}) — forwarding as-is")
    # Edit ke baad BOT forward karta hai (forward_message, userbot kabhi nahi).
    if post_id:
        await _forward_alert_recipients(bot, channel, x_val, post_id)
    else:
        logger.error(f"Forward skipped: post id unknown ({x_val}x @{channel})")

# --- extracted: _champion_video_stats ---
def _champion_video_stats(channel, badge):
    calls = get_call_history(channel)
    total = len(calls)
    alpha = sum(
        1 for c in calls
        if (c.get("current_x", c.get("highest_x", 0)) or 0) >= 2
        or (c.get("highest_x", 0) or 0) >= 2
    )
    win = round((alpha / total) * 100) if total else 0
    return {
        "rank":     badge.get("rank", 1),
        "points":   get_channel_points(channel),
        "calls":    total,
        "win_rate": f"{win}%",
    }

# --- extracted: _delete_champion_video ---
def _delete_champion_video(path):
    """Delete a generated champion video from the cache after posting.
    The video is per-alert (channel + X), so once posted it is never reused —
    deleting it stops the cache filling up with old videos."""
    try:
        if path and os.path.exists(path):
            os.remove(path)
            logger.info(f"🎬 Champion video deleted from cache: {path}")
    except Exception as e:
        logger.warning(f"🎬 Champion video cache delete failed {path}: {e}")

# --- extracted: _champion_video_try_send ---
async def _champion_video_try_send(bot, channel, x_val, chain, entry_fmt, current_fmt,
                                   ca, symbol, badge, text, keyboard, emoji_id, pack):
    """Returns (posted, post_id, needs_emoji_fix). Never raises."""
    # Champion video by X (Nov 11): 2/5/10 gauge (red), 20 rocket (blue),
    # 30 cyber slam (orange), 40+ green trio (40 matrix green, 50 orbit green,
    # 60-90 area purple, 100+ area bw). Other champion X
    # (8/15/22/25/28/35/45/55/65/75/85/95) has no video —
    # the alert still posts with normal X media (or parks if none is set).
    path = None
    try:
        import champion_video
        stats = _champion_video_stats(channel, badge)
        _av = os.path.join(_dp("avatars"), f"{str(channel).lstrip('@').lower()}.jpg")
        avatar = _av if os.path.exists(_av) else None
        # CHAMPION_VIDEO_TRIO_NOV11: video ONLY at CHAMPION_VIDEO_LEVELS
        # (2/5/10 gauge red, 20 rocket blue, 30 cyber slam orange,
        # 40+ green trio: 40 matrix green, 50 orbit green, 60-90 area purple,
        # 100+ area bw). Other champion levels return False here ->
        # normal X media path below.
        if x_val in (2, 5, 10):
            _bp = None
            try:
                import time as _time
                _tier = champion_video.tier_for(x_val)
                _cand = champion_video.base_path_for(_tier, x_val)
                if os.path.exists(_cand) and (_time.time() - os.path.getmtime(_cand)) < 1800:
                    _bp = _cand
            except Exception:
                _bp = None
            path = await champion_video.champion_alert_video(
                channel=channel, x_val=x_val, chain=chain, entry_fmt=entry_fmt,
                cur_fmt=current_fmt, ca=ca, symbol=symbol, stats=stats,
                avatar_path=avatar, base_path=_bp)
        elif x_val == 20:
            entry_mc = champion_video._parse_mc(entry_fmt)
            now_mc = champion_video._parse_mc(current_fmt)
            if entry_mc and not now_mc:
                now_mc = entry_mc * float(x_val)
            elif now_mc and not entry_mc:
                entry_mc = now_mc / float(x_val)
            import time as _time
            _out_dir = os.environ.get("CHAMP_VIDEO_DIR", "/tmp")
            os.makedirs(_out_dir, exist_ok=True)
            out_path = os.path.join(
                _out_dir,
                f"champ-rocket-{int(x_val)}x-{int(_time.time() * 1000) % 100000}.mp4")
            import champion_rocket_neon
            # ROCKET_CYBER_PRERENDER_NOV12: use a pre-rendered rocket base
            # when one exists and is fresh (warm path ~14s); else cold render.
            _bp = None
            try:
                _cand = champion_rocket_neon.rocket_base_path_for("blue", x_val)
                if os.path.exists(_cand) and (_time.time() - os.path.getmtime(_cand)) < 1800:
                    _bp = _cand
            except Exception:
                _bp = None
            _kw = dict(kol_name=channel, x_val=x_val, symbol=symbol, chain=chain,
                       entry_mc=entry_mc, now_mc=now_mc, rank=stats.get("rank", 1),
                       points=stats.get("points", 0), calls=stats.get("calls", 0),
                       win_rate=stats.get("win_rate", "0%"), avatar_path=avatar,
                       out_path=out_path, crf=18, preset="veryfast", base_path=_bp)
            path = await asyncio.to_thread(
                champion_rocket_neon.build_rocket_video, **_kw)
        elif x_val == 30:
            entry_mc = champion_video._parse_mc(entry_fmt)
            now_mc = champion_video._parse_mc(current_fmt)
            if entry_mc and not now_mc:
                now_mc = entry_mc * float(x_val)
            elif now_mc and not entry_mc:
                entry_mc = now_mc / float(x_val)
            import time as _time
            _out_dir = os.environ.get("CHAMP_VIDEO_DIR", "/tmp")
            os.makedirs(_out_dir, exist_ok=True)
            out_path = os.path.join(
                _out_dir,
                f"champ-cyber-30x-{int(_time.time() * 1000) % 100000}.mp4")
            import champion_cyberhud_orange
            # ROCKET_CYBER_PRERENDER_NOV12: use a pre-rendered cyber base
            # when one exists and is fresh (warm path ~15s); else cold render.
            _bp = None
            try:
                _cand = champion_cyberhud_orange.cyber_base_path_for("orange", x_val)
                if os.path.exists(_cand) and (_time.time() - os.path.getmtime(_cand)) < 1800:
                    _bp = _cand
            except Exception:
                _bp = None
            _kw = dict(kol_name=channel, x_val=x_val, symbol=symbol, chain=chain,
                       entry_mc=entry_mc, now_mc=now_mc, rank=stats.get("rank", 1),
                       points=stats.get("points", 0), calls=stats.get("calls", 0),
                       win_rate=stats.get("win_rate", "0%"), avatar_path=avatar,
                       out_path=out_path, crf=18, preset="veryfast", base_path=_bp)
            path = await asyncio.to_thread(
                champion_cyberhud_orange.build_cyber_video, entry="slam", **_kw)
        elif x_val in CHAMPION_VIDEO_LEVELS and x_val >= 40:
            # # CHAMPION_TRIO_ROUTING_NOV11: green trio renderer.
            # 40 matrix green, 50 orbit green, 60 area purple,
            # 80/90 area GREEN (old theme, Nov 14 2026 owner ref video),
            # 100+ area bw.
            if x_val == 40:
                _design, _color = "matrix", "green"
            elif x_val == 50:
                _design, _color = "orbit", "green"
            elif x_val == 60:
                _design, _color = "area", "purple"
            elif x_val in (80, 90):
                # OLD_THEME_80_90_NOV14: 80x/90x use the green area look
                # from the owner's reference 80x video, not purple.
                _design, _color = "area", "green"
            else:  # 100+
                _design, _color = "area", "bw"
            entry_mc = champion_video._parse_mc(entry_fmt)
            now_mc = champion_video._parse_mc(current_fmt)
            if entry_mc and not now_mc:
                now_mc = entry_mc * float(x_val)
            elif now_mc and not entry_mc:
                entry_mc = now_mc / float(x_val)
            import time as _time
            _out_dir = os.environ.get("CHAMP_VIDEO_DIR", "/tmp")
            os.makedirs(_out_dir, exist_ok=True)
            out_path = os.path.join(
                _out_dir,
                f"champ-trio-{int(x_val)}x-{int(_time.time() * 1000) % 100000}.mp4")
            import champion_green_trio
            # TRIO_PRERENDER_NOV12: use a pre-rendered trio base when one
            # exists and is fresh (warm path ~16s); else cold render.
            _bp = None
            try:
                _cand = champion_green_trio.trio_base_path_for(_design, _color, x_val)
                if os.path.exists(_cand) and (_time.time() - os.path.getmtime(_cand)) < 1800:
                    _bp = _cand
            except Exception:
                _bp = None
            _kw = dict(design=_design, x_val=x_val, color=_color,
                       kol_name=channel, symbol=symbol, chain=chain,
                       entry_mc=entry_mc, now_mc=now_mc, rank=stats.get("rank", 1),
                       points=stats.get("points", 0), calls=stats.get("calls", 0),
                       win_rate=stats.get("win_rate", "0%"), avatar_path=avatar,
                       out_path=out_path, crf=18, preset="veryfast", base_path=_bp)
            path = await asyncio.to_thread(
                champion_green_trio.build_green_video, **_kw)
        else:
            return False, None, False
    except Exception as e:
        logger.warning(f"🎬 Champion video build error @{channel} {x_val}x: {e}")
        path = None
    if not path or not os.path.exists(path):
        logger.warning(f"🎬 Champion video unavailable @{channel} {x_val}x — normal alert will be sent")
        return False, None, False

    # 1) Userbot: video + premium emojis + buttons ek hi send me (no edit/pencil)
    # Round-robin across 2 accounts + failover
    _ub_clients = _userbot_clients()
    if _ub_clients and emoji_id:
        try:
            from telethon.extensions.html import parse as tl_html_parse
            from telethon.tl.types import DocumentAttributeVideo
            _txt, _explicit = prepare_owner_emojis(text)
            plain_text, base_ents = tl_html_parse(_txt)
            if len(plain_text) <= 1024:
                all_ents = _build_premium_entities(plain_text, base_ents, emoji_id,
                                                   forced_pack=pack, explicit=_explicit)
                _picked = _pick_userbot()
                _ordered = [_picked] + [cl for cl in _ub_clients if cl is not _picked]
                sent = None
                for _ub in _ordered:
                    try:
                        sent = await asyncio.wait_for(
                            _ub.send_file(
                                TARGET_CHANNEL, path, caption=plain_text,
                                formatting_entities=all_ents,
                                buttons=_ptb_keyboard_to_tl(keyboard),
                                attributes=[DocumentAttributeVideo(
                                    duration=7, w=1920, h=820, supports_streaming=True)],
                                supports_streaming=True, force_document=False),
                            timeout=USERBOT_MEDIA_TIMEOUT_SECONDS)
                        break
                    except Exception as _se:
                        logger.warning(f"🎬 Champion video send failed on one client: {_se}")
                        continue
                if sent is not None:
                    logger.info(f"🎬 Champion video sent by userbot: @{channel} {x_val}x "
                                f"rank={badge.get('rank')} msg_id={sent.id}")
                    _delete_champion_video(path)
                    return True, sent.id, False
        except Exception as e:
            logger.warning(f"🎬 Champion video userbot send failed @{channel}: "
                           f"{type(e).__name__}: {e} — trying Bot API")

    # 2) Bot API fallback (premium emoji baad me userbot edit se lagte hain)
    if not _BOT_CHANNEL_FORBIDDEN["v"]:
        try:
            with open(path, "rb") as fh:
                m = await bot.send_video(
                    TARGET_CHANNEL, video=fh, caption=text, parse_mode="HTML",
                    reply_markup=keyboard, width=1920, height=820, duration=7,
                    supports_streaming=True)
            logger.info(f"🎬 Champion video sent by bot: @{channel} {x_val}x "
                        f"rank={badge.get('rank')} msg_id={m.message_id}")
            _delete_champion_video(path)
            return True, m.message_id, True
        except Exception as e:
            if "not a member" in str(e).lower() or "forbidden" in str(e).lower():
                _BOT_CHANNEL_FORBIDDEN["v"] = True
            logger.warning(f"🎬 Champion video bot send failed @{channel}: {e}")
    # All sends failed — delete the generated video so the cache does not pile up.
    _delete_champion_video(path)
    return False, None, False

# --- extracted: LEAD_RANK_EMOJIS_BY_X ---
LEAD_RANK_EMOJIS_BY_X = [
    (2, 10, {1: 6003466361060204218, 2: 6003544855882507005, 3: 6003802382121576201,
             4: 6003372958406419088, 5: 6003539444223714524, 6: 6001139639837007865,
             7: 6003814021482946755, 8: 6003640152616869707, 9: 6003461881409314873,
             10: 6003415865129706096}),
    (11, 20, {1: 6003712935132668091, 2: 6003823483295902552, 3: 6003319602027700134,
              4: 6003709550698437970, 5: 6005864413100253272, 6: 6003678480905018574,
              7: 6003633508302462775, 8: 6003556319150219127, 9: 6003556263315644328,
              10: 6003351685433400404}),
    (21, 30, {1: 6003703962945986556, 2: 6003620271213256999, 3: 6003849974654181680,
              4: 6003837815601765906, 5: 6005836732036030447, 6: 6003710379627128033,
              7: 6003421603206012387, 8: 6003515285032673025, 9: 6003470870775868095,
              10: 6003813256978768360}),
    (31, 50, {1: 6003660871539106191, 2: 6003383322162503610, 3: 6003732004787462339,
              4: 6003564831775399193, 5: 6003469878638419892, 6: 6003483755677752686,
              7: 6003504032218358258, 8: 6005661969816755771, 9: 6003479529429935720,
              10: 6003568083065643002}),
    (51, 75, {1: 6003682500994408226, 2: 6003375406537779003, 3: 6003787044793359987,
              4: 6005716356987624820, 5: 6003303139418054451, 6: 6003598006102793987,
              7: 6003753226220871999, 8: 6003701089612866659, 9: 6003459875659587126,
              10: 6003543004751601164}),
    (76, 99, {1: 6003739568224872602, 2: 6003573456069729832, 3: 6006078358306169726,
              4: 6003664797139213731, 5: 6003343443391159673, 6: 6003470853595996353,
              7: 6003303998411513679, 8: 6003507150364614091, 9: 6005815519192554646,
              10: 6003631820380315667}),
]

# --- extracted: _lead_rank_emoji ---
def _lead_rank_emoji(x_val, rank):
    """LEAD KOL badge ka rank-number emoji, X range ke color set se.
    100x+ pe standard leaderboard set."""
    try:
        xv = int(x_val)
        r = int(rank or 1)
    except Exception:
        return LEADERBOARD_PREMIUM_EMOJIS.get(1)
    for lo, hi, emojis in LEAD_RANK_EMOJIS_BY_X:
        if lo <= xv <= hi:
            return emojis.get(r, emojis.get(1))
    return LEADERBOARD_PREMIUM_EMOJIS.get(r, LEADERBOARD_PREMIUM_EMOJIS[1])

# --- extracted: LEAD_BADGE_CRYSTAL_BY_X ---
LEAD_BADGE_CRYSTAL_BY_X = [
    (51, 75, 5823481461554028957),  # purple theme
]

# --- extracted: _lead_badge_crystal ---
def _lead_badge_crystal(x_val, pack):
    """LEAD KOL badge ki pehli emoji = MC line wali crystal, X range ke color me."""
    try:
        xv = int(x_val)
    except Exception:
        return _pack_crystal(pack)
    for lo, hi, eid in LEAD_BADGE_CRYSTAL_BY_X:
        if lo <= xv <= hi:
            return eid
    return _pack_crystal(pack)

# --- extracted: send_alert ---
async def send_alert(bot, channel, msg_id, x_val, chain, entry_fmt, current_fmt, ca, symbol,
                     force=False, record_only=False):
    config   = load_config()
    media    = config.get("milestone_media",{}).get(str(x_val)) or config.get("milestone_media",{}).get("global")
    # Deleted-post calls (DELETE_DM_VIDEO_NOV16): X alerts ALWAYS post now.
    # _blocked_deleted only gates points + leaderboard refresh below, and
    # _is_deleted_blocked() is True only when the owner tapped "No".
    _blocked_deleted = bool(ca) and _is_deleted_blocked(tracked_calls.get(f"{channel}_{ca}"))
    # Compute badge once, pass to build_alert + emoji list
    badge    = _cached_badge(channel)  # BADGE_FRESH_NOV18
    # Champion KOLs: only 2x, 5x, 8x, 10x get a posted alert (with video).
    # Any other X (3x, 4x, 6x, 7x, 9x, 12x, ...) is recorded (points /
    # rankings) but NOT posted for a champion. Normal KOLs are unaffected.
    if (badge and badge.get("type") == "champions"
            and x_val not in CHAMPION_MILESTONES):
        record_only = True
    # ── REAL-TIME POST RULE ───────────────────────────────────────────────────
    # X alerts must NEVER be held just because media is missing. If media is
    # configured, use it; otherwise send the same alert as text immediately.
    # This is important on Railway/restarts where a missing media file must not
    # turn a verified X into a delayed alert. record_only remains silent by design.
    if record_only:
        _call_key = f"{channel}_{ca}" if ca else None
        try:
            record_xray_milestone(channel, x_val, {
                "symbol":  symbol or "TOKEN",
                "chain":   chain or "",
                "entry":   entry_fmt or "?",
                "ms_mc":   current_fmt or "?",
                "ca":      ca or "",
                "post_id": None,
                "at":      datetime.utcnow().isoformat(),
            })
        except Exception as _e_xr:
            logger.warning(f"xray archive write failed (silent): {_e_xr}")
        if _call_key and not _blocked_deleted:
            try:
                await award_points_for_milestone(channel, _call_key, x_val)
            except Exception as _e_pt:
                logger.warning(f"points award failed (silent): {_e_pt}")
        # Ranking refresh: recorded-only milestones bhi leaderboard/champions me
        # foran count hone chahiye (owner requirement).
        if x_val >= 2 and not _blocked_deleted:
            try:
                asyncio.create_task(_update_leaderboard_with_premium_emojis(bot))
                asyncio.create_task(_update_champions_with_premium_emojis(bot))
            except Exception:
                pass
        logger.info(f"⏸️ {x_val}X @{channel} recorded only — no channel post")
        return "silent"

    # (badge already computed above, before the record_only check)
    text     = build_alert(channel, msg_id, x_val, chain, entry_fmt, current_fmt, ca, symbol,
                           badge=badge, x_style=True)
    keyboard = build_x_alert_keyboard(channel, msg_id, ca, chain, x_val=x_val)
    # Pack-based premium emojis — X value decide karti hai kaun sa pack:
    #   2X-10X -> red (default) · 11X+ -> blue · owner ki custom ranges upar.
    pack     = _get_alert_pack_for_x(x_val)
    emoji_id = list(_get_alert_emoji_ids(x_val, chain, pack))
    # Append badge premium emoji IDs — leaderboard: [pack crystal, rank number]
    # (crystal = MC line wali hi, X range ke color me), champion: [lead, rank].
    if badge:
        if badge["type"] == "leaderboard":
            # Dono emojis X range ke theme color me:
            # pehli = MC line wali crystal, dusri = usi color ka rank number
            rank_emoji_id = _lead_rank_emoji(x_val, badge["rank"])
            emoji_id += [_lead_badge_crystal(x_val, pack), rank_emoji_id]
        else:
            rank_emoji_id = CHAMPIONS_PREMIUM_EMOJIS.get(badge["rank"], CHAMPIONS_PREMIUM_EMOJIS[1])
            emoji_id += [globals().get("CHAMP_BADGE_LEAD_EMOJI_ID") or 5823636561413021540,
                         rank_emoji_id]
    logger.info(f"Alert emoji_id for {x_val}x chain={chain} pack={pack['name']}: {emoji_id} | userbot={userbot_client is not None}")

    # ── Send to main channel ─────────────────────────────────────────────────────
    # PATTERN: Single send — NO edit after posting.
    # Edit = Telegram shows pencil "edited" mark, which we do NOT want on alerts.
    # Priority:
    #   1. Userbot sends media directly with premium emojis (no edit, no pencil)
    #   2. Bot sends media without emojis  (no edit, no pencil)
    #   3. Userbot sends text with emojis  (no edit, no pencil)
    #   4. Bot sends text only             (no edit, no pencil)
    posted         = False
    wizard_post_id = None
    call_key       = f"{channel}_{ca}" if ca else None
    # True jab post Bot API se (bina premium emoji) gayi — tab userbot edit karke
    # emojis lagayega aur uske baad BOT forward karega.
    needs_emoji_fix = False

    # ── 🎬 Champion KOL HD video (sirf badge type "champions") ───────────────
    # Lead KOL / baaqi KOLs yahan se guzar jaate hain, kuch nahi badalta.
    # Video na bane to posted False rehta hai -> neeche normal alert jati hai.
    if badge and badge.get("type") == "champions":
        _cv_ok, _cv_id, _cv_fix = await _champion_video_try_send(
            bot, channel, x_val, chain, entry_fmt, current_fmt, ca, symbol,
            badge, text, keyboard, emoji_id, pack)
        if _cv_ok:
            posted          = True
            wizard_post_id  = _cv_id
            needs_emoji_fix = _cv_fix
    # (d) fallback visibility: a champion alert whose video failed (or
    # which has no video level) AND no normal media exists will park in
    # the hold branch below — the warning there names the level.
    _cv_failed = bool(badge and badge.get("type") == "champions" and not posted)

    # `force=True` is the real-time path.  Keep media in realtime alerts, but
    # use the Telegram Bot API/file_id first.  This reuses Telegram's stored
    # media instantly and avoids the slow userbot download -> re-upload path.
    # Normal alerts still prefer the userbot so premium emoji entities remain.
    media_fail_reason = None  # XALERTBOT: init here, the HOLD branch
    # below reads it even when no media is set (base only inits
    # it inside this block).
    if not posted and media and media.get("file_id"):
        fid, ftype = media["file_id"], media.get("type", "photo")

        # Instant path: Bot API can send an existing file_id without copying
        # the file through Railway.  This preserves media and keeps latency low.
        # NOTE: only take this path when userbot/emoji isn't available — bot
        # accounts can't attach premium emoji entities, and editing afterward
        # to add them shows Telegram's pencil "edited" mark. When userbot +
        # emoji_id ARE available we skip straight to the userbot media-send
        # path below, which attaches the emojis in the original send (no edit,
        # no pencil), even though it's a touch slower than the instant path.
        if force and not _BOT_CHANNEL_FORBIDDEN["v"] and not (userbot_client and emoji_id):
            try:
                if ftype == "photo":
                    sent_msg = await bot.send_photo(
                        TARGET_CHANNEL, photo=fid, caption=text,
                        parse_mode="HTML", reply_markup=keyboard)
                else:
                    sent_msg = await bot.send_video(
                        TARGET_CHANNEL, video=fid, caption=text,
                        parse_mode="HTML", reply_markup=keyboard)
                posted         = True
                wizard_post_id = sent_msg.message_id
                logger.info(f"✅ Instant {x_val}x media sent by bot API (no userbot/emoji available): msg_id={wizard_post_id}")
            except Exception as e:
                # Bot channel ka member/admin nahi hai -> har alert par ye 403
                # round-trip waqt zaya karta tha. Ek dafa pata chal jaye to
                # seedha userbot path use karo (aur owner ko batao).
                if "not a member" in str(e).lower() or "forbidden" in str(e).lower():
                    _BOT_CHANNEL_FORBIDDEN["v"] = True
                    logger.error("🚫 Bot is NOT admin in " + str(TARGET_CHANNEL) +
                                 " — bot API media path disabled, using userbot. "
                                 "Fix: add the bot as admin in the channel for the fastest path.")
                logger.warning(f"Instant media send failed, trying userbot: {e}")

        # Normal path: userbot sends media + premium emojis in one shot.
        # DIAG/FIX: single 6s attempt was too short under real load (job is
        # already overloaded — see monitoring_job hard-timeout in logs — so a
        # slightly slow userbot response used to fall straight through to the
        # bot-fallback below, which is GUARANTEED to fail if the bot isn't
        # admin in TARGET_CHANNEL, landing on text-only. One retry gives a
        # transient stall a second chance before we give up on media.
        media_fail_reason = None
        if not posted and userbot_client and emoji_id:
            # FIX (2026-10-15): pehle 10s x 2 attempts the — pehli attempt cancel
            # hone par upload dobara shuru hota tha (aur kai dafa upload 10.4s mein
            # complete hone wala hota tha). Ab EK attempt, lamba timeout; upload
            # jaldi ho jaye to foran aage barh jata hai.
            for attempt in (1,):
                try:
                    sent_msg = await asyncio.wait_for(
                        _userbot_send_media_with_emoji(
                            bot, TARGET_CHANNEL, fid, ftype, text, emoji_id,
                            keyboard=keyboard, forced_pack=pack),
                        timeout=USERBOT_MEDIA_TIMEOUT_SECONDS)
                except asyncio.TimeoutError:
                    sent_msg = None
                    media_fail_reason = f"userbot media timed out after {USERBOT_MEDIA_TIMEOUT_SECONDS}s (attempt {attempt})"
                    logger.warning(f"{media_fail_reason}; continuing to Bot API fallback")
                    break
                if sent_msg:
                    posted = True
                    try: wizard_post_id = sent_msg.id
                    except Exception: pass
                    logger.info(f"✅ Alert {x_val}x sent by userbot (no pencil): msg_id={wizard_post_id}")
                break
        elif not posted:
            media_fail_reason = "userbot_client not connected" if not userbot_client else "no emoji_id"

        # NOTE (2026-11-07): Bot API media fallback yahan se hata diya.
        # Jab userbot media fail ho (e.g. milestone_media file_id expire),
        # pehle userbot TEXT (premium emojis ke saath) try hota hai neeche
        # Option 3 me. Bot API media (bina premium emoji) sirf uske baad
        # aata hai (Option 3b), taake premium emojis na khoyen.

    # HOLD (2026-11-07, v2): media+premium emoji ke baghair post hi na ho.
    # Userbot media na chali to yahin ruk jao, Option 3/3b/4 (text ya
    # bina-emoji media) par mat jao. v2: alert ko pending_media_alerts me
    # park karo, taake pending_media_job (har 15s) use dobara try kare.
    # Monitoring ne milestone pehle hi seal kar diya hota hai, is liye
    # "agle tick" retry nahi hoti, pending queue hi asal retry hai.
    # Media pehle se set ho (transient fail) to late post ho jayegi.
    # XALERTBOT: bypass the hold ONLY when media IS set but this bot
    # has no userbot session configured at all (XALERT_NO_USERBOT) -
    # then the Bot API media fallback below posts (caption without
    # premium emojis) instead of parking forever. Session configured
    # (even if transiently disconnected) or no media set: hold stays.
    if not posted and XALERT_REQUIRE_MEDIA_AND_EMOJI and not (media and media.get("file_id") and XALERT_NO_USERBOT):
        if _cv_failed and not (media and media.get("file_id")):
            if x_val in CHAMPION_VIDEO_LEVELS:
                _why = "champion video failed"
            else:
                _why = (f"no video at {x_val}x by design "
                        f"(video levels are 2/5/10/20/30/40/50/60/70/80/90/100+)")
            logger.warning(
                f"Champion @{channel} {x_val}x: {_why} and no normal media set — "
                f"alert HELD. Set media via /setmedia {x_val} to release it.")
        try:
            _add_pending_media(call_key, x_val)
        except Exception as _e_pm:
            logger.warning(f"pending_media park failed {x_val}x {call_key}: {_e_pm}")
        logger.info(
            f"Alert {x_val}x HELD, parked in pending_media_alerts "
            f"(reason: {media_fail_reason or 'no media/userbot/emoji'}). "
            f"pending_media_job will retry every 15s.")
        return False

    if not posted:
        # Option 3: Text-only — userbot sends with premium emojis.
        # Same one-retry pattern as the media path above: a single transient
        # stall (flood-wait, brief reconnect) shouldn't drop straight to the
        # Bot API fallback, which cannot attach premium emoji entities at all.
        if userbot_client and emoji_id:
            for attempt in (1, 2):
                try:
                    sent_msg = await asyncio.wait_for(
                        _userbot_send_with_premium_emoji(
                            TARGET_CHANNEL, text, emoji_id=emoji_id,
                            keyboard=keyboard, forced_pack=pack),
                        timeout=USERBOT_SEND_TIMEOUT_SECONDS)
                except asyncio.TimeoutError:
                    sent_msg = None
                    if attempt == 1:
                        logger.warning(
                            f"Userbot text send timed out after {USERBOT_SEND_TIMEOUT_SECONDS}s "
                            "(attempt 1), retrying once")
                        continue
                    logger.warning(
                        f"Userbot text send timed out after {USERBOT_SEND_TIMEOUT_SECONDS}s "
                        "(attempt 2); continuing to Bot API fallback (no premium emoji)")
                    break
                if sent_msg:
                    posted = True
                    try: wizard_post_id = sent_msg.id
                    except Exception: pass
                    logger.info(f"Alert {x_val}x text sent by userbot with premium emojis")
                break

        # Option 3b: Bot API media fallback — sirf jab userbot text bhi
        # fail ho jaye. Media wapas laata hai lekin premium emojis nahi
        # (Bot API limit). Ye Option 3 (userbot text) ke baad aata hai
        # taake premium emojis ko priority mile.
        if not posted and media and media.get("file_id"):
            try:
                _fb_fid, _fb_ftype = media["file_id"], media.get("type", "photo")
                if _fb_ftype == "photo":
                    sent_msg = await bot.send_photo(
                        TARGET_CHANNEL, photo=_fb_fid, caption=text,
                        parse_mode="HTML", reply_markup=keyboard)
                else:
                    sent_msg = await bot.send_video(
                        TARGET_CHANNEL, video=_fb_fid, caption=text,
                        parse_mode="HTML", reply_markup=keyboard)
                posted = True
                wizard_post_id = sent_msg.message_id
                needs_emoji_fix = True
                logger.info(f"Alert {x_val}x media sent by bot (no emojis): msg_id={wizard_post_id}")
            except Exception as e:
                if "not a member" in str(e).lower() or "forbidden" in str(e).lower():
                    _BOT_CHANNEL_FORBIDDEN["v"] = True
                logger.error(f"Bot API media fallback failed: {e}")

        # Option 4: Bot sends text (final fallback, no edit)
        if not posted:
            try:
                sent_msg = await bot.send_message(
                    chat_id=TARGET_CHANNEL, text=text,
                    parse_mode="HTML", disable_web_page_preview=True,
                    reply_markup=keyboard)
                posted = True
                try: wizard_post_id = sent_msg.message_id
                except Exception: pass
                needs_emoji_fix = True
                logger.info(f"Alert {x_val}x text sent by bot (text fallback)")
            except Exception as e:
                logger.error(f"Text alert to channel failed: {e}")

    # The caller persists the milestone only after a confirmed Telegram send.
    # This makes transient Telegram/userbot failures retry on the next tick.
    if not posted:
        return False

    # DM delivery is scheduled IMMEDIATELY after Telegram confirms the original
    # @WizardScan post.  It must not depend on milestone-file writes, X-Ray,
    # points, counters, promo jobs, or any other bookkeeping below: any one of
    # those can fail independently and previously prevented every DM forward.
    # `_forward_alert_recipients` uses forward_message only.
    if needs_emoji_fix and userbot_client and emoji_id:
        # Post bina premium emoji ke gayi -> pehle userbot edit se emojis lagao,
        # PHIR bot forward kare (forward original ka snapshot leta hai, isliye
        # edit se pehle forward karna non-premium copy bhej deta).
        asyncio.create_task(
            _fix_alert_emojis_then_forward(
                bot, channel, x_val, wizard_post_id, text, emoji_id, pack, keyboard),
            name=f"emoji-fix-{channel}-{x_val}-{wizard_post_id}")
    elif wizard_post_id:
        asyncio.create_task(
            _forward_alert_recipients(bot, channel, x_val, wizard_post_id),
            name=f"forward-alert-{channel}-{x_val}-{wizard_post_id}")

    # Save WizardScan post ID for this milestone
    if wizard_post_id and call_key:
        if call_key not in milestone_posts:
            milestone_posts[call_key] = {}
        milestone_posts[call_key][str(x_val)] = wizard_post_id
        _save_milestone_posts()

    # Permanent X-Ray record for this channel + milestone
    try:
        record_xray_milestone(channel, x_val, {
            "symbol":  symbol or "TOKEN",
            "chain":   chain or "",
            "entry":   entry_fmt or "?",
            "ms_mc":   current_fmt or "?",
            "ca":      ca or "",
            "post_id": wizard_post_id,
            "at":      datetime.utcnow().isoformat(),
        })
    except Exception as _e_xr:
        logger.warning(f"xray archive write failed: {_e_xr}")

    # Award points to channel for this milestone (champion kols points system)
    if call_key:
        awarded = await award_points_for_milestone(channel, call_key, x_val)
        if awarded > 0:
            logger.info(f"⭐ Points awarded: +{awarded} to @{channel} for {x_val}X milestone")

    # Track post count
    count = _inc_channel_post_count()
    # Hashtag every 100 alerts
    if count % 100 == 0:
        asyncio.create_task(_post_hashtag_to_channel(bot))
    # Promo post every 25 alerts (if enabled)
    if count % 25 == 0:
        promo_cfg = load_config()
        if promo_cfg.get("promo_enabled"):
            asyncio.create_task(_post_promo_to_channel(bot))

    return True

# --- extracted: _ADMIN_CH_CACHE ---
_ADMIN_CH_CACHE = {}

# --- extracted: _ADMIN_CH_CACHE_TTL ---
_ADMIN_CH_CACHE_TTL = 1800

# --- extracted: _bot_is_admin_in_channel ---
async def _bot_is_admin_in_channel(bot, channel: str) -> bool:
    if not channel:
        return False
    key = channel.lower()
    cached = _ADMIN_CH_CACHE.get(key)
    if cached:
        # Positive result 30 min cache; negative sirf 2 min (taake owner ke
        # bot ko admin banate hi agli alert forward ho jaye).
        ttl = _ADMIN_CH_CACHE_TTL if cached[0] else 120
        if (time.time() - cached[1]) < ttl:
            return cached[0]
    is_admin = False
    try:
        me = await bot.get_me()
        cm = await bot.get_chat_member(f"@{channel}", me.id)
        is_admin = getattr(cm, "status", "") in ("administrator", "creator")
    except Exception as e:
        logger.debug(f"admin check failed for @{channel}: {e}")
        is_admin = False
    _ADMIN_CH_CACHE[key] = (is_admin, time.time())
    return is_admin

# --- extracted: _TARGET_CH_ACCESS ---
_TARGET_CH_ACCESS = {"ok": True, "last_check": 0.0, "last_warn": 0.0}

# --- extracted: _TARGET_CH_RECHECK_INTERVAL ---
_TARGET_CH_RECHECK_INTERVAL = 300

# --- extracted: _TARGET_CH_WARN_INTERVAL ---
_TARGET_CH_WARN_INTERVAL    = 600

# --- extracted: _ensure_bot_can_read_target_channel ---
async def _ensure_bot_can_read_target_channel(bot) -> bool:
    """Returns True if the bot account itself is currently able to read/forward
    from TARGET_CHANNEL. Cheap positive-cache; frequent recheck while broken."""
    now = time.time()
    if _TARGET_CH_ACCESS["ok"] and (now - _TARGET_CH_ACCESS["last_check"]) < _ADMIN_CH_CACHE_TTL:
        return True
    if not _TARGET_CH_ACCESS["ok"] and (now - _TARGET_CH_ACCESS["last_check"]) < _TARGET_CH_RECHECK_INTERVAL:
        return False
    ok = False
    try:
        me = await bot.get_me()
        cm = await bot.get_chat_member(TARGET_CHANNEL, me.id)
        ok = getattr(cm, "status", "") in ("administrator", "creator", "member")
    except Exception as e:
        logger.debug(f"target-channel access check failed: {e}")
        ok = False
    was_ok = _TARGET_CH_ACCESS["ok"]
    _TARGET_CH_ACCESS["ok"] = ok
    _TARGET_CH_ACCESS["last_check"] = now
    if ok and not was_ok:
        logger.info(f"✅ Bot access to {TARGET_CHANNEL} restored — DM/channel forwards resuming.")
        _BOT_CHANNEL_FORBIDDEN["v"] = False
    return ok

# --- extracted: _forward_alert_post ---
async def _forward_alert_post(bot, chat_id, wizard_post_id, label="") -> bool:
    """ALWAYS forward (never copy/paste) so premium emojis survive.
    Bot API only: retry, then return False — caller must NOT fall back to
    copy_message or a different template; it simply skips that recipient.
    Never use the owner/Thomas userbot for user DMs or KOL channels."""
    if not wizard_post_id:
        return False

    # DIAG/FIX: fail fast + loudly instead of burning 3 retries per recipient
    # on a failure mode that is known to affect ALL recipients equally.
    if not await _ensure_bot_can_read_target_channel(bot):
        now = time.time()
        if (now - _TARGET_CH_ACCESS["last_warn"]) > _TARGET_CH_WARN_INTERVAL:
            _TARGET_CH_ACCESS["last_warn"] = now
            logger.error(
                f"🚫 DM/channel forwards are ALL failing: the bot account is not "
                f"an admin/member of {TARGET_CHANNEL}. forward_message requires "
                f"the bot itself (not the userbot) to have access to the source "
                f"channel. FIX: add the bot as admin in {TARGET_CHANNEL} — "
                f"forwarding will resume automatically within {_TARGET_CH_RECHECK_INTERVAL}s, "
                f"no restart needed.")
        return False

    last_err = None
    for attempt in range(3):
        try:
            await bot.forward_message(chat_id=chat_id,
                                      from_chat_id=TARGET_CHANNEL,
                                      message_id=int(wizard_post_id))
            logger.info(f"✅ Forwarded alert post {wizard_post_id} → {chat_id} {label}")
            return True
        except Exception as e:
            last_err = e
            msg = str(e).lower()
            if "flood" in msg:
                await asyncio.sleep(2 + attempt * 2)
                continue
            if any(k in msg for k in ("chat not found", "bot was kicked", "not enough rights",
                                      "forbidden", "chat_write_forbidden", "blocked")):
                break
            await asyncio.sleep(0.6)
    logger.warning(
        f"Bot-API forward → {chat_id} failed ({last_err}); "
        "owner userbot fallback is disabled"
    )
    return False

# --- extracted: _auto_forward_to_own_channel ---
async def _auto_forward_to_own_channel(bot, channel, wizard_post_id, already_linked: set):
    """Forward the just-posted alert into the KOL's own channel if the bot
    has admin/post rights there — skipped if /linkme already set this up
    (that loop already forwards it, avoids a duplicate post)."""
    if not channel or not wizard_post_id:
        return
    if channel.lower() in already_linked:
        return
    try:
        if not await _bot_is_admin_in_channel(bot, channel):
            logger.debug(f"skip self-forward @{channel}: bot not admin")
            return
        ok = await _forward_alert_post(bot, f"@{channel}", wizard_post_id,
                                       label="(own channel)")
        if not ok:
            logger.warning(f"Auto self-channel forward @{channel} failed on all paths")
    except Exception as e:
        logger.warning(f"Auto self-channel forward @{channel} failed: {e}")

# --- extracted: SMART_LINK_MILESTONES ---
SMART_LINK_MILESTONES = {5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}

# --- extracted: _smart_link_milestone_qualifies ---
def _smart_link_milestone_qualifies(x_val) -> bool:
    try:
        xv = float(x_val)
    except (TypeError, ValueError):
        return True  # fail-open — never silently eat an alert on a bad value
    if xv > 100:
        return True
    return int(round(xv)) in SMART_LINK_MILESTONES

# --- extracted: _forward_alert_recipients ---
async def _forward_alert_recipients(bot, channel, x_val, wizard_post_id):
    """Forward the already-published WizardScan post without rebuilding it.

    Forwarding preserves Telegram's premium custom-emoji entities.  All
    recipients are handled concurrently and subscriber IDs are normalized to
    int, which also repairs old JSON files that stored IDs as strings.
    """
    try:
        channel_key = (channel or "").lstrip("@").lower()
        owner_id = load_kol_owners().get(channel_key)
        try:
            owner_id = int(owner_id) if owner_id is not None else None
        except (TypeError, ValueError):
            owner_id = None

        linked = load_linked_channels()
        destinations = []
        for kol_ch, link_val in linked.items():
            if str(kol_ch).lstrip("@").lower() != channel_key:
                continue
            # /linkme stores a plain "@channel" string (forward everything).
            # DM_ALERT_SETTINGS4_NOV25: /linksmart's "smart" tier gate is
            # retired (the channel's Channel Alert choice is now the single
            # source of truth). /linkme stores a plain "@channel" string,
            # /linksmart stores {"target": "@channel", "mode": "smart"} -
            # both are read the same way now.
            if isinstance(link_val, dict):
                target = link_val.get("target")
            else:
                target = link_val
            if not target:
                continue
            if not channel_alert_x_allows(str(target).lstrip("@").lower(), x_val):
                continue
            destinations.append((target, f"({x_val}X linked)"))

        # Auto-forward to the KOL's own channel, if bot has admin rights.
        # DM_ALERT_SETTINGS4_NOV25: gated by the channel's Channel Alert
        # choice (default all, so nothing changes until a choice is set).
        if channel_alert_x_allows(channel_key, x_val):
            try:
                await _auto_forward_to_own_channel(
                    bot, channel_key, wizard_post_id,
                    already_linked={
                        str(k).lstrip("@").lower() for k in linked.keys()
                    })
            except Exception as e:
                logger.warning(f"auto self-forward failed @{channel_key}: {e}")

        if owner_id:
            destinations.append((owner_id, f"(KOL owner @{channel_key} {x_val}X)"))

        # Channel-specific subscribers are the primary /subscribe path.
        specific = load_channel_subs().get(channel_key, []) or []
        # Keep legacy global subscribers working too, without duplicates.
        global_subs = load_subscriptions() or []
        seen = set()
        forwards = []
        for uid in list(specific) + list(global_subs):
            try:
                uid = int(uid)
            except (TypeError, ValueError):
                continue
            if uid == owner_id or uid in seen:
                continue
            seen.add(uid)
            label = f"(sub @{channel_key})" if uid in {
                int(x) for x in specific
                if str(x).lstrip("-").isdigit()
            } else "(global sub)"
            destinations.append((uid, label))

        async def _one(chat_id, label):
            # Forward only. Never copy, rebuild, or send a link/template.
            ok = await _forward_alert_post(bot, chat_id, wizard_post_id, label=label)
            if not ok:
                logger.error(
                    f"Alert forward failed: chat={chat_id} "
                    f"post={wizard_post_id} @{channel_key} {x_val}X")



        # DM_ALERT_SETTINGS3_NOV25: each DM recipient only gets the X levels
        # they chose for THIS channel (default all). Channel forwards
        # (str targets) pass through.
        destinations = [(c, l) for (c, l) in destinations
                        if not isinstance(c, int) or _dm_alert_x_allows(c, channel_key, x_val)]

        await asyncio.gather(*[_one(chat_id, label) for chat_id, label in destinations],
                             return_exceptions=True)
    except Exception as e:
        logger.error(f"_forward_alert_recipients failed @{channel}: {e}")

# --- extracted: HOT_STREAK_MIN ---
HOT_STREAK_MIN = 4

# --- extracted: HOT_STREAK_MAX ---
HOT_STREAK_MAX = 9

# --- extracted: HOT_STREAK_COLORS ---
HOT_STREAK_COLORS = ["red", "blue", "green", "golden"]

# --- extracted: HOT_STREAK_PACKS ---
HOT_STREAK_PACKS = {
    "red": {
        "checks": [5879457242041819253, 5879776611514981971, 5879906920822742529,
                   5879485524401464691, 5879857764922041309, 5879678952548605515,
                   5879591528489296028],
        "emoji": 5922462341554774161,
    },
    "blue": {
        "checks": [5879779519207842656, 5879792571613454694, 5879898081780048072,
                   5879856991827927106, 5879983920996425347, 5879776126183678163,
                   5879773811196305347],
        "emoji": 5920335246116527693,
    },
    "green": {
        "checks": [5879596051089858109, 5879559402133921572, 5879776366701845939,
                   5879673716983472905, 5879956209867431448, 5879477827820068149,
                   5879661618060600315],
        "emoji": 5922350041044886140,
    },
    "golden": {
        "checks": [5879553921755652107, 5879835753214648278, 5879893314366349311,
                   5879903128366620392, 5879693164595388798, 5879972659592174922,
                   5879645576357748539],
        "emoji": 5922570858198474764,
    },
}

# --- extracted: _HS_LOCK ---
_HS_LOCK = None

# --- extracted: _hs_lock ---
def _hs_lock():
    global _HS_LOCK
    if _HS_LOCK is None:
        _HS_LOCK = asyncio.Lock()
    return _HS_LOCK

# --- extracted: _HS_RECENT_POSTS ---
_HS_RECENT_POSTS = {}

# --- extracted: _hs_is_duplicate ---
def _hs_is_duplicate(ch, hits):
    keys = frozenset(r.get("call_key") for r in hits)
    now = time.time()
    for k in [k for k, (_, t) in _HS_RECENT_POSTS.items() if now - t > 900]:
        _HS_RECENT_POSTS.pop(k, None)
    prev = _HS_RECENT_POSTS.get(ch)
    if prev and (prev[0] & keys):
        return True
    _HS_RECENT_POSTS[ch] = (keys, now)
    return False

# --- extracted: _hs_snapshot_link_info ---
def _hs_snapshot_link_info(row, call_key):
    """Freeze channel/msg_id into the streak row so the CALL link never goes
    empty after the call is archived / removed from tracked_calls."""
    call = tracked_calls.get(call_key, {}) or {}
    if not call:
        arch = (load_call_archive() or {}).get(call_key) or {}
        call = arch if isinstance(arch, dict) else {}
    ch = str(call.get("channel") or "").lstrip("@")
    mid = str(call.get("msg_id") or "").strip()
    if ch and mid and mid not in ("0", "None"):
        row["src_channel"] = ch
        row["src_msg_id"] = mid
    posts = milestone_posts.get(call_key, {}) or {}
    pid = posts.get("2") or posts.get("2.0") or (list(posts.values())[0] if posts else None)
    if pid:
        row["wiz_post_id"] = pid

# --- extracted: _HS_SETTLED_MAX ---
_HS_SETTLED_MAX = 5000

# --- extracted: _hs_settle ---
def _hs_settle(state, rows):
    g = state.setdefault("_global", {})
    lst = g.setdefault("settled", [])
    have = set(lst)
    for r in rows:
        k = r.get("call_key")
        if k and k not in have:
            lst.append(k)
            have.add(k)
    if len(lst) > _HS_SETTLED_MAX:
        del lst[:len(lst) - _HS_SETTLED_MAX]

# --- extracted: _hs_unsettle ---
def _hs_unsettle(state, rows):
    g = state.setdefault("_global", {})
    drop = {r.get("call_key") for r in rows}
    g["settled"] = [k for k in g.get("settled", []) if k not in drop]

# --- extracted: _hs_channel_key ---
def _hs_channel_key(channel):
    return (channel or "").lstrip("@").lower().strip()

# --- extracted: _hs_next_color ---
def _hs_next_color(state):
    """Rotate: red -> blue -> green -> golden -> red -> ..."""
    idx = int((state.get("_global") or {}).get("color_idx", 0) or 0)
    color = HOT_STREAK_COLORS[idx % 4]
    state.setdefault("_global", {})["color_idx"] = idx + 1
    return color

# --- extracted: _hs_on_call_hit ---
async def _hs_on_call_hit(bot, channel, call_key, x_val):
    """Hook: call right after a call's 2X+ milestone alert is successfully sent."""
    try:
        if not call_key or x_val < 2:
            return
        ch = _hs_channel_key(channel)
        if not ch:
            return
        async with _hs_lock():
            state = _hs_load()
            if call_key in set((state.get("_global") or {}).get("settled", [])):
                return   # already counted in a streak / declared dead — never reuse
            entry = state.setdefault(ch, {"seq": []})
            row = next((r for r in entry["seq"] if r.get("call_key") == call_key), None)
            if row is not None and row.get("status") == "dead":
                return   # dead call hitting 2X late must NOT join a streak
            if row is None:
                row = {"call_key": call_key,
                       "chain": (tracked_calls.get(call_key, {}) or {}).get("chain", "SOL"),
                       "status": "pending", "peak_x": 0.0,
                       "registered": datetime.utcnow().isoformat()}
                entry["seq"].append(row)
            # OWNER FIX (2026-09-24, "RH calls showing SOL emoji" bug): the row's
            # chain used to be frozen at whatever _hs_register_call guessed the
            # moment the call was FIRST seen — usually the generic "EVM" bucket,
            # since chain detection only resolves to a specific chain (RH/ETH/
            # BNB/BASE) later once the live dex data comes in. "EVM" isn't a key
            # in HOT_STREAK_PACKS[...]["chains"], so _hs_chain_emoji_for() always
            # fell back to the SOL emoji for every non-SOL EVM-family call,
            # including RH. By the time a call actually HITS 2X (here), its
            # tracked_calls chain has long since been resolved to the real value
            # — so re-sync it into the streak row every hit, not just on first
            # registration.
            resolved_chain = (tracked_calls.get(call_key, {}) or {}).get("chain")
            if resolved_chain and resolved_chain.upper() != "EVM":
                row["chain"] = resolved_chain.upper()
            row["status"] = "hit"
            row["hit_at"] = datetime.utcnow().isoformat()
            try:
                row["peak_x"] = max(float(row.get("peak_x", 0) or 0), float(x_val))
            except (TypeError, ValueError):
                row["peak_x"] = float(x_val)
            try:
                _hs_snapshot_link_info(row, call_key)
            except Exception:
                pass
            _hs_save(state)
        await _hs_evaluate(bot, ch)
    except Exception as e:
        logger.warning(f"hot-streak hit-hook failed @{channel}: {e}")

# --- extracted: _hs_restore_claimed ---
async def _hs_restore_claimed(ch, claimed):
    """Post failed — put the claimed streak back at the front of the sequence."""
    async with _hs_lock():
        state = _hs_load()
        entry = state.setdefault(ch, {"seq": []})
        have = {r.get("call_key") for r in entry["seq"]}
        entry["seq"] = [r for r in claimed if r.get("call_key") not in have] + entry["seq"]
        _hs_unsettle(state, claimed)
        _hs_save(state)

# --- extracted: _hs_evaluate ---
async def _hs_evaluate(bot, channel):
    """Core streak state machine — hold a 3..9-call streak until it either
    caps at 9, or the call right after it is confirmed dead; then post+reset.

    FIX (2026-10-07): the streak is CLAIMED (removed from state and saved)
    under the lock BEFORE the alert is sent, so concurrent evaluations can
    never see the same streak twice."""
    ch = _hs_channel_key(channel)
    if not ch:
        return
    while True:
        to_post = None
        async with _hs_lock():
            state = _hs_load()
            entry = state.get(ch)
            if not entry:
                return
            seq = entry.get("seq", [])
            hits = []
            i = 0
            while i < len(seq) and seq[i].get("status") == "hit":
                hits.append(seq[i])
                i += 1

            if len(hits) >= HOT_STREAK_MAX:
                to_post = hits[:HOT_STREAK_MAX]
                entry["seq"] = seq[HOT_STREAK_MAX:]
                _hs_settle(state, to_post)
                _hs_save(state)
            elif i >= len(seq):
                return  # no next call registered yet — keep holding
            else:
                nxt = seq[i]
                if nxt.get("status") != "dead":
                    return  # next call still pending -> keep holding
                if len(hits) >= HOT_STREAK_MIN:
                    to_post = hits
                # Streak ends here. Hits shorter than MIN are discarded too
                # (they never count again), and the dead call is settled.
                _hs_settle(state, hits + [nxt])
                entry["seq"] = seq[i + 1:]
                _hs_save(state)
        if to_post:
            res = await _hs_post_alert(bot, ch, to_post)
            if res is None:
                await _hs_restore_claimed(ch, to_post)
                return

# --- extracted: _hs_chain_emoji_for ---
def _hs_chain_emoji_for(pack, chain=None):
    """OWNER CHANGE (2026-10-07): per-chain emojis removed. Every CALL line now
    uses ONE emoji — the streak color's own emoji (red/green/golden/blue)."""
    return f'<tg-emoji emoji-id="{pack["emoji"]}">🔥</tg-emoji>'

# --- extracted: _hs_chain_ids_in_order ---
def _hs_chain_ids_in_order(pack, hits):
    """Kept for compatibility. The CALL emojis are explicit <tg-emoji> tags in
    the text, so no positional IDs are needed any more."""
    return []

# --- extracted: _hs_banner_html ---
def _hs_banner_html(pack):
    """OWNER FIX (2026-09-23): the 7 IDs under pack['checks'] are NOT
    per-call checkmarks — they're 7 slices of one wide 'HOT STREAK ALERT'
    banner graphic that only looks right when all 7 render back-to-back in
    a single row. The old code sprinkled one per CALL line instead, which
    is why the banner showed up as broken letter-fragments ('A', 'OT',
    'ST'...) scattered next to random calls. Join them here, all 7 in a
    row, as the post's title graphic instead."""
    return "".join(f'<tg-emoji emoji-id="{cid}">🔥</tg-emoji>' for cid in pack["checks"])

# --- extracted: _hs_call_link ---
def _hs_call_link(row):
    """FIX (2026-09-30 + 2026-10-07): every CALL must be a link. Order:
    1) live @WizardScan milestone post, 2) wizard post id frozen in the row,
    3) live tracked call's original post, 4) channel/msg_id frozen in the row,
    5) archive. The frozen copies stop links going empty once a call is
    archived / dropped from tracked_calls."""
    ck = row.get("call_key")
    posts = milestone_posts.get(ck, {}) or {}
    pid = (posts.get("2") or posts.get("2.0") or (list(posts.values())[0] if posts else None)
           or row.get("wiz_post_id"))
    if pid:
        return f'https://t.me/{TARGET_CHANNEL.lstrip("@")}/{pid}'
    call = tracked_calls.get(ck, {}) or {}
    ch = str(call.get("channel") or row.get("src_channel") or "").lstrip("@")
    mid = str(call.get("msg_id") or row.get("src_msg_id") or "").strip()
    if not (ch and mid) or mid in ("0", "None"):
        arch = (load_call_archive() or {}).get(ck) or {}
        if isinstance(arch, dict):
            ch = str(arch.get("channel") or "").lstrip("@")
            mid = str(arch.get("msg_id") or "").strip()
    if ch and mid and mid not in ("0", "None"):
        return f"https://t.me/{ch}/{mid}"
    return None

# --- extracted: _hs_build_text ---
def _hs_build_text(channel_key, hits, color):
    pack = HOT_STREAK_PACKS[color]
    n = len(hits)
    lines_call = []
    for idx, row in enumerate(hits):
        post_url = _hs_call_link(row)
        chain_e = _hs_chain_emoji_for(pack)
        label = f"CALL {idx + 1}"
        if post_url:
            label = f'<a href="{post_url}">{label}</a>'
        # Color emoji goes BEFORE the label: "<emoji> CALL 1".
        lines_call.append(f"{chain_e} {label}")

    rows_txt = []
    for a in range(0, len(lines_call), 2):
        rows_txt.append("     ".join(lines_call[a:a + 2]))

    best_idx, best_row = max(enumerate(hits), key=lambda t: float(t[1].get("peak_x", 0) or 0))
    best_x = float(best_row.get("peak_x", 0) or 0)
    # OWNER FIX (2026-09-23): test alerts must show a fixed "TestChannel"
    # placeholder, not a real KOL's handle — _display_handle also lowercases
    # everything after the first letter (bigbangcall -> Bigbangcall), which
    # doesn't match "TestChannel"'s exact capitalization either.
    ch_display = "TestChannel" if channel_key.lower() == "testchannel" else _display_handle(channel_key)

    _best_url = _hs_call_link(best_row)
    _best_label = f"CALL {best_idx + 1}"
    if _best_url:
        _best_label = f'<a href="{_best_url}">{_best_label}</a>'
    return (
        _hs_banner_html(pack) + "\n\n"
        f"<b>@{ch_display} delivered {n} consecutive 2X+ calls.\n"
        f"No breaks. No dead calls.</b>\n\n"
        + "\n".join(rows_txt) +
        f"\n\n<b>This KOL is worth following</b>\n"
        f"{n} Wins | Avg {fmt_x(best_x)} ({_best_label})"
    )

# --- extracted: _hs_can_pin ---
async def _hs_can_pin(bot, chat_id, is_channel):
    try:
        if not is_channel:
            return True  # a bot may pin in its own private chat with a user
        me = await bot.get_me()
        cm = await bot.get_chat_member(chat_id, me.id)
        return bool(getattr(cm, "can_pin_messages", False)) or getattr(cm, "status", "") == "creator"
    except Exception:
        return False

# --- extracted: _hs_forward_and_pin ---
async def _hs_forward_and_pin(bot, channel_key, wizard_post_id):
    """Forward the streak post to the KOL owner's DM (auto-pin there), and —
    only if the bot is admin AND has pin rights — forward + pin it in the
    KOL's own channel too. Never posts anywhere the bot lacks rights for."""
    try:
        owner_id = load_kol_owners().get(channel_key)
        try:
            owner_id = int(owner_id) if owner_id is not None else None
        except (TypeError, ValueError):
            owner_id = None
        if owner_id:
            try:
                fwd = await bot.forward_message(chat_id=owner_id, from_chat_id=TARGET_CHANNEL,
                                                message_id=int(wizard_post_id))
                await bot.pin_chat_message(chat_id=owner_id, message_id=fwd.message_id,
                                           disable_notification=True)
            except Exception as e:
                logger.debug(f"hot-streak DM forward/pin skipped @{channel_key}: {e}")

        if await _bot_is_admin_in_channel(bot, channel_key):
            try:
                fwd_ch = await bot.forward_message(chat_id=f"@{channel_key}", from_chat_id=TARGET_CHANNEL,
                                                    message_id=int(wizard_post_id))
                if await _hs_can_pin(bot, f"@{channel_key}", is_channel=True):
                    await bot.pin_chat_message(chat_id=f"@{channel_key}", message_id=fwd_ch.message_id,
                                               disable_notification=True)
            except Exception as e:
                logger.debug(f"hot-streak channel forward/pin skipped @{channel_key}: {e}")
    except Exception as e:
        logger.warning(f"hot-streak forward/pin failed @{channel_key}: {e}")

# --- extracted: _hs_post_alert ---
async def _hs_post_alert(bot, channel_key, hits, forced_color=None):
    """Build + send a Hot Streak Alert to TARGET_CHANNEL, then forward/pin it
    to the KOL owner DM and (if allowed) the KOL's own channel."""
    try:
        if not forced_color and _hs_is_duplicate(channel_key, hits):
            logger.warning(f"🔥 hot-streak DUPLICATE blocked @{channel_key} ({len(hits)} calls)")
            return 0   # falsy but not None -> caller must NOT restore/retry
        state = _hs_load()
        color = forced_color or _hs_next_color(state)
        _hs_save(state)

        # OWNER FIX (2026-09-23): custom/premium emoji ARE supported on
        # inline button labels via icon_custom_emoji_id (Bot API 9.4+,
        # already used elsewhere in this bot via _pkb()) — corrected after
        # initially saying otherwise. Using the theme-specific "View KOL"
        # icon IDs the owner provided, via the same safe-fallback pattern.
        VIEW_KOL_ICON_IDS = {
            "red":    5816433179703124832,
            "golden": 5823636561413021540,
            "blue":   5438132790983942059,
            "green":  5821002775797965247,
        }
        text = _hs_build_text(channel_key, hits, color)
        kb = InlineKeyboardMarkup([[_icon_kb(
            "View KOL", VIEW_KOL_ICON_IDS.get(color), url=_x_alert_caller_link(channel_key, None))]])

        pack = HOT_STREAK_PACKS[color]
        emoji_ids = list(pack["checks"]) + _hs_chain_ids_in_order(pack, hits)
        media = (cfg_get("hot_streak_media", {}) or {}).get(color)

        # (Superseded by the 2026-09-23b fix below: Bot API does NOT actually
        # render <tg-emoji> premium emoji — it silently falls back to the
        # plain glyph. Button is now attached via edit after a userbot send.)
        # FIX (2026-09-23b, "premium emojis gayab" incident): the previous
        # fix sent Hot Streak Alerts via plain Bot API to guarantee the
        # button worked — but Bot API silently drops <tg-emoji> to its plain
        # fallback glyph (🔥/🔮 etc), exactly as already documented elsewhere
        # in this file for dropped-call/X-alerts ("bot sends = no premium
        # emojis"). Now: send via userbot FIRST (premium emoji render
        # correctly, same as every other alert type), then attach the "View
        # KOL" button afterward via the bot's channel-admin edit rights
        # (editMessageReplyMarkup works on ANY message in a channel the bot
        # administers, regardless of who originally sent it). Falls back to
        # plain Bot API (no premium emoji, but never silently missing) only
        # if the userbot is unavailable/fails.
        wizard_post_id = None
        sent_via_userbot = False
        if userbot_client:
            try:
                emoji_ids_hs = list(pack["checks"]) + _hs_chain_ids_in_order(pack, hits)
                if media and media.get("file_id"):
                    sm = await _userbot_send_media_with_emoji(
                        bot, TARGET_CHANNEL, media["file_id"], media.get("type", "photo"),
                        text, emoji_ids_hs, forced_pack=None)
                else:
                    sm = await _userbot_send_with_premium_emoji(
                        TARGET_CHANNEL, text, emoji_id=emoji_ids_hs, forced_pack=None)
                if sm:
                    wizard_post_id = sm.id
                    sent_via_userbot = True
            except Exception as e:
                logger.warning(f"hot-streak userbot send failed, falling back to bot API: {e}")

        if wizard_post_id:
            try:
                await bot.edit_message_reply_markup(
                    chat_id=TARGET_CHANNEL, message_id=wizard_post_id, reply_markup=kb)
            except Exception as e:
                logger.warning(f"hot-streak View KOL button attach failed: {e}")
        else:
            try:
                if media and media.get("file_id"):
                    if media.get("type") == "video":
                        m = await bot.send_video(TARGET_CHANNEL, video=media["file_id"], caption=text,
                                                 parse_mode="HTML", reply_markup=kb)
                    else:
                        m = await bot.send_photo(TARGET_CHANNEL, photo=media["file_id"], caption=text,
                                                 parse_mode="HTML", reply_markup=kb)
                    wizard_post_id = m.message_id
            except Exception as e:
                logger.warning(f"hot-streak bot-API media send failed: {e}")
        if not wizard_post_id:
            try:
                m = await bot.send_message(TARGET_CHANNEL, text, parse_mode="HTML",
                                           disable_web_page_preview=True, reply_markup=kb)
                wizard_post_id = m.message_id
            except Exception as e:
                logger.error(f"hot-streak text send FAILED @{channel_key}: {e}")
                return None

        logger.info(f"🔥 Hot Streak Alert posted @{channel_key} — {len(hits)} calls, color={color}")
        if wizard_post_id:
            asyncio.create_task(_hs_forward_and_pin(bot, channel_key, wizard_post_id))
        return wizard_post_id
    except Exception as e:
        logger.error(f"hot-streak post failed @{channel_key}: {e}")
        return None

# --- extracted: _gt_get ---
def _gt_get(url, timeout=12):
    try:
        r = _dex_session.get(url, headers={"Accept": "application/json"}, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.debug(f"gecko get failed {url}: {e}")
    return None

# --- extracted: call_peak_ratio ---
def call_peak_ratio(call):
    """Best known ATH multiplier for a tracked call (never below live ratio)."""
    try:
        pr = float(call.get("peak_ratio", 0) or 0)
    except (TypeError, ValueError):
        pr = 0.0
    try:
        lr = float(call.get("last_ratio", 0) or 0)
    except (TypeError, ValueError):
        lr = 0.0
    return max(pr, lr)

# --- extracted: _update_peak ---
def _update_peak(call, ratio, cur_mc=0.0):
    """Store a new all-time-high ratio/MC on the call. Returns True if raised."""
    try:
        ratio = float(ratio or 0)
    except (TypeError, ValueError):
        return False
    if ratio <= 0:
        return False
    prev = 0.0
    try:
        prev = float(call.get("peak_ratio", 0) or 0)
    except (TypeError, ValueError):
        prev = 0.0
    if ratio <= prev + 1e-9:
        return False
    entry_mc = 0.0
    try:
        entry_mc = float(call.get("entry_mc", 0) or 0)
    except (TypeError, ValueError):
        entry_mc = 0.0
    peak_mc = cur_mc if cur_mc and cur_mc > 0 else (entry_mc * ratio if entry_mc > 0 else 0.0)
    call["peak_ratio"] = round(ratio, 4)
    if peak_mc > 0:
        call["peak_mc"] = peak_mc
        call["peak_mc_fmt"] = fmt_mc(peak_mc)
    call["peak_at"] = datetime.utcnow().isoformat()
    return True

# --- extracted: _complete_history_call ---
def _complete_history_call(call, highest_x, current_x, current_mc):
    """Return True once a tracked call has enough real data for history.

    A new call is a valid record from the moment its token details are resolved;
    it must not be hidden merely because it is still near its 1.0x entry.  The
    old performance gate made correctly tracked fresh calls disappear from the
    username lookup until they moved more than 1%, which looked like data loss.
    """
    try:
        entry_mc = float(call.get("entry_mc", 0) or 0)
    except (TypeError, ValueError):
        entry_mc = 0.0
    # OWNER FIX (2026-09-14): /addmissedcall always stores msg_id=0 (there is
    # no real Telegram message for a manually-added call — see cmd_addmissedcall).
    # `call.get("msg_id")` truthy-checks that value, and bool(0) is False, so
    # EVERY manually-added call — and every /fixmc-corrected call that started
    # life as a manually-added one — was silently dropped from /history and
    # channel records forever, no matter how correct entry_mc/symbol/chain
    # were. msg_id only needs to exist on the dict; 0 is a valid value for a
    # manual entry, so check presence (`is not None`), not truthiness.
    return bool(
        call.get("ca") and call.get("msg_id") is not None and call.get("symbol")
        and call.get("chain") and entry_mc > 0 and current_mc > 0
        and call.get("entry_fmt") not in ("", "N/A")
        and not call.get("dex_pending")
    )

# --- extracted: pending_media_job ---
async def pending_media_job(context):
    """Post milestones that were held back because their media was missing.
    As soon as the owner uploads media for that X level, the alert goes out
    and the channel/leaderboard/champions/trending start showing it."""
    try:
        if not pending_media_alerts:
            return
        changed = False
        for call_key in list(pending_media_alerts.keys()):
            levels = sorted(pending_media_alerts.get(call_key, set()))
            if not levels:
                pending_media_alerts.pop(call_key, None); changed = True; continue
            call = tracked_calls.get(call_key)
            if not call:
                pending_media_alerts.pop(call_key, None); changed = True; continue
            ready = [x for x in levels if milestone_has_media(x)]
            if not ready:
                continue
            top = max(ready)
            entry_mc = call.get("entry_mc", 0) or 0
            # MAX_BACKFILL_FIX_NOV11: show the REAL stored peak MC, not
            # entry x milestone (a computed number that looks plausible
            # even when wrong). Fall back to the threshold only when no
            # peak was ever recorded.
            _pk_mc = float(call.get("peak_mc", 0) or 0)
            cur_fmt = fmt_mc(_pk_mc) if _pk_mc > 0 else (fmt_mc(entry_mc * top) if entry_mc > 0 else f"{top}X")
            try:
                posted = await send_alert(
                    context.bot, call.get("channel", ""), call.get("msg_id", 0),
                    top, call.get("chain", "SOL"), call.get("entry_fmt", "N/A"),
                    cur_fmt, call.get("ca", ""), call.get("symbol", ""))
            except Exception as e:
                logger.error(f"pending_media_job send failed: {e}")
                continue
            if posted and posted != "silent":
                rest = {x for x in levels if x not in ready}
                if rest:
                    pending_media_alerts[call_key] = rest
                else:
                    pending_media_alerts.pop(call_key, None)
                changed = True
                logger.info(f"🚀 Held milestone released: {top}X {call_key} (media now set)")
            await asyncio.sleep(0.3)
        if changed:
            _save_pending_media()
    except Exception as e:
        logger.error(f"pending_media_job: {e}")

# --- extracted: TG_CHANNEL_LINK ---
TG_CHANNEL_LINK = "https://t.me/WizardScan"

# --- extracted: BOT_LINK ---
BOT_LINK        = "https://t.me/WIZARD_SCAN_BOT"

# --- extracted: _PKB_FALLBACK_LOGGED ---
_PKB_FALLBACK_LOGGED = {"done": False}

# --- extracted: _icon_kb ---
def _icon_kb(text, icon_id, **kwargs):
    """Same safe-fallback pattern as _pkb(), but takes any custom emoji ID —
    for buttons that need a theme-specific icon instead of the fixed 🔮
    WIZ_BTN_EMOJI_ID (e.g. Hot Streak Alert's per-color 'View KOL' button)."""
    try:
        return InlineKeyboardButton(text, icon_custom_emoji_id=icon_id, **kwargs)
    except TypeError as e:
        if not _PKB_FALLBACK_LOGGED["done"]:
            _PKB_FALLBACK_LOGGED["done"] = True
            try:
                import telegram as _tg_mod
                _ptb_ver = getattr(_tg_mod, "__version__", "unknown")
            except Exception:
                _ptb_ver = "unknown"
            logger.error(
                f"⚠️ icon_custom_emoji_id REJECTED by installed python-telegram-bot "
                f"(version={_ptb_ver}) — buttons falling back to plain text. "
                f"requirements.txt needs a newer python-telegram-bot that supports "
                f"InlineKeyboardButton(icon_custom_emoji_id=...). Error: {e}"
            )
        return InlineKeyboardButton(text, **kwargs)

# --- extracted: X_MILESTONES ---
X_MILESTONES = [2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 15, 18, 20, 22, 25, 28,
                30, 32, 34, 35, 38, 40, 45, 50,
                55, 60, 65, 75, 100, 150, 200, 250, 300,  # SUPPRESS_LEVELS_NOV14: 70 removed - no 70x alerts
                500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000,
                6000, 7000, 8000, 9000, 10000,
                11000, 12000, 13000, 14000, 15000, 20000, 25000, 30000,
                40000, 50000, 60000, 70000, 80000, 90000, 100000]

# --- extracted: CHAMPION_MILESTONES ---
CHAMPION_MILESTONES = [2, 5, 8, 10, 15, 20, 22, 25, 28, 30, 35, 40, 45, 50, 55, 60, 65, 75, 80, 90, 100, 150, 200, 250, 300, 500, 600, 700, 800, 900, 1000]

# --- extracted: CHAMPION_VIDEO_LEVELS ---
CHAMPION_VIDEO_LEVELS = (2, 5, 10, 20, 30, 40, 50, 60, 80, 90, 100, 150, 200, 250, 300, 500, 600, 700, 800, 900, 1000)

# --- extracted: MAX_MILESTONE ---
MAX_MILESTONE = 1_000

# --- extracted: SUPPORTED_CHAINS ---
SUPPORTED_CHAINS = {"ethereum": "ETH", "bsc": "BNB", "base": "BASE", "solana": "SOL",
                    "robinhood": "RH", "ton": "TON", "arc": "ARC"}

# --- extracted: CHAIN_TO_DEXPATH ---
CHAIN_TO_DEXPATH = {"SOL": "solana", "ETH": "ethereum", "BNB": "bsc", "BASE": "base",
                    "RH": "robinhood", "TON": "ton", "ARC": "arc"}

# --- extracted: LEADERBOARD_PREMIUM_EMOJIS ---
LEADERBOARD_PREMIUM_EMOJIS = {
    "star":  5807621337035842393,
    "arrow": 5809813380969537713,
    1:  5807696606337703890,
    2:  5807710826974420795,
    3:  5807617338421289285,
    4:  5807698311439721429,
    5:  5807461821950467873,
    6:  5807463114735624394,
    7:  5807614559577449387,
    8:  5807410527156052455,
    9:  5807690301325712919,
    10: 5807897739656175948,
}

# --- extracted: CHAMPIONS_PREMIUM_EMOJIS ---
CHAMPIONS_PREMIUM_EMOJIS = {
    "star":  5807491057792853272,
    "arrow": 5807952487604297661,
    "bolt":  6044166381690166762,   # ⚡️ — 3 info points in the champions post
    1:  5807615306901759629,
    2:  5807631344309642763,
    3:  5807396667296588812,
    4:  5807712914328526729,
    5:  5807614774325813553,
    6:  5807561465191737490,
    7:  5807686972726058383,
    8:  5807454391657045517,
    9:  5807632997872049875,
    10: 5807816251241670522,
}

# --- extracted: _display_handle ---
def _display_handle(value):
    """Display a Telegram username with a capitalized first character."""
    raw = str(value or "").lstrip("@").strip()
    return raw[:1].upper() + raw[1:].lower() if raw else raw

# --- extracted: TEER_EMOJI_ID ---
TEER_EMOJI_ID = 5807638920631951079

# --- extracted: MC_ENTRY_EMOJI_ID ---
MC_ENTRY_EMOJI_ID = 5816433179703124832

# --- extracted: MAESTRO_REFLINK_BASE ---
MAESTRO_REFLINK_BASE = "https://t.me/maestro?start="

# --- extracted: MAESTRO_REF_SUFFIX ---
MAESTRO_REF_SUFFIX   = "_r-wizard_scan"

# --- extracted: MAESTRO_PLAIN_REFLINK ---
MAESTRO_PLAIN_REFLINK = "https://t.me/maestro?start=r-wizard_scan"

# --- extracted: WIZARD_X_FALLBACK_LINK ---
WIZARD_X_FALLBACK_LINK = "https://x.com/Cheesy_Wolf"

# --- extracted: WIZARD_DEV_LINK ---
WIZARD_DEV_LINK        = "https://t.me/Wizard_Scan"

# --- extracted: EMOJI_PACKS ---
EMOJI_PACKS = [
    {   # Sirf yahi emojis — har 10 posts wali rotation band.
        # Owner baad me naye packs isi list me add karega.
        "name": "default",
        "crystal": 5816433179703124832,   # Crystal ball (entry MC)
        "kol":     5816473758554136304,   # Caller
        "mae":     5816930983592599467,   # Trade
        "bot":     5816604050682028040,   # Details
        "caller":  5816473758554136304,
        "trade":   5816930983592599467,
        "details": 5816604050682028040,
        "twitter": 5816676712938741891,
        "dex":     5816447254310954356,
        "dev":     5816456703239004710,
        "chain": {
            "SOL":  5816406349042425264,
            "ETH":  5816735725789388508,
            "BNB":  5816541305504800959,
            "BSC":  5816541305504800959,
            "BASE": 5816735725789388508,  # BASE emoji nahi diya gaya — ETH fallback
            "RH":   5818717423699763201,
            "TON":  5816609367851542120,
            "ARC":  5861777584693453399,
        },
    },
    {   # BLUE pack — 11X aur us se upar wali X alerts ke liye.
        "name": "blue",
        "teer":    5818848205453927129,   # Teer (MC separator)
        "crystal": 5816826345304367540,   # Crystal ball (entry MC)
        "kol":     5864095960795193775,   # Caller
        "mae":     5866050647656241980,   # Trade
        "bot":     5863725747499181228,   # Details
        "caller":  5864095960795193775,
        "trade":   5866050647656241980,
        "details": 5863725747499181228,
        "twitter": 5866085209258076126,
        "dex":     5863945855983166730,
        "dev":     5864070027782661496,
        "chain": {
            "SOL":  5865982877367279051,
            "ETH":  5865974768469026405,
            "BNB":  5866260216290483682,
            "BSC":  5866260216290483682,
            "BASE": 5866335262254047661,
            "RH":   5866254272055747007,
            "TON":  5866181502424850828,
            "ARC":  5866195366579281407,
        },
    },
]

# --- extracted: X76_PACK ---
X76_PACK = {
    "name":    "x76_99",
    "teer":    5924885884225725783,
    "crystal": 5922528720274333196,
    "kol":     5924840202953563651,   # Caller
    "mae":     5924887348809572269,   # Trade
    "bot":     5925032716272672656,   # Details
    "caller":  5924840202953563651,
    "trade":   5924887348809572269,
    "details": 5925032716272672656,
    "twitter": 5924797287640342361,
    "dex":     5924716748413608464,
    "dev":     5924778419849011394,
    "chain": {
        "SOL":  5922628608328737076,
        "ETH":  5926897127216128491,
        "BNB":  5924529380465319917,
        "BSC":  5924529380465319917,
        "BASE": 5922793719756498359,
        "RH":   5925025792785391919,
        "TON":  5922492423505715776,
        "ARC":  5926935975195320004,
    },
}

# --- extracted: X_PACK_RULES ---
X_PACK_RULES = [
    (2, 10, "default"),
    (11, 75, "blue"),
    (76, 99, "x76_99"),
    (100, 10 ** 9, "blue"),
]

# --- extracted: DROPPED_NUM_EMOJI ---
DROPPED_NUM_EMOJI   = 5877468380125990242

# --- extracted: DROPPED_ARROW_EMOJI ---
DROPPED_ARROW_EMOJI = 5884123981706956210

# --- extracted: load_channels ---
def load_channels():
    """Return tracked channels, self-healing case-insensitive duplicates
    (e.g. 'SomeKOL' and 'somekol' both stored) that caused the same channel
    to show twice under Tracked KOLs and be scanned/monitored twice."""
    raw = load_json(CHANNELS_FILE, [])
    seen, deduped, dup_found = set(), [], False
    for c in raw:
        key = str(c).lstrip("@").lower()
        if key in seen:
            dup_found = True
            continue
        seen.add(key)
        deduped.append(c)
    if dup_found:
        save_json(CHANNELS_FILE, deduped)
    return deduped

# --- extracted: load_removed_channels ---
def load_removed_channels() -> set:
    try:
        return {str(c).lstrip("@").lower() for c in load_json(REMOVED_CHANNELS_FILE, [])}
    except Exception:
        return set()

# --- extracted: save_config ---
def save_config(c):          save_json(BOT_CONFIG_FILE, c)

# --- extracted: _clean_x_handle ---
def _clean_x_handle(value):
    """Return a safe bare X handle; reject Telegram/other URLs and junk."""
    raw = str(value or "").strip()
    if not raw:
        return ""
    lowered = raw.lower()
    if any(part in lowered for part in ("t.me/", "telegram.me/", "telegram.dog/")):
        return ""
    # Accept x.com/twitter.com profile URLs, but never arbitrary URLs.
    url_match = re.fullmatch(
        r"https?://(?:www\.)?(?:x\.com|twitter\.com)/([A-Za-z0-9_]{1,15})/?(?:\?.*)?",
        raw, re.IGNORECASE)
    if url_match:
        raw = url_match.group(1)
    elif "://" in raw or "/" in raw:
        return ""
    raw = raw.lstrip("@").strip()
    return raw if re.fullmatch(r"[A-Za-z0-9_]{1,15}", raw) else ""

# --- extracted: load_x_accounts ---
def load_x_accounts():
    data = load_json(X_ACCOUNTS_FILE, {})
    if isinstance(data, list):
        # Migrate old list format → dict {channel_lower: x_handle}
        result = {}
        for item in data:
            if isinstance(item, dict):
                ch = item.get("channel","").lstrip("@").lower()
                x  = (item.get("x","") or item.get("twitter","") or item.get("handle","")).lstrip("@")
                if ch and x:
                    clean = _clean_x_handle(x)
                    if clean:
                        result[ch] = clean
        if result:
            save_json(X_ACCOUNTS_FILE, result)
        return result
    if not isinstance(data, dict):
        return {}
    # Sanitize legacy data on every read so an old t.me value can never leak
    # into Leaderboard/Champions after a backup restore.
    cleaned = {}
    for channel, value in data.items():
        handle = _clean_x_handle(value)
        if handle:
            cleaned[str(channel).lstrip("@").lower()] = handle
    if cleaned != data:
        save_json(X_ACCOUNTS_FILE, cleaned)
    return cleaned

# --- extracted: _get_channel_x_handle ---
def _get_channel_x_handle(channel):
    """Return X/Twitter handle for a channel (without @), or '' if not set."""
    return _clean_x_handle(load_x_accounts().get(channel.lstrip("@").lower(), ""))

# --- extracted: load_subscriptions ---
def load_subscriptions():    return load_json(SUBSCRIPTIONS_FILE, [])

# --- extracted: load_channel_subs ---
def load_channel_subs():     return load_json(CHANNEL_SUBS_FILE, {})

# --- extracted: load_call_archive ---
def load_call_archive():   return load_json(CALL_ARCHIVE_FILE, {})

# --- extracted: load_channel_points ---
def load_channel_points():
    """Load points. Points can NEVER be negative — any legacy negative value
    stored on disk is clamped to 0 on read."""
    data = load_json(CHANNEL_POINTS_FILE, {})
    try:
        for _k, _v in data.items():
            if isinstance(_v, dict) and _v.get("points", 0) < 0:
                _v["points"] = 0
    except Exception:
        pass
    return data

# --- extracted: load_kol_owners ---
def load_kol_owners():           return load_json(KOL_OWNERS_FILE, {})

# --- extracted: load_linked_channels ---
def load_linked_channels():  return load_json(LINKED_CHANNELS_FILE, {})

# --- extracted: cfg_get ---
def cfg_get(key, default=None): return load_config().get(key, default)

# --- extracted: get_milestones ---
def get_milestones():
    cfg = load_config()
    stored = cfg.get("custom_milestones")
    if stored:
        try: base = sorted(set(int(x) for x in stored))
        except Exception: base = list(X_MILESTONES)
    else:
        base = list(X_MILESTONES)
    # Also include X values for which milestone_media is set, so those X levels are always tracked
    try:
        media_keys = cfg.get("milestone_media", {}).keys()
        extra = [int(k) for k in media_keys if k.lstrip('-').isdigit() and int(k) <= MAX_MILESTONE]
        if extra:
            base = sorted(set(base) | set(extra))
    except Exception:
        pass
    return base

# --- extracted: media_milestone_levels ---
def media_milestone_levels():
    """Return set of X levels that have media configured, or None if a global
    media is set (meaning every level is publishable)."""
    try:
        cfg = load_config()
        mm  = cfg.get("milestone_media", {}) or {}
    except Exception:
        return None
    g = mm.get("global")
    if isinstance(g, dict) and g.get("file_id"):
        return None
    levels = set()
    for k, v in mm.items():
        if str(k).lstrip('-').isdigit() and isinstance(v, dict) and v.get("file_id"):
            levels.add(int(k))
    return levels

# --- extracted: milestone_has_media ---
def milestone_has_media(x) -> bool:
    """True only if media is configured for this X level (or a global media
    exists). Milestones without media are recorded internally but NOT posted."""
    levels = media_milestone_levels()
    if levels is None:
        return True
    try:
        return int(x) in levels
    except Exception:
        return False

# --- extracted: display_x ---
def display_x(x):
    """Cap an X value down to the highest milestone that has media configured.
    Used for leaderboard / champions / trending so un-published milestones
    never appear publicly."""
    try: x = int(x)
    except Exception: return 0
    # Media is no longer required for public visibility — leaderboard /
    # champions / trending always show the true X instantly.
    return x

# --- extracted: _milestones_since ---
def _milestones_since(call_key, reset_dt):
    """Milestones of a call that were reached AFTER reset_dt.
    Milestones without a recorded timestamp (legacy data) are treated as old."""
    ms_all = list(sent_milestones.get(call_key, set()))
    if not reset_dt:
        return ms_all
    times = milestone_times.get(str(call_key), {})
    out = []
    for m in ms_all:
        ts = times.get(str(int(m)))
        if not ts:
            continue
        try:
            if datetime.fromisoformat(ts) >= reset_dt:
                out.append(m)
        except Exception:
            continue
    return out

# --- extracted: _locked_userbot_edit ---
async def _locked_userbot_edit(*args, **kwargs):
    """Same as userbot_client.edit_message(...) but serialized via _userbot_edit_lock."""
    async with _userbot_edit_lock:
        return await userbot_client.edit_message(*args, **kwargs)

# --- extracted: POINTS_FOR_CHAMPION ---
POINTS_FOR_CHAMPION = 100

# --- extracted: get_channel_points ---
def get_channel_points(channel):
    """Return total points for a channel (never negative)."""
    pts = load_channel_points()
    return max(0, pts.get(channel.lower(), {}).get("points", 0))

# --- extracted: _is_deleted_blocked ---
def _is_deleted_blocked(call) -> bool:
    """Rank/points gate for a call whose original Telegram post was deleted.
    (DELETE_DM_VIDEO_NOV16) X alerts ALWAYS fire now, even for deleted-post
    calls. Only Champions / Leaderboard / Trending / points are blocked,
    and only when the owner tapped "No" (delete_decision == "no").
    Ignore (decision None) or "Yes" count normally. The call still stays
    in the channel record, tagged as deleted."""
    try:
        return bool(call) and bool(call.get("post_deleted")) and call.get("delete_decision") == "no"
    except Exception:
        return False

# --- extracted: safe_format ---
def safe_format(template, **kwargs):
    # User-facing templates should never render a fully lowercase @username.
    # Keep stored channel keys lowercase; capitalize only the display value.
    if "channel" in kwargs:
        kwargs["channel"] = _display_handle(kwargs["channel"])
    lk = {k.lower(): v for k, v in kwargs.items()}
    return re.sub(r'\{([A-Za-z_][A-Za-z0-9_]*)\}',
                  lambda m: str(lk[m.group(1).lower()]) if m.group(1).lower() in lk else m.group(0),
                  template)

# --- extracted: fmt_mc ---
def fmt_mc(value):
    """Format market cap without a currency sign, e.g. 12.5K MC or 3.45M MC."""
    if not value: return "N/A"
    if value >= 1_000_000_000: return f"{value/1_000_000_000:.2f}B MC"
    elif value >= 1_000_000:   return f"{value/1_000_000:.2f}M MC"
    elif value >= 1_000:       return f"{value/1_000:.1f}K MC"
    return f"{value:.0f} MC"

# --- extracted: _mc_no_label ---
def _mc_no_label(s):
    """Strip the trailing ' MC' label off an already fmt_mc()-formatted string,
    e.g. '12.1K MC' -> '12.1K'. Used in spots where the surrounding template
    text already says 'MC' on its own (dropped-call header, x-alert entry/
    current, channel history), so the number isn't doubled up with 'MC MC'."""
    if not isinstance(s, str):
        return s
    if s.endswith(" MC"):
        return s[:-3]
    return s

# --- extracted: fmt_x ---
def fmt_x(x: float) -> str:
    """Format X multiplier as readable decimal string (e.g. 1.25x, 2.7x, 123x)."""
    if x <= 0: return "1x"
    if x >= 100:   return f"{x:.0f}x"
    elif x >= 2:   return f"{x:.1f}x"
    elif x > 1.01: return f"{x:.1f}x"
    return "1x"

# --- extracted: _dex_cache ---
_dex_cache: dict = {}

# --- extracted: _pick_market_cap ---
def _pick_market_cap(pairs: list) -> tuple[float, bool]:
    """Return (market_cap, is_fdv) — the most trustworthy market cap from a
    DexScreener pairs list, and whether it had to fall back to FDV.

    DexScreener reports marketCap/fdv per pair and a single stale pair can be
    badly wrong. We therefore:
      1. take marketCap from the deepest-liquidity pair,
      2. cross-check it against the median marketCap of all pairs of the same
         token — if it is more than 3x off, trust the median instead,
      3. never substitute FDV for real market cap AS IF it were real.

    FIX (RH-chain "called at 19K, bot shows 119K instantly" incident,
    2026-09-24): point 3 used to mean "return 0.0" whenever no pair had a
    real marketCap field — which DexScreener commonly never populates for
    Robinhood-chain (tokenized-stock) pairs, only `fdv`. A silent 0.0 here
    means _reconcile_entry_mc sees "nothing to compare the caller's post-hint
    against" and blindly LOCKS IN that hint as fully "confirmed", with the
    entire cross-check/correction machinery (_confirm_entry_mc, the pre-post
    settle check, the _mc_is_fdv ratio guard) never engaging at all — because
    all of that machinery only ever triggers off an explicit _mc_is_fdv flag,
    and this path never set one. A caller posting (or a bot mis-parsing) a
    lowballed "called at 2.7K" MC then rode straight through to a real 65K
    chart as a fake "24X", with zero of the anti-cheat checks built for
    exactly this scenario ever firing.
    Now: FDV is still never trusted AS real MC (nothing changes for the
    normal case), but it IS returned as a flagged, provisional number instead
    of a bare 0 — so callers correctly route it through the same
    live_fdv/post_unconfirmed provisional path already built for alt sources
    (Birdeye/GeckoTerminal), which locks it in only after an independent
    cross-check confirms it.
    """
    mcs = []
    for p in pairs:
        try:
            v = float(p.get("marketCap") or 0)
        except (TypeError, ValueError):
            v = 0.0
        if v > 0:
            mcs.append(v)
    if mcs:
        primary = mcs[0]
        if len(mcs) >= 3:
            ordered = sorted(mcs)
            median = ordered[len(ordered) // 2]
            if median > 0 and (primary > median * 3 or primary * 3 < median):
                return (median, False)
        return (primary, False)
    # No pair has a real circulating marketCap — fall back to the
    # deepest-liquidity pair's FDV, flagged, rather than a bare 0.0.
    for p in sorted(pairs, key=lambda p: (p.get("liquidity", {}) or {}).get("usd", 0) or 0,
                     reverse=True):
        try:
            fdv = float(p.get("fdv") or 0)
        except (TypeError, ValueError):
            fdv = 0.0
        if fdv > 0:
            return (fdv, True)
    return (0.0, False)

# --- extracted: _parse_dex_pairs ---
def _parse_dex_pairs(pairs: list, min_liquidity: int, token_address: str = "", prefer_chain: str = None) -> dict | None:
    """Parse a DexScreener pairs list into our internal dict. Returns None if unusable."""
    sup = [p for p in pairs if p.get("chainId","").lower() in SUPPORTED_CHAINS
           and (p.get("liquidity",{}).get("usd") or 0) >= min_liquidity]
    # priceUsd always describes baseToken.  A searched contract can also occur
    # as quoteToken; treating that pair as the call token silently tracks the
    # other asset (often SOL/WETH) and leaves the call stuck around 1x.
    if token_address:
        wanted = token_address.lower()
        sup = [p for p in sup if ((p.get("baseToken") or {}).get("address") or "").lower() == wanted]
    # WIZCHAIN_NOV23: when the caller knows the chain (e.g. the KOL's pair
    # link was on BSC), only pairs on that chain count. A multi-chain token
    # must not resolve to a different chain's pair (newest-pair guard /
    # ETH-first lookup). Safe fallback: if no pair matches, sup is unchanged.
    if prefer_chain:
        _pc = str(prefer_chain).lower()
        _on = [p for p in sup if (p.get("chainId") or "").lower() == _pc]
        if _on:
            sup = _on
    if not sup:
        return None
    # Cross-chain address collision guard: the exact same literal address can
    # exist as a completely unrelated pair on more than one EVM chain (vanity/
    # CREATE2 deployments, or plain coincidence). Sorting by liquidity alone
    # in that case can silently lock onto a totally different, much bigger
    # project on another chain — e.g. a fresh 435 MC KOL call getting matched
    # to an unrelated $1.5M pool elsewhere that happens to share this address.
    # A freshly-posted call is virtually always the NEWEST pool, so when
    # multiple different chains match, narrow down to the chain whose pool
    # was created most recently before ranking by liquidity within it.
    chains_seen = {p.get("chainId", "").lower() for p in sup}
    if len(chains_seen) > 1:
        dated = [p for p in sup if p.get("pairCreatedAt")]
        if dated:
            newest_chain = max(dated, key=lambda p: p.get("pairCreatedAt") or 0).get("chainId", "").lower()
            same_chain = [p for p in sup if p.get("chainId", "").lower() == newest_chain]
            if same_chain and len(same_chain) < len(sup):
                logger.warning(
                    f"⚠️ Cross-chain address collision for {token_address[:14]}… "
                    f"across {sorted(chains_seen)} — restricting to newest pool's "
                    f"chain ({newest_chain}) to avoid tracking the wrong project")
                sup = same_chain
    sup = sorted(sup, key=lambda p: p.get("liquidity",{}).get("usd",0) or 0, reverse=True)
    best = sup[0]
    chain  = SUPPORTED_CHAINS[best.get("chainId","").lower()]
    price  = float(best.get("priceUsd") or 0)
    mc, mc_is_fdv = _pick_market_cap(sup)
    symbol = best.get("baseToken",{}).get("symbol","")
    if price <= 0 and mc <= 0 and not symbol:
        return None
    price_change_h1  = float((best.get("priceChange") or {}).get("h1") or 0)
    price_change_h24 = float((best.get("priceChange") or {}).get("h24") or 0)
    volume_h24       = float((best.get("volume") or {}).get("h24") or 0)
    liquidity_usd    = float((best.get("liquidity") or {}).get("usd") or 0)
    txns_h24_buys    = int((best.get("txns") or {}).get("h24", {}).get("buys") or 0)
    txns_h24_sells   = int((best.get("txns") or {}).get("h24", {}).get("sells") or 0)
    listed_at        = best.get("pairCreatedAt")
    socials = best.get("info",{}).get("socials") or []
    tg_link = ""
    for s in socials:
        if "t.me" in (s.get("url","")) or "telegram" in (s.get("type","")).lower():
            tg_link = s.get("url",""); break
    return {
        "chain":           chain,
        "mcap":            mc,
        "mcap_fmt":        fmt_mc(mc) if mc > 0 else "N/A",
        "price":           price,
        "symbol":          symbol,
        "name":            best.get("baseToken",{}).get("name",""),
        "pair_addr":       best.get("pairAddress",""),
        "tg_link":         tg_link,
        "price_change_h1": price_change_h1,
        "price_change_h24":price_change_h24,
        "volume_h24":      volume_h24,
        "liquidity_usd":   liquidity_usd,
        "txns_buys":       txns_h24_buys,
        "txns_sells":      txns_h24_sells,
        "listed_at":       listed_at,
        "_partial":        (price <= 0 and mc <= 0),
        "_mc_is_fdv":      bool(mc_is_fdv and mc > 0),
    }

# --- extracted: _dex_miss_cache ---
_dex_miss_cache: dict = {}

# --- extracted: _DEX_MISS_TTL ---
_DEX_MISS_TTL = 3

# --- extracted: _DEX_CACHE_MAX ---
_DEX_CACHE_MAX = 4000

# --- extracted: _DEX_429_UNTIL ---
_DEX_429_UNTIL = 0.0

# --- extracted: _dex_breaker_open ---
def _dex_breaker_open() -> bool:
    return time.time() < _DEX_429_UNTIL

# --- extracted: _dex_trip_breaker ---
def _dex_trip_breaker():
    global _DEX_429_UNTIL
    _DEX_429_UNTIL = time.time() + _DEX_429_COOLDOWN

# --- extracted: _DEX_STATS ---
_DEX_STATS = {"ok": 0, "real429": 0, "budget_denied": 0, "breaker_denied": 0, "neterr": 0,
              "bulk_ok": 0, "single_ok": 0, "mon_ok": 0,
              "bulk_denied": 0, "single_denied": 0, "mon_denied": 0}

# --- extracted: _DEX_LAST_429_AT ---
_DEX_LAST_429_AT = 0.0

# --- extracted: _DEX_LAST_DEC_AT ---
_DEX_LAST_DEC_AT = 0.0

# --- extracted: _DEX_BUDGET_FLOOR ---
_DEX_BUDGET_FLOOR = 60

# --- extracted: _dex_stat ---
def _dex_stat(key: str):
    _DEX_STATS[key] = _DEX_STATS.get(key, 0) + 1

# --- extracted: _dex_stats_maybe_log ---
def _dex_stats_maybe_log():
    global _DEX_STATS_AT
    now = time.time()
    if now - _DEX_STATS_AT < 60:
        return
    snap = dict(_DEX_STATS)
    for k in list(_DEX_STATS):
        _DEX_STATS[k] = 0
    _DEX_STATS_AT = now
    logger.info(
        f"📊 DexScreener last min: ok={snap.get('ok',0)} real_429={snap.get('real429',0)} "
        f"budget_denied={snap.get('budget_denied',0)} breaker_denied={snap.get('breaker_denied',0)} "
        f"net_err={snap.get('neterr',0)} | budget={int(_DEX_EFF_BUDGET)}/{_DEX_MAX_REQUESTS_PER_MINUTE} "
        f"| lanes ok bulk={snap.get('bulk_ok',0)} single={snap.get('single_ok',0)} mon={snap.get('mon_ok',0)} "
        f"denied bulk={snap.get('bulk_denied',0)} single={snap.get('single_denied',0)} mon={snap.get('mon_denied',0)} "
        f"cooldown={_DEX_429_COOLDOWN}s adaptive={'on' if _DEX_ADAPTIVE else 'off'}")

# --- extracted: _dex_effective_budget ---
def _dex_effective_budget() -> int:
    global _DEX_EFF_BUDGET, _DEX_LAST_ADJ_AT
    now = time.time()
    if (_DEX_EFF_BUDGET < _DEX_MAX_REQUESTS_PER_MINUTE
            and now - _DEX_LAST_429_AT > 60 and now - _DEX_LAST_ADJ_AT > 30):
        _DEX_EFF_BUDGET = min(float(_DEX_MAX_REQUESTS_PER_MINUTE),
                              _DEX_EFF_BUDGET + max(5.0, _DEX_MAX_REQUESTS_PER_MINUTE * 0.05))
        _DEX_LAST_ADJ_AT = now
    return int(_DEX_EFF_BUDGET)

# --- extracted: _dex_on_real_429 ---
def _dex_on_real_429():
    """Called when DexScreener itself returned HTTP 429."""
    global _DEX_EFF_BUDGET, _DEX_LAST_429_AT, _DEX_LAST_DEC_AT
    now = time.time()
    _DEX_LAST_429_AT = now
    if not _DEX_ADAPTIVE:
        return   # observe only - never lowers the request budget
    # several in-flight requests can 429 together - cut only once per 10s
    if now - _DEX_LAST_DEC_AT >= 10:
        _DEX_LAST_DEC_AT = now
        _DEX_EFF_BUDGET = max(float(_DEX_BUDGET_FLOOR), _DEX_EFF_BUDGET * 0.8)

# --- extracted: _dex_rate_budget_ok ---
def _dex_rate_budget_ok(lane: str = None) -> bool:
    now = time.time()
    lane = lane or _DEX_LANE_CV.get()
    if lane not in _DEX_LANE_TS:
        lane = "single"
    budget = _dex_effective_budget()
    with _DEX_RATE_LOCK:
        for dq in _DEX_LANE_TS.values():
            while dq and now - dq[0] > 60:
                dq.popleft()
        total = sum(len(dq) for dq in _DEX_LANE_TS.values())
        if total >= budget:
            _dex_stat(lane + "_denied")
            return False
        # naye call ka instant track ("single" lane) ke liye ~12% budget HAMESHA reserved:
        # bulk/mon lanes use kabhi nahi chhoo sakte, taake dropped-call speed par asar na pade.
        if lane in ("bulk", "mon") and total >= budget - max(40, int(budget * 0.14)):   # 2026-10-26: 40 slots naye call ke liye reserved
            _dex_stat(lane + "_denied")
            return False
        if lane == "mon" and len(_DEX_LANE_TS["mon"]) >= max(8, int(budget * _DEX_MON_SHARE)):
            _dex_stat("mon_denied")
            return False
        _DEX_LANE_TS[lane].append(now)
        _DEX_RATE_TIMESTAMPS.append(now)          # purana counter (compat)
        while _DEX_RATE_TIMESTAMPS and now - _DEX_RATE_TIMESTAMPS[0] > 60:
            _DEX_RATE_TIMESTAMPS.popleft()
        _dex_stat(lane + "_ok")
        return True

# --- extracted: _birdeye_rate_budget_ok ---
def _birdeye_rate_budget_ok() -> bool:
    now = time.time()
    with _BIRDEYE_RATE_LOCK:
        while _BIRDEYE_RATE_TIMESTAMPS and now - _BIRDEYE_RATE_TIMESTAMPS[0] > 60:
            _BIRDEYE_RATE_TIMESTAMPS.popleft()
        if len(_BIRDEYE_RATE_TIMESTAMPS) >= _BIRDEYE_MAX_REQUESTS_PER_MINUTE:
            return False
        _BIRDEYE_RATE_TIMESTAMPS.append(now)
        return True

# --- extracted: _dex_cache_gc ---
def _dex_cache_gc():
    """Evict expired / overflowing cache entries (prevents unbounded RAM growth)."""
    import time as _time
    now = _time.time()
    for cache in (_dex_cache, _dex_miss_cache):
        if len(cache) < _DEX_CACHE_MAX:
            continue
        for k in [k for k, v in list(cache.items())
                  if (v[1] if isinstance(v, tuple) else v) < now]:
            cache.pop(k, None)
        while len(cache) > _DEX_CACHE_MAX:
            try:
                cache.pop(next(iter(cache)))
            except StopIteration:
                break

# --- extracted: _dex_get ---
def _dex_get(url: str, timeout: int = 5, fresh: bool = False, lane: str = None):
    """GET with shared session. fresh=True bypasses intermediary HTTP caches."""
    try:
        _dex_stats_maybe_log()
    except Exception:
        pass
    if _dex_breaker_open():
        _dex_stat("breaker_denied")
        return 429  # breaker open: request bhejne ke baghair rate-limited batao
    if not _dex_rate_budget_ok(lane):
        _dex_stat("budget_denied")
        return 429  # apna hi budget khatam — DexScreener ko hit kiye baghair fallback pe jao
    try:
        headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"} if fresh else None
        if fresh:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}_ts={int(time.time() * 1000)}"
        resp = _dex_session.get(url, headers=headers, timeout=timeout)
    except Exception as e:
        _dex_stat("neterr")
        logger.warning(f"DexScreener network error ({url.split('/')[-1][:16]}…): {e}")
        return None
    if resp.status_code == 429:
        _dex_stat("real429")
        _dex_on_real_429()
        _dex_trip_breaker()
        return 429
    if resp.status_code != 200:
        return None
    _dex_stat("ok")
    try:
        return resp.json()
    except Exception:
        return None

# --- extracted: _dex_candidate_chains ---
def _dex_candidate_chains(ca: str) -> list:
    """Which DexScreener chains can this address exist on? Keeps the
    chain-scoped lookup to 1-4 cheap requests instead of brute-forcing."""
    # len==42: standard EVM (0x+40hex). len==66: Robinhood/"RH" 32-byte
    # address format (0x+64hex) seen on DexScreener chart links — without
    # this, a correctly-extracted Robinhood CA still failed here and fell
    # through to being queried as if it were a Solana address.
    if ca.startswith("0x") and len(ca) in (42, 66):
        return ["ethereum", "bsc", "base", "robinhood"]
    if ca.startswith(("EQ", "UQ")):
        return ["ton"]
    return ["solana"]

# --- extracted: _pair_token_cache ---
_pair_token_cache: dict = {}

# --- extracted: _PAIR_RESOLVE_NEG_TTL ---
_PAIR_RESOLVE_NEG_TTL = 90

# --- extracted: _resolve_pair_to_token ---
def _resolve_pair_to_token(addr: str):
    """Return the baseToken mint for a pair/pool address, else None."""
    if not addr:
        return None
    cached = _pair_token_cache.get(addr)
    if cached is not None:
        # WIZCHAIN_NOV23: cache is (token, chain_slug, expires_at); old
        # 2-tuple entries (token, expires_at) are still honoured.
        if len(cached) == 3:
            token_c, chain_c, expires_at = cached
        else:
            token_c, expires_at = cached
            chain_c = None
        if token_c or time.time() < expires_at:
            return (token_c, chain_c)
        # negative result expired — fall through and try again
    token = None
    pair_slug = None  # WIZCHAIN_NOV23: DexScreener slug where the pair lives
    try:
        for slug in (_dex_candidate_chains(addr) or []):
            d = _dex_get(f"https://api.dexscreener.com/latest/dex/pairs/{slug}/{addr}",
                         timeout=8)
            if d == 429:
                break
            if not isinstance(d, dict):
                continue
            prs = d.get("pairs") or ([d.get("pair")] if d.get("pair") else [])
            for p in prs:
                if not isinstance(p, dict):
                    continue
                base = ((p.get("baseToken") or {}).get("address") or "").strip()
                if base and base.lower() != addr.lower():
                    token = base
                    pair_slug = slug  # WIZCHAIN_NOV23: remember the pair's chain
                    break
            if token:
                break
    except Exception as e:
        logger.debug(f"pair→token resolve failed {addr[:12]}…: {e}")
    # FIX (2026-09-25, maybac_whales RH pair-link calls stuck dex_pending
    # forever incident): logs showed several Robinhood-chain calls where the
    # KOL only posted a dexscreener.com/robinhood/<pairAddress> link (no
    # separate contract line) sitting at dex_pending=True for 30-60+ minutes
    # straight and never resolving — silently excluded from tracking,
    # Leaderboard and Champions the whole time even though other chains'
    # pair-links resolved in seconds. DexScreener's OWN /latest/dex/pairs/
    # <chain>/<addr> endpoint (above) can badly lag or never index a
    # brand-new Robinhood-chain pool. GeckoTerminal indexes the "robinhood"
    # network independently (already relied on elsewhere in this file for
    # RH-chain cross-checks) and exposes the base token on a plain pool
    # lookup, so it is now tried as a second, independent resolver instead
    # of leaving the call stuck forever on DexScreener alone.
    if not token:
        _GT_PAIR_SLUG = {"robinhood": "robinhood", "solana": "solana", "ethereum": "eth",
                         "bsc": "bsc", "base": "base", "ton": "ton", "arc": "arc"}
        try:
            for slug in (_dex_candidate_chains(addr) or []):
                net = _GT_PAIR_SLUG.get(slug)
                if not net:
                    continue
                gd = _gt_get(f"https://api.geckoterminal.com/api/v2/networks/{net}/pools/{addr}")
                base_id = ((((gd or {}).get("data") or {}).get("relationships") or {})
                           .get("base_token") or {}).get("data", {}).get("id", "") or ""
                if "_" in base_id:
                    base_addr = base_id.split("_", 1)[1].strip()
                    if base_addr and base_addr.lower() != addr.lower():
                        token = base_addr
                        pair_slug = slug  # WIZCHAIN_NOV23: remember the pair's chain
                        logger.info(f"🔗 Pair address resolved via GeckoTerminal (DexScreener "
                                    f"had no pool data): {addr[:10]}… → token {token[:10]}…")
                        break
        except Exception as e:
            logger.debug(f"pair→token GT resolve failed {addr[:12]}…: {e}")
    if len(_pair_token_cache) > 4000:
        _pair_token_cache.clear()
    # Positive resolves are permanent (a pair's base token never changes).
    # Negative resolves expire quickly so a too-early attempt gets retried
    # once DexScreener/GeckoTerminal actually indexes the pair.
    # WIZCHAIN_NOV23: cache now stores (token, chain_slug, expires_at).
    _pair_token_cache[addr] = (token, pair_slug, time.time() + _PAIR_RESOLVE_NEG_TTL)
    return (token, pair_slug)

# --- extracted: _GT_NET ---
_GT_NET   = {"SOL": "solana", "ETH": "eth", "BNB": "bsc", "BASE": "base", "TON": "ton", "ARC": "arc"}

# --- extracted: _DP_NET ---
_DP_NET   = {"SOL": "solana", "ETH": "ethereum", "BNB": "bsc", "BASE": "base", "TON": "ton", "ARC": "arc"}

# --- extracted: _BE_CHAIN ---
_BE_CHAIN = {"SOL": "solana", "ETH": "ethereum", "BNB": "bsc", "BASE": "base"}

# --- extracted: _MO_EVM ---
_MO_EVM   = {"ETH": "eth", "BNB": "bsc", "BASE": "base", "ARC": "arc"}

# --- extracted: _SLUG_TO_DISP ---
_SLUG_TO_DISP = {"solana": "SOL", "ethereum": "ETH", "eth": "ETH", "bsc": "BNB",
                 "base": "BASE", "ton": "TON", "robinhood": "RH", "arc": "ARC"}

# --- extracted: _alt_result ---
def _alt_result(chain, symbol, price, mcap, name="", source="", mc_is_fdv=False):
    """Build a dict in the exact shape _parse_dex_pairs() returns.

    mc_is_fdv: True when `mcap` was only obtainable via a fully-diluted-value
    fallback (no real circulating marketCap field was available from this
    provider). FDV is computed off max/total supply, not circulating supply,
    and can read 10-100x+ higher than the token's real market cap for tokens
    with large locked/uncirculated allocations — this is what caused fresh
    listings (e.g. Robinhood-chain tokens) to show as millions in MC when the
    caller's real entry was only thousands. Callers that need entry-MC
    accuracy (e.g. _reconcile_entry_mc) use this flag to avoid trusting an
    FDV-derived number over the KOL's own stated MC.
    """
    try:
        price = float(price or 0)
    except Exception:
        price = 0.0
    try:
        mcap = float(mcap or 0)
    except Exception:
        mcap = 0.0
    if price <= 0 and mcap <= 0 and not symbol:
        return None
    dexpath = CHAIN_TO_DEXPATH.get(chain, "solana") if "CHAIN_TO_DEXPATH" in globals() else "solana"
    return {
        "chain":            chain,
        "mcap":             mcap,
        "mcap_fmt":         fmt_mc(mcap) if mcap > 0 else "N/A",
        "price":            price,
        "symbol":           symbol or "",
        "name":             name or symbol or "",
        "pair_addr":        "",
        "tg_link":          "",
        "price_change_h1":  0.0,
        "price_change_h24": 0.0,
        "volume_h24":       0.0,
        "liquidity_usd":    0.0,
        "txns_buys":        0,
        "txns_sells":       0,
        "listed_at":        None,
        "_partial":         (price <= 0 and mcap <= 0),
        "_source":          source or "alt",
        "_dexpath":         dexpath,
        "_mc_is_fdv":       bool(mc_is_fdv and mcap > 0),
    }

# --- extracted: _alt_chains_for ---
def _alt_chains_for(ca: str) -> list:
    """Display-chain candidates for a contract address."""
    return [_SLUG_TO_DISP.get(s, "SOL") for s in _dex_candidate_chains(ca)
            if s in _SLUG_TO_DISP]

# --- extracted: _src_geckoterminal ---
def _src_geckoterminal(ca: str, chains: list):
    for ch in chains:
        net = _GT_NET.get(ch)
        if not net:
            continue
        try:
            r = _dex_session.get(
                f"https://api.geckoterminal.com/api/v2/networks/{net}/tokens/{ca}",
                headers={"Accept": "application/json"}, timeout=8)
            if r.status_code != 200:
                continue
            a = (r.json().get("data") or {}).get("attributes") or {}
            price   = float(a.get("price_usd") or 0)
            mc_real = float(a.get("market_cap_usd") or 0)
            mc_fdv  = float(a.get("fdv_usd") or 0)
            mc      = mc_real or mc_fdv
            res   = _alt_result(ch, a.get("symbol") or "", price, mc,
                                a.get("name") or "", "geckoterminal",
                                mc_is_fdv=(mc_real <= 0 and mc_fdv > 0))
            if res:
                return res
        except Exception as e:
            logger.debug(f"GeckoTerminal source failed ({ch}): {e}")
    return None

# --- extracted: _src_dexpaprika ---
def _src_dexpaprika(ca: str, chains: list):
    for ch in chains:
        net = _DP_NET.get(ch)
        if not net:
            continue
        try:
            r = _dex_session.get(
                f"https://api.dexpaprika.com/networks/{net}/tokens/{ca}",
                headers={"Accept": "application/json"}, timeout=8)
            if r.status_code != 200:
                continue
            d  = r.json() or {}
            sm = d.get("summary") or {}
            price   = float(sm.get("price_usd") or d.get("price_usd") or 0)
            mc_real = float(d.get("market_cap_usd") or sm.get("market_cap_usd") or 0)
            mc_fdv  = float(d.get("fdv") or 0)
            mc      = mc_real or mc_fdv
            res = _alt_result(ch, d.get("symbol") or "", price, mc,
                              d.get("name") or "", "dexpaprika",
                              mc_is_fdv=(mc_real <= 0 and mc_fdv > 0))
            if res:
                return res
        except Exception as e:
            logger.debug(f"DexPaprika source failed ({ch}): {e}")
    return None

# --- extracted: _src_birdeye ---
def _src_birdeye(ca: str, chains: list):
    if not BIRDEYE_API_KEY:
        return None
    for ch in chains:
        bchain = _BE_CHAIN.get(ch)
        if not bchain:
            continue
        if not _birdeye_rate_budget_ok():
            continue  # account-wide budget used up this minute — skip, try next chain/next tick
        try:
            r = _dex_session.get(
                "https://public-api.birdeye.so/defi/token_overview",
                params={"address": ca},
                headers={"X-API-KEY": BIRDEYE_API_KEY, "x-chain": bchain,
                         "accept": "application/json"},
                timeout=8)
            if r.status_code != 200:
                continue
            d = (r.json() or {}).get("data") or {}
            if not d:
                continue
            price   = float(d.get("price") or 0)
            mc_real = float(d.get("marketCap") or d.get("mc") or d.get("realMc") or 0)
            mc_fdv  = float(d.get("fdv") or 0)
            mc      = mc_real or mc_fdv
            res = _alt_result(ch, d.get("symbol") or "", price, mc,
                              d.get("name") or "", "birdeye",
                              mc_is_fdv=(mc_real <= 0 and mc_fdv > 0))
            if res:
                return res
        except Exception as e:
            logger.debug(f"Birdeye source failed ({ch}): {e}")
    return None

# --- extracted: _src_moralis ---
def _src_moralis(ca: str, chains: list):
    if not MORALIS_API_KEY:
        return None
    hdr = {"X-API-Key": MORALIS_API_KEY, "accept": "application/json"}
    for ch in chains:
        try:
            if ch == "SOL":
                r = _dex_session.get(
                    f"https://solana-gateway.moralis.io/token/mainnet/{ca}/price",
                    headers=hdr, timeout=8)
                if r.status_code != 200:
                    continue
                d = r.json() or {}
                price = float(d.get("usdPrice") or 0)
                sym   = ""
                mc    = 0.0
                try:
                    rm = _dex_session.get(
                        f"https://solana-gateway.moralis.io/token/mainnet/{ca}/metadata",
                        headers=hdr, timeout=8)
                    if rm.status_code == 200:
                        m   = rm.json() or {}
                        sym = m.get("symbol") or ""
                        supply = float(m.get("supply") or 0)
                        dec    = int(m.get("decimals") or 0) if str(m.get("decimals") or "").isdigit() else 0
                        if supply > 0 and price > 0:
                            if dec and supply > 10 ** 12:
                                supply = supply / (10 ** dec)
                            mc = supply * price
                except Exception:
                    pass
                res = _alt_result("SOL", sym, price, mc, sym, "moralis")
                if res:
                    return res
            else:
                evm = _MO_EVM.get(ch)
                if not evm:
                    continue
                r = _dex_session.get(
                    f"https://deep-index.moralis.io/api/v2.2/erc20/{ca}/price",
                    params={"chain": evm}, headers=hdr, timeout=8)
                if r.status_code != 200:
                    continue
                d = r.json() or {}
                price = float(d.get("usdPrice") or 0)
                sym   = (d.get("tokenSymbol") or "")
                res = _alt_result(ch, sym, price, 0, d.get("tokenName") or sym, "moralis")
                if res:
                    return res
        except Exception as e:
            logger.debug(f"Moralis source failed ({ch}): {e}")
    return None

# --- extracted: _src_coinmarketcap ---
def _src_coinmarketcap(ca: str, chains: list):
    if not CMC_API_KEY:
        return None
    hdr = {"X-CMC_PRO_API_KEY": CMC_API_KEY, "Accept": "application/json"}
    try:
        r = _dex_session.get(
            "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info",
            params={"address": ca}, headers=hdr, timeout=10)
        if r.status_code != 200:
            return None
        data = (r.json() or {}).get("data") or {}
        if not data:
            return None
        cid, info = next(iter(data.items()))
        if isinstance(info, list):
            info = info[0] if info else {}
        sym  = info.get("symbol") or ""
        name = info.get("name") or ""
        rq = _dex_session.get(
            "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest",
            params={"id": cid}, headers=hdr, timeout=10)
        price = mc = 0.0
        if rq.status_code == 200:
            q = ((rq.json() or {}).get("data") or {}).get(str(cid)) or {}
            if isinstance(q, list):
                q = q[0] if q else {}
            usd   = (q.get("quote") or {}).get("USD") or {}
            price = float(usd.get("price") or 0)
            mc    = float(usd.get("market_cap") or usd.get("fully_diluted_market_cap") or 0)
        ch = (chains or ["SOL"])[0]
        return _alt_result(ch, sym, price, mc, name, "coinmarketcap")
    except Exception as e:
        logger.debug(f"CoinMarketCap source failed: {e}")
    return None

# --- extracted: _fetch_alt_sources_sync ---
def _fetch_alt_sources_sync(ca: str):
    """DexScreener ke fail hone par baaki sources try karo (order = fastest first)."""
    if not ca or not EXTRA_SOURCES_ENABLED:
        return None
    chains = _alt_chains_for(ca) or ["SOL"]
    # Birdeye first ONLY for chain-unambiguous addresses (SOL/TON) — it's a
    # paid, independently-indexed source and far more reliable than the free
    # geckoterminal/dexpaprika endpoints (which often return mc=N/A for
    # fresh/low-cap tokens). For EVM (0x...) addresses the chain itself is a
    # guess across ETH/BSC/Base/Robinhood, and Birdeye just takes the first
    # chain with any data — which can be a wrong/unrelated token on a
    # different chain with a very different MC. So for those, try the
    # chain-neutral sources first and treat Birdeye as a last resort there.
    is_evm_ambiguous = ca.startswith("0x") and len(ca) in (42, 66)
    order = ((_src_geckoterminal, _src_dexpaprika, _src_birdeye,
              _src_moralis, _src_coinmarketcap) if is_evm_ambiguous else
             (_src_birdeye, _src_geckoterminal, _src_dexpaprika,
              _src_moralis, _src_coinmarketcap))
    # SPEED_NOV12: all alt sources start at once (was: strictly sequential,
    # so a slow first source blocked every later one — up to 5 x 8s = 40s).
    # Results are still taken in the same priority order: the first source
    # (by priority) with data wins, exactly as before. Only the wall time
    # changes, capped by a shared 10s deadline. Stragglers are left to
    # finish in the background (shutdown(wait=False)) so an early hit is
    # never delayed by a slow lower-priority source.
    try:
        import concurrent.futures as _cf
        _deadline = time.time() + 10
        _ex = _cf.ThreadPoolExecutor(max_workers=len(order))
        try:
            _futs = [_ex.submit(fn, ca, chains) for fn in order]
            for _fn, _f in zip(order, _futs):
                try:
                    res = _f.result(timeout=max(0.1, _deadline - time.time()))
                except Exception as e:
                    logger.debug(f"alt source {getattr(_fn, '__name__', '?')} error: {e}")
                    res = None
                if res and (res.get("price", 0) > 0 or res.get("mcap", 0) > 0 or res.get("symbol")):
                    logger.info(f"🛰️ Fallback source {res.get('_source')} hit for {ca[:12]}… "
                                f"({res.get('symbol','?')} mc={res.get('mcap_fmt','N/A')})")
                    return res
        finally:
            _ex.shutdown(wait=False)
    except Exception as e:
        logger.debug(f"alt sources parallel run failed: {e}")
    return None

# --- extracted: _fetch_dex_sync ---
def _fetch_dex_sync(ca, retries=2, min_liquidity=500, allow_alt=True, prefer_chain=None):
    """Fetch live token data from DexScreener.

    Order of operations:
      1. Fresh positive cache hit (< 20s)  -> return instantly
      2. Fresh negative cache hit (< 45s)  -> return None without a request
      3. /latest/dex/tokens/{ca}           -> primary, all chains
      4. /latest/dex/search?q={ca}         -> fallback for freshly-created pairs
      5. 429 -> exponential backoff, then retry
    """
    import time as _time
    if not ca:
        return None

    cached = _dex_cache.get(ca)
    if cached:
        result, exp = cached
        if _time.time() < exp:
            return result

    miss_exp = _dex_miss_cache.get(ca)
    if miss_exp and _time.time() < miss_exp:
        return None

    for attempt in range(max(1, retries)):
        data = _dex_get(f"https://api.dexscreener.com/latest/dex/tokens/{ca}")
        if data == 429:
            # Rate-limited (breaker _dex_get me trip ho chuka hai). DexScreener
            # ko sleep kar ke dobara MAT bajao — wahi backoff storms thay jo
            # ticks wedge kar ke calls skip kara rahe thay. Seedha doosri dex
            # sources se data lo.
            logger.warning(f"DexScreener 429 (ca={ca[:12]}…) — alt dex sources try ho rahi hain")
            alt429 = _fetch_alt_sources_sync(ca) if allow_alt else None
            if alt429:
                _dex_cache[ca] = (alt429, _time.time() + _DEX_CACHE_TTL)
                _dex_miss_cache.pop(ca, None)
                _dex_cache_gc()
                return alt429
            # Alt bhi fail — 5s negative cache taake koi storm na bane.
            _dex_miss_cache[ca] = _time.time() + 5
            _dex_cache_gc()
            return None

        pairs = (data or {}).get("pairs") or []

        # Fallback: brand-new pairs sometimes only show on the search endpoint.
        if not pairs:
            sdata = _dex_get(f"https://api.dexscreener.com/latest/dex/search?q={ca}")
            if sdata and sdata != 429:
                pairs = [
                    p for p in (sdata.get("pairs") or [])
                    if ca.lower() in (
                        (p.get("baseToken", {}).get("address", "") or "").lower(),
                        (p.get("quoteToken", {}).get("address", "") or "").lower(),
                    )
                ]

        if pairs:
            result = _parse_dex_pairs(pairs, min_liquidity, ca, prefer_chain=prefer_chain)
            if result is None:
                # Pairs exist but all below the liquidity floor — retry without it
                # so the UI still shows a live price instead of "N/A".
                result = _parse_dex_pairs(pairs, 0, ca, prefer_chain=prefer_chain)
        else:
            result = None

        # The /latest endpoint caps its answer at 30 pairs, so a multi-chain
        # token can come back with zero pairs on a chain we support. Ask the
        # chain-scoped v1 endpoint directly for the chains this CA can live on.
        if not result:
            scoped = []
            for chain_slug in _dex_candidate_chains(ca):
                cdata = _dex_get(f"https://api.dexscreener.com/tokens/v1/{chain_slug}/{ca}", timeout=10)
                if cdata == 429:
                    break
                if isinstance(cdata, list) and cdata:
                    scoped.extend(cdata)
            if scoped:
                result = (_parse_dex_pairs(scoped, min_liquidity, ca, prefer_chain=prefer_chain)
                          or _parse_dex_pairs(scoped, 0, ca, prefer_chain=prefer_chain))

        if True:
            if result:
                _dex_cache[ca] = (result, _time.time() + _DEX_CACHE_TTL)
                _dex_miss_cache.pop(ca, None)
                _dex_cache_gc()
                return result

        # ── PAIR-ADDRESS FALLBACK ────────────────────────────────────────────
        # Bohat se KOLs (jaise @Big_Whales_Call) dexscreener ka PAIR/POOL link
        # post karte hain: dexscreener.com/solana/<pairAddress>. Wo address
        # token mint nahi hota, is liye /latest/dex/tokens/<addr> khali aata
        # tha aur poori call "no dex data yet" keh kar skip ho jati thi.
        # Yahan us address ko pairs endpoint se resolve kar ke asli baseToken
        # mint nikalte hain, phir usi se normal lookup chalta hai.
        if not result:
            # WIZCHAIN_NOV23: the resolver now returns (token, chain_slug).
            # The pair's chain is the call's chain — pass it down so the
            # token lookup cannot wander off to another chain's pair.
            _tok, _pair_slug = _resolve_pair_to_token(ca)
            if _tok and _tok.lower() != (ca or "").lower():
                logger.info(f"🔗 Pair address resolved: {ca[:10]}… → token {_tok[:10]}…")
                result = _fetch_dex_sync(_tok, retries=1,
                                         min_liquidity=min_liquidity,
                                         allow_alt=allow_alt,
                                         prefer_chain=_pair_slug or prefer_chain)
                if result:
                    result["resolved_ca"] = _tok

        if result:
            _dex_cache[ca] = (result, _time.time() + _DEX_CACHE_TTL)
            _dex_miss_cache.pop(ca, None)
            _dex_cache_gc()
            return result

        # DexScreener has nothing — try the slower alternate providers only on
        # normal lookup/new-call paths. The 1-second monitoring loop passes
        # allow_alt=False so fallback APIs cannot wedge instant alerts.
        alt = _fetch_alt_sources_sync(ca) if allow_alt else None
        if alt:
            _dex_cache[ca] = (alt, _time.time() + _DEX_CACHE_TTL)
            _dex_miss_cache.pop(ca, None)
            _dex_cache_gc()
            return alt

        # Not indexed anywhere yet -> short negative cache, no more retries this round.
        _dex_miss_cache[ca] = _time.time() + _DEX_MISS_TTL
        _dex_cache_gc()
        return None

    return None

# --- extracted: fetch_dexscreener ---
async def fetch_dexscreener(ca, *, allow_alt=True, retries=2, min_liquidity=500):
    return await asyncio.to_thread(_fetch_dex_sync, ca, retries, min_liquidity, allow_alt)

# --- extracted: prepare_owner_emojis ---
def prepare_owner_emojis(text: str):
    """Userbot path. Convert the owner's own premium-emoji syntax
    (`[[emoji:ID]]` or `<tg-emoji emoji-id="ID">x</tg-emoji>`) into plain 🔮
    markers, remembering which marker index must use which explicit ID.

    Returns (html_text_with_markers, {marker_index: emoji_id}).
    """
    if not text:
        return text, {}
    out = []
    explicit = {}
    idx = 0          # running count of 🔮 markers already emitted
    pos = 0
    pattern = re.compile(
        r"\[\[emoji:(?P<a>\d{5,25})\]\]"
        r'|<tg-emoji\s+emoji-id="(?P<b>\d{5,25})"\s*>(?P<inner>.*?)</tg-emoji>',
        re.IGNORECASE | re.DOTALL)
    for m in pattern.finditer(text):
        chunk = text[pos:m.start()]
        out.append(chunk)
        idx += chunk.count("🔮")
        eid = m.group("a") or m.group("b")
        explicit[idx] = int(eid)
        out.append("🔮")
        idx += 1
        pos = m.end()
    tail = text[pos:]
    out.append(tail)
    return "".join(out), explicit

# --- extracted: _build_premium_entities ---
def _build_premium_entities(plain_text, base_entities, emoji_ids, forced_pack=None,
                            explicit=None):
    """Replace each 🔮 in plain_text with the next emoji_id from the list.
    emoji_ids may be a single str/int or a list. Falls back to last id when list runs out.

    Special-cases (in priority order):
    1. 🔮KOL / 🔮BOT  → pack kol/bot emoji (always, regardless of position)
    2. 🔮X or 🔮 X    → pack x emoji (locked — works even if template edited)
    3. "Champion KOL - Rank # 🔮" / "Leaderboard KOL - Rank # 🔮"
       → us KOL ki rank numbering premium emoji (auto-detected)
    4. (reserved)
    5. MC separator 🔮 (between entry MC and current MC) → TEER_EMOJI_ID=5346105514575025401 (locked)
    6. all other 🔮s  → next item from non-badge portion of emoji_ids

    Badge emojis (star + rank, appended at end of emoji_ids by send_alert) are
    auto-detected from the text so they NEVER get consumed by template 🔮 placeholders,
    even when the owner edits the template with extra 🔮s.
    """
    from telethon.tl.types import MessageEntityCustomEmoji
    if not isinstance(emoji_ids, list):
        emoji_ids = [emoji_ids]
    emoji_ids = [e for e in emoji_ids if e]
    # Promo link text (set via /setpromolink) — its 🔮 must ALWAYS render
    # PROMO_EMOJI_ID, never a positional id.
    _promo_txt = ""
    _promo_emoji_id = PROMO_EMOJI_ID
    try:
        _p = load_config().get("promo_link") or {}
        if _p.get("text"):
            _promo_txt = html.unescape(str(_p["text"])).strip()
        if _p.get("emoji_id"):
            _promo_emoji_id = int(_p["emoji_id"])
    except Exception:
        _promo_txt = _promo_txt or ""

    pack = forced_pack if forced_pack is not None else _get_alert_pack()
    kol_id = pack.get("kol")
    bot_id = pack.get("bot")
    mae_id = pack.get("mae") or 0
    if not emoji_ids and not kol_id and not bot_id and not mae_id:
        return base_entities or []

    # ── Badge auto-detection ───────────────────────────────────────────────────
    # Naya badge format (hamesha sabse aakhir me lagta hai):
    #   "Champion KOL - Rank # 🔮"  /  "Leaderboard KOL - Rank # 🔮"
    # 2 badge emojis hoti hain: pehli = lead/crystal, dusri = rank number emoji.
    # Leaderboard ki pehli = pack crystal (MC line wali hi, X range ke color me).
    #   non_badge_ids — consumed by regular template 🔮s via pos_index
    #   badge_ids     — reserved for the rank 🔮, never consumed by template 🔮s
    _badge_type = None
    if 'LEAD KOL - Rank #' in plain_text:
        _badge_type = 'leaderboard'
    elif 'CHAMP KOL - Rank #' in plain_text:
        _badge_type = 'champion'
    badge_count = 2 if _badge_type else 0  # champion: [lead, rank] · leaderboard: [crystal, rank]

    if badge_count and len(emoji_ids) >= badge_count:
        non_badge_ids = emoji_ids[:-badge_count]
        badge_ids     = emoji_ids[-badge_count:]
    else:
        non_badge_ids = emoji_ids
        badge_ids     = []

    # ── Dropped-call STRUCTURAL role map (language independent) ───────────────
    # FIX: the 1️⃣/➡️ lock above only recognised English/Chinese/Vietnamese
    # label text, so every other language (Turkish, Russian, Hindi, Arabic,
    # Spanish, ...) fell through to the positional chain-emoji list and the
    # chain emoji showed up 8x instead of 2x. Roles are now derived from the
    # template LAYOUT instead of label wording: the first line holding 🔮 is
    # the header (chain emoji), and every later line holding exactly two 🔮 is
    # a "1️⃣ label ➡️ value" row → (NUM, ARROW). MAE/KOL/BOT lines have a
    # single 🔮 so they are untouched.
    _drop_roles = {}
    if pack.get('name') == 'dropped':
        _off = 0
        _hdr_seen = False
        for _ln in plain_text.split('\n'):
            _pos = [_off + _m for _m in range(len(_ln)) if _ln.startswith('🔮', _m)]
            if _pos:
                if not _hdr_seen:
                    _hdr_seen = True
                elif len(_pos) == 2:
                    _drop_roles[_pos[0]] = DROPPED_NUM_EMOJI
                    _drop_roles[_pos[1]] = DROPPED_ARROW_EMOJI
            _off += len(_ln) + 1

    custom_entities = []
    explicit = explicit or {}
    pos_index = 0
    marker_idx = 0
    utf16_off = 0
    i = 0
    PLACEHOLDER = '🔮'
    PH_LEN = len(PLACEHOLDER)
    emoji_u16 = len(PLACEHOLDER.encode('utf-16-le')) // 2
    _badge_rank_pending = False  # True after we emitted the badge star emoji

    while i < len(plain_text):
        if plain_text[i:i+PH_LEN] == PLACEHOLDER:
            rest = plain_text[i+PH_LEN:]
            eid  = None
            _this_marker = marker_idx
            marker_idx += 1

            if _this_marker in explicit and i not in _drop_roles:
                # (dropped-call 1️⃣/➡️ slots are locked structurally — a stored lang template
                #  carrying the chain emoji id there must NOT win, that was the 8x bug)
                # Owner ne khud is jagah premium emoji ID di hai — wahi use hogi.
                eid = explicit[_this_marker]

            elif _promo_txt and rest.lstrip(' ').startswith(_promo_txt):
                # 🔮 <promo text> → owner ka chuna hua premium emoji
                # (/setpromolink me di gayi emoji ID), warna default.
                eid = _promo_emoji_id

            elif rest.startswith('KOL') and kol_id:
                # 🔮KOL → pack kol emoji
                eid = kol_id

            elif rest.startswith('BOT') and bot_id:
                # 🔮BOT → pack bot emoji
                eid = bot_id

            elif mae_id and rest.startswith('MAE'):
                # 🔮MAE → pack mae (Maestro) emoji — replaces the old 🔮X slot
                eid = mae_id

            elif rest.lstrip(' ').startswith('Caller') and pack.get('caller'):
                # 🔮Caller → pack caller emoji (X-alert in-text link row)
                # .lstrip(' ') tolerates "🔮 Caller" (with space) too, not just "🔮Caller"
                eid = pack.get('caller')

            elif rest.lstrip(' ').startswith('Twitter') and pack.get('twitter'):
                eid = pack.get('twitter')

            elif rest.lstrip(' ').startswith('Trade') and pack.get('trade'):
                eid = pack.get('trade')

            elif rest.lstrip(' ').upper().startswith('DEX') and pack.get('dex'):
                eid = pack.get('dex')

            elif rest.lstrip(' ').startswith('Details') and pack.get('details'):
                eid = pack.get('details')

            elif rest.lstrip(' ').startswith('Channel') and pack.get('channel'):
                eid = pack.get('channel')

            elif rest.lstrip(' ').startswith('Chart') and pack.get('chart'):
                # 🔮Chart → pack chart emoji (dropped-call link row, orange / green+purple themes)
                eid = pack.get('chart')

            elif rest.lstrip(' ').startswith('Post') and pack.get('post'):
                # 🔮Post → pack post emoji (dropped-call link row, red theme)
                eid = pack.get('post')

            elif rest.lstrip(' ').upper().startswith('DEV') and pack.get('dev'):
                eid = pack.get('dev')

            elif (_badge_type == 'champion' and len(badge_ids) >= 2
                  and rest.lstrip(' ').startswith('CHAMP KOL')):
                # "🔮 CHAMP KOL - Rank # 🔮" → leading badge emoji
                eid = badge_ids[0]

            elif (_badge_type == 'leaderboard' and len(badge_ids) >= 2
                  and rest.lstrip(' ').startswith('LEAD KOL')):
                # "🔮 LEAD KOL - Rank # 🔮" → leading badge emoji = pack crystal
                # (MC line wali hi crystal, X range ke color me)
                eid = badge_ids[0]

            elif badge_ids and plain_text[:i].endswith('Rank # '):
                # "🔮 CHAMP KOL - Rank # 🔮" / "Leaderboard KOL - Rank # 🔮"
                # → us KOL ki rank numbering premium emoji (badge ka aakhri id)
                eid = badge_ids[-1]

            elif i in _drop_roles:
                # Language-independent dropped-call lock (see _drop_roles above)
                eid = _drop_roles[i]

            elif (pack.get('name') == 'dropped' and rest.startswith(
                    ('Token Symbol', 'Current MC', 'Chain Symbol',
                     '代币符号', '当前市值', '链符号',
                     'Ký hiệu Token', 'Vốn hóa hiện tại', 'Ký hiệu Chain'))):
                # HARD LOCK (dropped-call only): the "1️⃣" before these three
                # labels must always be DROPPED_NUM_EMOJI — never the
                # positional chain-emoji list. This is a context lock (not
                # dependent on the `explicit` map), so it self-heals even if
                # the stored dropped_call_template ever loses/corrupts its
                # saved tg-emoji IDs (that corruption is exactly what made
                # this emoji show as the chain emoji 8x instead of 2x).
                # FIX: originally only matched the English label text, so a
                # Chinese/Vietnamese/Turkish/Russian-tagged dropped-call template (translated
                # labels) never tripped this lock at all — EVERY 🔮 in that
                # template (header + all 3 label/arrow pairs) fell through to
                # the generic 2-item chain rotation instead, which is exactly
                # what produced "chain emoji 8x instead of 2x".
                eid = DROPPED_NUM_EMOJI

            elif (pack.get('name') == 'dropped' and plain_text[:i].rstrip().endswith(
                    ('Token Symbol', 'Current MC', 'Chain Symbol',
                     '代币符号', '当前市值', '链符号',
                     'Ký hiệu Token', 'Vốn hóa hiện tại', 'Ký hiệu Chain'))):
                # HARD LOCK (dropped-call only): the "➡️" right after those
                # same three labels must always be DROPPED_ARROW_EMOJI.
                # Same Chinese/Vietnamese label fix as above.
                eid = DROPPED_ARROW_EMOJI

            else:
                # ── MC separator lock ─────────────────────────────────────────
                # The 🔮 between entry MC and current MC must ALWAYS be TEER_EMOJI_ID.
                # Detect by context: preceded by MC value (ends K/M/B/digit) and
                # followed (after spaces) by $ or digit.
                before_stripped = plain_text[:i].rstrip()
                after_stripped  = rest.lstrip()
                # Require a REAL number token right before the 🔮 (start-of-
                # string or whitespace immediately before the digit run),
                # not just "last character is a digit". A bare last-char
                # check false-positives on anything ending in a digit that
                # ISN'T a market-cap value — e.g. a KOL username like
                # "@musterseed99" — which mis-fires the teer lock, skips
                # advancing pos_index, and desyncs every 🔮 after it (this is
                # exactly what flipped the number-badge/arrow order in the
                # leaderboard from rank 4 onward).
                if (before_stripped
                        and _MC_VALUE_BEFORE_RE.search(before_stripped)
                        and after_stripped
                        and after_stripped[0] in '$0123456789'):
                    eid = _pack_teer(pack)
                    # NOTE: pos_index NOT advanced — teer slot is not counted against
                    # the positional list, keeping badge emoji alignment correct.
                elif non_badge_ids:
                    if pos_index >= len(non_badge_ids):
                        # DIAG: this 🔮 didn't match ANY special case above
                        # (Caller/Twitter/Trade/DEX/Details/DEV/KOL/BOT/MAE/
                        # badge/teer-lock) and the positional list has run
                        # out, so it's about to repeat the LAST id (teer) —
                        # this is exactly the "wrong emoji on custom-template
                        # button labels" bug. Logging the text right after
                        # the 🔮 pinpoints which label text failed to match.
                        logger.warning(
                            f"🔮 emoji placeholder #{pos_index} fell through to "
                            f"positional fallback (reusing last id) — text after "
                            f"placeholder: {rest[:20]!r}")
                    eid = non_badge_ids[min(pos_index, len(non_badge_ids) - 1)]
                    pos_index += 1

            if eid:
                custom_entities.append(MessageEntityCustomEmoji(
                    offset=utf16_off, length=emoji_u16,
                    document_id=int(eid)
                ))
            utf16_off += emoji_u16
            i += PH_LEN
            continue
        char_u16 = len(plain_text[i].encode('utf-16-le')) // 2
        utf16_off += char_u16
        i += 1
    return (base_entities or []) + custom_entities

# --- extracted: _tl_buttons_from_keyboard ---
def _tl_buttons_from_keyboard(keyboard):
    """PTB InlineKeyboardMarkup → Telethon ReplyInlineMarkup (URL buttons only)."""
    if not keyboard:
        return None
    try:
        from telethon.tl.types import ReplyInlineMarkup, KeyboardButtonRow, KeyboardButtonUrl
        rows = []
        for row in keyboard.inline_keyboard:
            btns = [KeyboardButtonUrl(text=b.text, url=b.url) for b in row if getattr(b, "url", None)]
            if btns:
                rows.append(KeyboardButtonRow(buttons=btns))
        return ReplyInlineMarkup(rows=rows) if rows else None
    except Exception as e:
        logger.warning(f"keyboard convert failed: {e}")
        return None

# --- extracted: _userbot_send_with_premium_emoji ---
async def _userbot_send_with_premium_emoji(chat, text, emoji_id=None, link_preview=False, forced_pack=None, keyboard=None):
    """Send via userbot (round-robin across 2 accounts + failover).
    Returns sent Message object on success, None on failure."""
    clients = _userbot_clients()
    if not clients:
        return None
    picked = _pick_userbot()
    ordered = [picked] + [cl for cl in clients if cl is not picked]
    tl_buttons = _tl_buttons_from_keyboard(keyboard)
    text, _explicit = prepare_owner_emojis(text)
    for ub in ordered:
        try:
            if emoji_id or _explicit:
                try:
                    from telethon.extensions.html import parse as tl_html_parse
                    plain_text, base_entities = tl_html_parse(text)
                    all_entities = _build_premium_entities(plain_text, base_entities, emoji_id,
                                                           forced_pack=forced_pack, explicit=_explicit)
                    msg = await ub.send_message(
                        chat, plain_text,
                        formatting_entities=all_entities,
                        link_preview=link_preview,
                        buttons=tl_buttons
                    )
                    return msg
                except Exception as e:
                    logger.warning(f"Custom emoji send failed, fallback: {e}")
                    if tl_buttons is not None:
                        # Buttons zaroori hain (X alert) — is client se nahi gaye
                        # to agle client pe try karo; dono fail hon to None.
                        continue
            msg = await ub.send_message(chat, text, parse_mode="html",
                                        link_preview=link_preview, buttons=tl_buttons)
            return msg
        except Exception as e:
            logger.warning(f"userbot send failed on one client, trying next: {e}")
            continue
    logger.error("userbot send: all clients failed")
    return None

# --- extracted: _MEDIA_FILE_CACHE ---
_MEDIA_FILE_CACHE = {}

# --- extracted: _MEDIA_TL_CACHE ---
_MEDIA_TL_CACHE   = {}

# --- extracted: _media_cache_dir ---
def _media_cache_dir():
    try:
        os.makedirs(_MEDIA_CACHE_DIR, exist_ok=True)
        return _MEDIA_CACHE_DIR
    except Exception:
        os.makedirs("/tmp/wizard_media", exist_ok=True)
        return "/tmp/wizard_media"

# --- extracted: invalidate_media_cache ---
def invalidate_media_cache(file_id=None):
    """Owner naya media set kare to purana cache foran chhod do."""
    if file_id:
        _MEDIA_TL_CACHE.pop(file_id, None)
        _MEDIA_FILE_CACHE.pop(file_id, None)
    else:
        _MEDIA_TL_CACHE.clear()
        _MEDIA_FILE_CACHE.clear()

# --- extracted: _userbot_send_media_with_emoji ---
async def _userbot_send_media_with_emoji(bot_app, chat, file_id, file_type, text, emoji_id, keyboard=None, forced_pack=None, _retry=True):
    """Send media via userbot with premium emoji — NO edit = NO pencil mark.
    Media ek dafa download/upload hoti hai, phir cache se instant."""
    import os
    tmp_path = None
    # FIX (2026-10-13): keep the ORIGINAL text (with <tg-emoji> tags). The code
    # below overwrites `text` with the placeholder version; if the send fails with
    # "stale file reference" the retry used to get that already-converted text, so
    # prepare_owner_emojis() found no tags -> every placeholder fell back to the
    # positional id list (CALL 1/2/3 got the banner's last tile instead of their emoji).
    _orig_text = text
    try:
        cached_media = _MEDIA_TL_CACHE.get(file_id)
        # Stale cache: a photo cached for a video/gif (from the .jpg bug) must
        # not be reused — drop it so we re-download and send as video.
        if (cached_media is not None and file_type in ('video', 'gif')
                and getattr(cached_media, 'photo', None) is not None
                and getattr(cached_media, 'document', None) is None):
            cached_media = None
        if cached_media is None:
            # GIF/animation is an MP4 video too — a .jpg suffix made Telethon
            # send it as a PHOTO (the "video posts as a photo" bug). Only real
            # photos get .jpg.
            _suffix = '.mp4' if file_type in ('video', 'gif') else '.jpg'
            tmp_path = _MEDIA_FILE_CACHE.get(file_id)
            # Stale on-disk cache: an old .jpg file cached for a gif/video
            # must be re-downloaded with the right suffix.
            if tmp_path and not tmp_path.endswith(_suffix):
                tmp_path = None
            if not tmp_path or not os.path.exists(tmp_path):
                suffix = _suffix
                safe_id = re.sub(r'[^A-Za-z0-9_-]', '', str(file_id))[-64:]
                tmp_path = os.path.join(_media_cache_dir(), f"{safe_id}{suffix}")
                tg_file = await bot_app.get_file(file_id)
                await tg_file.download_to_drive(tmp_path)
                _MEDIA_FILE_CACHE[file_id] = tmp_path

        from telethon.extensions.html import parse as tl_html_parse
        text, _explicit = prepare_owner_emojis(text)
        plain_text, base_ents = tl_html_parse(text)
        all_entities = _build_premium_entities(plain_text, base_ents, emoji_id,
                                               forced_pack=forced_pack, explicit=_explicit)

        # Convert PTB InlineKeyboardMarkup → Telethon buttons
        tl_buttons = None
        if keyboard:
            try:
                from telethon.tl.types import ReplyInlineMarkup, KeyboardButtonRow, KeyboardButtonUrl
                rows = []
                for row in keyboard.inline_keyboard:
                    btns = [KeyboardButtonUrl(text=b.text, url=b.url) for b in row if b.url]
                    if btns:
                        rows.append(KeyboardButtonRow(buttons=btns))
                if rows:
                    tl_buttons = ReplyInlineMarkup(rows=rows)
            except Exception:
                pass

        # Round-robin across 2 userbot accounts + failover (ek fail/timeout
        # ho to dusra try karo — is se aaj subah wale "Timed out" masle pe
        # bhi dusra account backup deta hai)
        _clients = _userbot_clients()
        if not _clients:
            raise RuntimeError("no userbot client connected")
        _picked = _pick_userbot()
        _ordered = [_picked] + [cl for cl in _clients if cl is not _picked]
        msg = None
        _last_err = None
        for _ub in _ordered:
            try:
                _send_kwargs = dict(
                    caption=plain_text,
                    formatting_entities=all_entities,
                    buttons=tl_buttons,
                    supports_streaming=True,
                )
                if file_type == 'gif':
                    # Send as a looping GIF/animation, not a still photo.
                    from telethon.tl.types import DocumentAttributeAnimated
                    _send_kwargs['attributes'] = [DocumentAttributeAnimated()]
                msg = await _ub.send_file(
                    chat, cached_media if cached_media is not None else tmp_path,
                    **_send_kwargs
                )
                break
            except Exception as _se:
                _last_err = _se
                logger.warning(f"Userbot media send failed on one client, trying next: {_se}")
                continue
        if msg is None:
            raise _last_err if _last_err else RuntimeError("userbot media send failed")
        # Pehli kamyab upload ke baad Telegram ka media reference cache kar lo —
        # agli dafa upload bilkul nahi hoga (instant post).
        if cached_media is None and msg is not None:
            try:
                if getattr(msg, "media", None) is not None:
                    _MEDIA_TL_CACHE[file_id] = msg.media
            except Exception:
                pass
        logger.info(f"✅ Userbot media send SUCCESS (no pencil) file_type={file_type} "
                    f"cached={cached_media is not None}")
        return msg
    except Exception as e:
        # Stale media reference (file reference expired) -> cache clear karke
        # agli koshish fresh upload se hogi. FIX: pehle sirf cache clear hota
        # tha lekin usi call mein give up ho jata tha -> seedha Bot API (no
        # emoji) fallback. Ab cache clear hote hi FORAN ek retry (fresh
        # download+upload ke saath) hota hai, taake premium emoji wala send
        # hi succeed ho jaye is call mein — Bot API tak jaana hi na pade.
        if _retry and ("reference" in str(e).lower() or "FILE_REFERENCE" in str(e)):
            invalidate_media_cache(file_id)
            logger.warning(f"Userbot media send: stale file reference, retrying fresh once: {e}")
            return await _userbot_send_media_with_emoji(
                bot_app, chat, file_id, file_type, _orig_text, emoji_id,
                keyboard=keyboard, forced_pack=forced_pack, _retry=False)
        logger.error(f"Userbot media send failed: {e}")
        return None

# --- extracted: _ranking_channels ---
def _ranking_channels():
    """Har woh channel jiski calls track ho rahi hain — chahe woh channels.json
    me na ho. Pehle sirf load_channels() use hota tha, isliye jin KOLs ki calls
    X jati thi magar entry list me case/@ mismatch tha, woh leaderboard aur
    champions dono se ghayab rehte the."""
    seen, out = set(), []
    removed = load_removed_channels()
    for ch in load_channels():
        c = str(ch).lstrip("@")
        if c.lower() in removed or c.lower() in seen:
            continue
        seen.add(c.lower()); out.append(c)
    for call in tracked_calls.values():
        c = str(call.get("channel", "")).lstrip("@")
        if not c or c.lower() in removed or c.lower() in seen:
            continue
        seen.add(c.lower()); out.append(c)
    return out

# --- extracted: champion_total_x ---
def champion_total_x(channel, champ_snap=None, excluded_keys=None):
    """Sum each qualifying call's best X (2X + 2X = 4X) for Champions."""
    champ_snap = champ_snap or {}
    excluded_keys = excluded_keys or set()
    total = 0
    best_call_key = None
    best_call_x = 0
    for call_key, call in tracked_calls.items():
        if call.get("channel", "").lower() != channel.lower() or _is_deleted_blocked(call):
            continue
        # Frozen calls (via /freezecall) must NOT contribute their X to
        # Champions — freezing a suspicious/fake call should retroactively
        # remove its 100x etc. from the channel's champion total, not just
        # stop future milestone alerts.
        if call.get("frozen"):
            continue
        # /removechampion exclusion is authoritative — it wins over the
        # reset snapshot below, so a removed channel stays out even if the
        # call is also in the snapshot.
        if call_key in excluded_keys:
            continue
        if call_key in champ_snap:
            levels = [m for m in sent_milestones.get(call_key, set()) if m > champ_snap[call_key]]
        else:
            levels = list(sent_milestones.get(call_key, set()))
        # Only count milestones that have a real WizardScan post backing them.
        # A milestone in sent_milestones with no post is a phantom (fake X from
        # the old ratio bug, or a post that was never recorded) — it must not
        # inflate Champions, and it would show with no link.
        _posts = milestone_posts.get(call_key, {}) or {}
        levels = [m for m in levels if str(m) in _posts]
        if not levels:
            continue
        call_x = display_x(max(levels))
        if call_x < 2:
            continue
        total += call_x
        if call_x > best_call_x:
            best_call_x = call_x
            best_call_key = call_key
    return total, best_call_key

# --- extracted: get_call_history ---
def get_call_history(channel, chain_filter=None, top=False):
    calls = []
    # OWNER CHANGE (2026-10-07): channel record is LIFETIME — the old 90-day
    # cutoff is gone (dead tokens leave live tracking but stay in the record).
    cutoff = None
    # DIAG: counts so an empty /history result is diagnosable from Railway logs
    # instead of a silent "No calls found" with no way to tell why.
    _matched_channel = 0
    _excluded_chain   = 0
    _excluded_stale   = 0
    _excluded_incomplete = 0
    for call_key, call in tracked_calls.items():
        if call.get("channel","").lower() != channel.lower(): continue
        _matched_channel += 1
        if chain_filter and call.get("chain","").upper() != chain_filter.upper():
            _excluded_chain += 1
            continue
        if not top and cutoff:
            ts = call.get("tracked_since","")
            if ts:
                try:
                    if datetime.fromisoformat(ts) < cutoff:
                        _excluded_stale += 1
                        continue
                except Exception: pass
        milestones = list(sent_milestones.get(call_key, set()))
        # Milestones held back because no media was set for that X level are still
        # part of the channel's real record.
        try:
            milestones += [int(x) for x in pending_media_alerts.get(call_key, set())]
        except Exception:
            pass
        highest_x  = max(milestones) if milestones else 0
        entry_mc   = call.get("entry_mc", 0)
        # Use live ratio for decimal display; floor at highest milestone so crashes don't hide peaks
        # Real ATH multiplier (peak ever recorded), not the current live price
        last_ratio  = call_peak_ratio(call)
        # Safety cap: never display a ratio higher than MAX_MILESTONE × 2 to prevent phantom billions
        last_ratio  = min(last_ratio, MAX_MILESTONE * 2)
        # For display: only trust last_ratio if it's close to the confirmed milestone.
        # Phantom ratios from DexScreener glitches (e.g. 105K MC token showing billions)
        # must not inflate the displayed X or MC.
        if highest_x > 0 and last_ratio > highest_x * 3:
            current_x = float(highest_x)   # use confirmed milestone, ignore phantom ratio
        else:
            current_x = round(max(last_ratio, float(highest_x) if highest_x > 0 else 1.0), 2)
        if current_x < 1.0: current_x = 1.0
        # Cap display MC: if entry_mc × ratio produces an absurd number, use confirmed milestone MC
        current_mc  = entry_mc * current_x if entry_mc > 0 else entry_mc
        try:
            _pmc = float(call.get("peak_mc", 0) or 0)
        except (TypeError, ValueError):
            _pmc = 0.0
        if _pmc > current_mc and _pmc < 1_000_000_000:
            current_mc = _pmc
        if current_mc > 1_000_000_000 and highest_x > 0:  # cap at $1B (tightened from $10B)
            current_mc = entry_mc * highest_x  # use confirmed milestone MC, not inflated ratio
        # FIX (2026-10-18): a call that already reached 2x+ (X alert was sent) is ALWAYS
        # part of the record, even if some data field is missing/incomplete.
        if highest_x < 2 and not _complete_history_call(call, highest_x, current_x, current_mc):
            _excluded_incomplete += 1
            if _excluded_incomplete <= 5:  # cap noise — first few examples are enough to diagnose
                logger.info(
                    f"🔍 /history @{channel}: excluded {call_key} — "
                    f"ca={bool(call.get('ca'))} msg_id={call.get('msg_id')!r} "
                    f"symbol={call.get('symbol')!r} chain={call.get('chain')!r} "
                    f"entry_mc={entry_mc} current_mc={current_mc} "
                    f"entry_fmt={call.get('entry_fmt')!r} dex_pending={call.get('dex_pending')}")
            continue
        calls.append({
            "symbol":         call.get("symbol","TOKEN") or "TOKEN",
            "chain":          call.get("chain",""),
            "ca":             call.get("ca",""),
            "msg_id":         call.get("msg_id", 0),
            "entry_mc":       entry_mc,
            "entry_fmt":      call.get("entry_fmt","N/A"),
            "highest_x":      highest_x,       # integer milestone — used for sorting
            "current_x":      current_x,       # real decimal X — used for display
            "highest_mc_fmt": fmt_mc(current_mc) if current_mc > 0 else call.get("entry_fmt","N/A"),
            "tracked_since":  call.get("tracked_since",""),
            "post_deleted":   bool(call.get("post_deleted")),
        })
    if _matched_channel == 0:
        logger.info(f"🔍 /history @{channel}: 0 tracked_calls have channel=={channel!r} "
                     f"(total tracked_calls in memory: {len(tracked_calls)})")
    elif not calls:
        logger.info(f"🔍 /history @{channel}: {_matched_channel} calls matched channel, "
                     f"but 0 passed filters — chain_excluded={_excluded_chain} "
                     f"stale_excluded={_excluded_stale} incomplete_excluded={_excluded_incomplete}")

    # ── Merge in permanently-archived (pruned/rugged) calls ──────────────────
    # OWNER REQUIREMENT: a rugged token's active-tracking data is fine to
    # delete, but /history must keep showing it. Archived entries had their
    # companion dicts (sent_milestones etc.) already wiped by the prune job,
    # so their display fields are read directly from the archive snapshot
    # instead of being recomputed live.
    try:
        archive = load_call_archive()
        for call_key, arc in archive.items():
            if call_key in tracked_calls:
                continue  # live entry already covered above, avoid duplicate
            if (arc.get("channel") or "").lower() != channel.lower():
                continue
            if chain_filter and (arc.get("chain") or "").upper() != chain_filter.upper():
                continue
            if not top and cutoff:
                ts = arc.get("tracked_since", "")
                if ts:
                    try:
                        if datetime.fromisoformat(ts) < cutoff:
                            continue
                    except Exception:
                        pass
            entry_mc  = arc.get("entry_mc", 0) or 0
            highest_x = int(arc.get("best_x", 0) or 0)
            current_mc = entry_mc * highest_x if entry_mc > 0 and highest_x > 0 else entry_mc
            calls.append({
                "symbol":         arc.get("symbol", "TOKEN") or "TOKEN",
                "chain":          arc.get("chain", ""),
                "ca":             arc.get("ca", ""),
                "msg_id":         arc.get("msg_id", 0),
                "entry_mc":       entry_mc,
                "entry_fmt":      arc.get("entry_fmt", "N/A"),
                "highest_x":      highest_x,
                "current_x":      float(highest_x) if highest_x > 0 else 1.0,
                "highest_mc_fmt": fmt_mc(current_mc) if current_mc > 0 else arc.get("entry_fmt", "N/A"),
                "tracked_since":  arc.get("tracked_since", ""),
                "rugged":         True,
                "post_deleted":   bool(arc.get("post_deleted")),
            })
    except Exception as e:
        logger.warning(f"get_call_history: archive merge failed: {e}")

    if top:
        calls.sort(key=lambda x: x["highest_x"], reverse=True)
        return calls[:50]
    calls.sort(key=lambda x: x.get("tracked_since",""), reverse=True)
    return calls

# --- extracted: _leaderboard_hidden_list ---
def _leaderboard_hidden_list():
    """Channels hidden from the Leaderboard via /removeleaderboard.
    Config key 'leaderboard_hidden' — list of lowercase handles. Points are
    untouched; the channel only disappears from post 136."""
    try:
        return [str(h).lstrip("@").lower() for h in (cfg_get("leaderboard_hidden", []) or [])]
    except Exception:
        return []

# --- extracted: _leaderboard_hidden_keys ---
def _leaderboard_hidden_keys():
    """Companion map for /removeleaderboard: {channel: [call keys that
    existed at removal time]}. A hidden channel only comes back when a FRESH
    call (a key NOT in this list) first reaches 2x — its old calls must not
    trigger re-entry."""
    try:
        return dict(cfg_get("leaderboard_hidden_keys", {}) or {})
    except Exception:
        return {}

# --- extracted: _channel_has_fresh_2x ---
def _channel_has_fresh_2x(channel, known_keys):
    """True if `channel` has a call whose key is NOT in known_keys and which
    already reached a 2x+ milestone (not frozen, not deleted-blocked)."""
    known = set(known_keys or [])
    for call_key, call in tracked_calls.items():
        if call.get("channel", "").lower() != channel.lower():
            continue
        if call.get("frozen"):
            continue
        if _is_deleted_blocked(call):
            continue
        if call_key in known:
            continue
        ms = sent_milestones.get(call_key, set())
        if ms and display_x(max(ms)) >= 2:
            return True
    return False

# --- extracted: _unhide_leaderboard_channel ---
def _unhide_leaderboard_channel(channel):
    """Remove `channel` from leaderboard_hidden (+ its known-keys entry)."""
    ch = channel.lstrip("@").lower()
    c = load_config()
    hidden = [h for h in (c.get("leaderboard_hidden", []) or []) if str(h).lstrip("@").lower() != ch]
    c["leaderboard_hidden"] = hidden
    keys = dict(c.get("leaderboard_hidden_keys", {}) or {})
    keys.pop(ch, None)
    c["leaderboard_hidden_keys"] = keys
    save_config(c)

# --- extracted: _LB_CACHE ---
_LB_CACHE = {"ts": 0.0, "data": None}

# --- extracted: _LB_CACHE_TTL ---
_LB_CACHE_TTL = 60.0

# --- extracted: _calc_leaderboard_scores ---
def _calc_leaderboard_scores():
    """Calculate top 10 leaderboard channels.
    Channels already in Champions list (>=100 points, top-10) are excluded
    so they don't appear in both lists.
    Returns list of (ch, best_x, wizard_post_id)."""
    global _LB_CACHE
    import time as _time
    _c = _LB_CACHE
    if _c["data"] is not None and _time.time() - _c["ts"] < _LB_CACHE_TTL:
        return list(_c["data"])
    channels = _ranking_channels()
    pts_data = load_channel_points()

    # FIX: Build champion_set using the SAME sort key as build_champions_text()
    # i.e. sort by (best_x DESC, pts DESC) — not just pts.
    # Previously it sorted by pts only, so a KOL could land in champion_set
    # (excluded from leaderboard) but NOT appear in the champion post's top-10
    # (which uses best_x as primary sort). Result: KOL vanished from both lists.
    cfg_excl = load_config()
    _champ_snap_lb   = cfg_excl.get("champion_milestone_snapshot", {})
    _champ_excl_keys = set(cfg_excl.get("champion_excluded_call_keys", []))

    champ_candidates_bx = []
    for ch in channels:
        pts = pts_data.get(ch.lower(), {}).get("points", 0)
        if pts < POINTS_FOR_CHAMPION:
            continue
        ch_bx, _ = champion_total_x(ch, _champ_snap_lb, _champ_excl_keys)
        champ_candidates_bx.append((ch, pts, ch_bx))

    # Sort by (best_x desc, pts desc) — exact same key as build_champions_text()
    champ_candidates_bx.sort(key=lambda t: (t[2], t[1]), reverse=True)
    champion_set = {ch.lower() for ch, _, _ in champ_candidates_bx[:10]}

    cfg_now = load_config()
    # Primary exclusion: call_keys snapshot at reset time (most reliable)
    excluded_keys = set(cfg_now.get("lb_excluded_call_keys", []))
    # Secondary: date-based filter
    lb_reset_since = cfg_now.get("leaderboard_reset_since", "")
    lb_reset_dt = None
    if lb_reset_since:
        try: lb_reset_dt = datetime.fromisoformat(lb_reset_since)
        except Exception: lb_reset_dt = None

    # Snapshot-based reset: only milestones ABOVE what existed at reset time count.
    lb_snap = cfg_now.get("lb_milestone_snapshot", {})

    all_scores = {}
    hidden_set = set(_leaderboard_hidden_list())
    hidden_keys = _leaderboard_hidden_keys()
    for ch in channels:
        if ch.lower() in hidden_set:
            # /removeleaderboard: hidden — unless a FRESH call (made after
            # the removal) already hit 2x, in which case wapas lao.
            known = hidden_keys.get(ch.lower(), [])
            if _channel_has_fresh_2x(ch, known):
                _unhide_leaderboard_channel(ch)
                hidden_set.discard(ch.lower())
            else:
                continue
        if ch.lower() in champion_set:
            continue   # already a Champion — skip from Leaderboard
        best_x = 1
        best_call_key = None
        for call_key, call in tracked_calls.items():
            if call.get("channel","").lower() != ch.lower(): continue
            # Frozen calls should not keep contributing their X to the
            # Leaderboard either — same reasoning as Champions above.
            if call.get("frozen"): continue
            # Caller deleted this post (not approved by owner) → X not counted.
            if _is_deleted_blocked(call): continue

            if call_key in lb_snap:
                # Snapshot-based exclusion: only count milestones ABOVE snap value
                snap_max = lb_snap[call_key]
                ms = [m for m in sent_milestones.get(call_key, set()) if m > snap_max]
            elif call_key in excluded_keys:
                # Legacy blanket exclusion (pre-snapshot resets).
                # FIX: instead of dropping the call forever, count only the X
                # levels that were actually reached AFTER the reset.
                ms = _milestones_since(call_key, lb_reset_dt)
                if not ms:
                    continue
            else:
                # New call added after reset — count all milestones
                ms = list(sent_milestones.get(call_key, set()))
                # Secondary date filter for calls not in snapshot and not in excluded_keys
                if lb_reset_dt:
                    ts_str = call.get("tracked_since", "")
                    # Missing/broken timestamp ab call ko disqualify nahi karta —
                    # warna sahi X karne wale KOLs list se gayab ho jate the.
                    if ts_str:
                        try:
                            ts = datetime.fromisoformat(ts_str)
                            if ts < lb_reset_dt:
                                continue
                        except Exception:
                            pass

            if ms:
                # Only count milestones that have a real WizardScan post backing
                # them. A milestone in sent_milestones with no post is a phantom
                # (fake X from the old ratio bug, or a post that was never
                # recorded) — it must not inflate the leaderboard, and it would
                # show with no link. Filtering here also fixes the missing-link
                # problem: every X that survives has a post, so the link below
                # is always found.
                _posts = milestone_posts.get(call_key, {}) or {}
                ms = [m for m in ms if str(m) in _posts]
            if ms:
                mx = display_x(max(ms))
                if mx > best_x:
                    best_x = mx
                    best_call_key = call_key
        wizard_post_id = None
        if best_call_key:
            posts = milestone_posts.get(best_call_key, {})
            wizard_post_id = posts.get(str(best_x))
            # Fallback: find closest available WizardScan post for this channel
            if wizard_post_id is None and posts:
                _valid = {str(k): v for k, v in posts.items() if v and str(k).lstrip('-').isdigit()}
                if _valid:
                    _nearest = sorted(_valid.items(), key=lambda kv: abs(int(kv[0]) - best_x))
                    wizard_post_id = _nearest[0][1]
        # Only include channels that have reached at least 2x milestone
        if best_x < 2:
            continue
        all_scores[ch] = (best_x, wizard_post_id)

    lb_scores = [(ch, x, wpost) for ch, (x, wpost) in all_scores.items()]
    lb_scores.sort(key=lambda t: t[1], reverse=True)
    _res = lb_scores[:10]
    _LB_CACHE = {"ts": _time.time(), "data": _res}
    return _res

# --- extracted: _get_kol_badge ---
def _get_kol_badge(channel, fresh=False):
    """Check if KOL is in champions (>=100 points, top10) or leaderboard (top10 by highest X).
    Returns dict {type, rank, entry, peak} or None.
    fresh=True (BADGE_FRESH_NOV18): leaderboard scores cache se nahi, dobara
    compute kar ke lo - X alert ka badge usi milestone se banta hai jo abhi
    fire hua, cache me wo abhi nahi hota."""
    try:
        # Check champions first — based on points system
        # Sort by (best_x, pts) to match build_champions_text() display order exactly
        pts_data = load_channel_points()
        channels = _ranking_channels()
        # FIX: Apply same champion reset snapshot filter as build_champions_text()
        # so badge rank matches actual champion post rank exactly
        _badge_cfg = load_config()
        _champ_snap = _badge_cfg.get("champion_milestone_snapshot", {})
        _champ_excl = set(_badge_cfg.get("champion_excluded_call_keys", []))
        champ_list_with_x = []
        for ch in channels:
            pts = pts_data.get(ch.lower(), {}).get("points", 0)
            if pts >= POINTS_FOR_CHAMPION:
                ch_best_x, _ = champion_total_x(ch, _champ_snap, _champ_excl)
                champ_list_with_x.append((ch, pts, ch_best_x))
        # Sort by (best_x desc, pts desc) — same order as build_champions_text
        champ_list_with_x.sort(key=lambda t: (t[2], t[1]), reverse=True)
        for i, (ch, pts, ch_bx) in enumerate(champ_list_with_x[:10]):
            if ch.lower() == channel.lower():
                bx = ch_bx; bck = None
                for ck, call in tracked_calls.items():
                    if call.get("channel","").lower() != ch.lower(): continue
                    if _is_deleted_blocked(call): continue
                    ms = list(sent_milestones.get(ck, set()))
                    if ms and max(ms) == bx:
                        bck = ck; break
                if bck is None:
                    for ck, call in tracked_calls.items():
                        if call.get("channel","").lower() != ch.lower(): continue
                        if _is_deleted_blocked(call): continue
                        ms = list(sent_milestones.get(ck, set()))
                        if ms and max(ms) >= bx:
                            bck = ck
                call_d = tracked_calls.get(bck, {}) if bck else {}
                em = call_d.get("entry_mc", 0)
                ef = call_d.get("entry_fmt", "N/A")
                pf = fmt_mc(em * bx) if em > 0 else f"{bx}X"
                return {"type": "champions", "rank": i+1, "entry": ef, "peak": pf}
        # Check leaderboard (top 10 by highest X)
        if fresh:
            # BADGE_FRESH_NOV18: cache expire kar do taake _calc_... dobara
            # compute kare. Jo milestone abhi fire hua (yehi alert), usi se
            # channel board par aaya - cached list me wo nahi hota.
            try:
                _LB_CACHE["ts"] = 0.0
            except Exception:
                pass
        top10 = _calc_leaderboard_scores()
        for i, (ch, bx, wpost) in enumerate(top10):
            if ch.lower() == channel.lower():
                bck2 = None; bx2 = 1
                for ck, call in tracked_calls.items():
                    if call.get("channel","").lower() != ch.lower(): continue
                    if _is_deleted_blocked(call): continue
                    ms = list(sent_milestones.get(ck, set()))
                    if ms:
                        mx = display_x(max(ms))
                        if mx > bx2: bx2 = mx; bck2 = ck
                call_d = tracked_calls.get(bck2, {}) if bck2 else {}
                em = call_d.get("entry_mc", 0); ef = call_d.get("entry_fmt", "N/A")
                pf = fmt_mc(em * bx2) if em > 0 else f"{bx2}X"
                return {"type": "leaderboard", "rank": i+1, "entry": ef, "peak": pf}
    except Exception as e:
        logger.warning(f"_get_kol_badge error: {e}")
    return None

# --- extracted: _ex_name ---
def _ex_name(c):
    sym = (c.get("symbol") or "").strip()
    return html.escape(f"${sym.upper()}" if sym else (c.get("ca", "")[:6] + "…"))

# --- extracted: _ex_prem_all ---
def _ex_prem_all():
    try: return dict(load_config().get("premium_users", {}) or {})
    except Exception: return {}

# --- extracted: _ex_prem_get ---
def _ex_prem_get(uid):
    rec = _ex_prem_all().get(str(uid))
    if not isinstance(rec, dict): return None
    try:
        if datetime.fromisoformat(rec.get("exp", "")) < datetime.utcnow(): return None
    except Exception:
        return None
    return rec

# --- extracted: _ex_premium_dm ---
async def _ex_premium_dm(bot, call_key, call, ms, cur_fmt, record_only=False):
    """Fire-and-forget DM to premium users. Never raises, never blocks alerts."""
    try:
        if record_only: return
        pu = _ex_prem_all()
        if not pu: return
        text = (f"🔔 <b>{_ex_name(call)} hit {ms}x</b>\n"
                f"⛓ {html.escape(str(call.get('chain','?')))} | Caller: @{html.escape(str(call.get('channel','?')))}\n"
                f"Entry {html.escape(str(call.get('entry_fmt','?')))} → Now {html.escape(str(cur_fmt))}\n"
                f"<code>{html.escape(str(call.get('ca','')))}</code>")
        for uid, rec in pu.items():
            if not _ex_prem_get(uid): continue
            if ms < int(rec.get("min_x", 5) or 5): continue
            try:
                await bot.send_message(int(uid), text, parse_mode="HTML", disable_web_page_preview=True)
            except Exception:
                pass
            await asyncio.sleep(0.05)
    except Exception as e:
        logger.warning(f"premium dm failed: {e}")


# ── DM Alert Settings prefs (DM_ALERT_SETTINGS_NOV25/2/3/4) ─────────────────
# Per-user-per-channel X-alert level choice, saved by the MAIN bot's
# settings UI in data/dm_alert_prefs.json:
# {"<uid>": {"<channel>": {"x": "all"|"standard"|"major"|"disable",
# "dip": ...}}} (old shape {"<uid>": {"x": ..., "dip": ...}} is read as the
# default for every channel). Default (no saved choice) is "all".
# Channel prefs live under the top-level "channel" key:
# {"channel": {"<channel>": {"x": ..., "dip": ...}}}.
# xalertbot only READS this file (the main bot's settings UI writes it).
DM_ALERT_PREFS_FILE = _dp("dm_alert_prefs.json")
DM_ALERT_X_STANDARD_LEVELS = [2, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55,
                              60, 65, 70, 75, 80, 85, 90, 95, 100, 150, 200,
                              250, 300, 500, 600, 700, 800, 900, 1000]
DM_ALERT_X_MAJOR_LEVELS = [2, 10, 20, 50, 100, 200, 300, 500, 1000]
DM_ALERT_DIP_STANDARD_LEVELS = [2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 25, 30,
                                35, 40, 45, 50, 60, 70, 80, 90, 100, 150, 200,
                                250, 500, 1000, 1500]
DM_ALERT_DIP_MAJOR_LEVELS = [2, 10, 20, 30, 50, 100, 200, 300, 500, 1000]


def _dm_alert_prefs_load():
    try:
        d = load_json(DM_ALERT_PREFS_FILE, {})
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _dm_alert_prefs_save(prefs):
    try:
        save_json(DM_ALERT_PREFS_FILE, prefs)
    except Exception as e:
        logger.error(f"dm_alert_prefs save failed: {e}")


def _dm_alert_choice(rec, key, channel):
    # Read one level choice ('x' or 'dip') from a user's prefs record.
    # New shape: {"<channel>": {"x": ..., "dip": ...}}. Old shape {"x": ...,
    # "dip": ...} (no channel key) is the default for every channel, so
    # choices saved before channel selection keep working everywhere.
    try:
        if not isinstance(rec, dict):
            return "all"
        if channel:
            ch = str(channel).lstrip("@").lower()
            c = rec.get(ch)
            if isinstance(c, dict) and c.get(key):
                v = str(c.get(key)).lower()
                if v in ("all", "standard", "major", "disable"):
                    return v
        v = str(rec.get(key) or "all").lower()
        return v if v in ("all", "standard", "major", "disable") else "all"
    except Exception:
        return "all"


def dm_alert_x_choice(uid, channel=None):
    try:
        rec = _dm_alert_prefs_load().get(str(int(uid)), {}) or {}
        return _dm_alert_choice(rec, "x", channel)
    except Exception:
        return "all"


def _dm_alert_x_allows(uid, channel, x_val):
    try:
        choice = dm_alert_x_choice(uid, channel)
        if choice == "all":
            return True
        if choice == "disable":
            return False
        x = int(x_val)
        if choice == "standard":
            return x in DM_ALERT_X_STANDARD_LEVELS
        if choice == "major":
            return x in DM_ALERT_X_MAJOR_LEVELS
        return True
    except Exception:
        return True


def dm_alert_dip_choice(uid, channel=None):
    try:
        rec = _dm_alert_prefs_load().get(str(int(uid)), {}) or {}
        return _dm_alert_choice(rec, "dip", channel)
    except Exception:
        return "all"


def _dm_alert_dip_allows(uid, channel, x_val):
    try:
        choice = dm_alert_dip_choice(uid, channel)
        if choice == "all":
            return True
        if choice == "disable":
            return False
        x = int(x_val)
        if choice == "standard":
            return x in DM_ALERT_DIP_STANDARD_LEVELS or (x > 1500 and (x - 1500) % 500 == 0)
        if choice == "major":
            return x in DM_ALERT_DIP_MAJOR_LEVELS or (x > 1500 and (x - 1500) % 500 == 0)
        return True
    except Exception:
        return True


def channel_alert_x_choice(channel):
    if not channel:
        return "all"
    try:
        prefs = (_dm_alert_prefs_load() or {}).get("channel", {})
        return str((prefs.get(str(channel).lstrip("@").lower()) or {}).get("x", "all")).lower()
    except Exception:
        return "all"


def channel_alert_dip_choice(channel):
    if not channel:
        return "all"
    try:
        prefs = (_dm_alert_prefs_load() or {}).get("channel", {})
        return str((prefs.get(str(channel).lstrip("@").lower()) or {}).get("dip", "all")).lower()
    except Exception:
        return "all"


def channel_alert_x_allows(channel, x_val):
    ch = channel_alert_x_choice(channel)
    if ch == "all":
        return True
    if ch == "disable":
        return False
    try:
        xv = float(x_val)
    except Exception:
        return True
    if ch == "standard":
        return xv in DM_ALERT_X_STANDARD_LEVELS
    if ch == "major":
        return xv in DM_ALERT_X_MAJOR_LEVELS
    return True


def channel_alert_dip_allows(channel, x_val):
    ch = channel_alert_dip_choice(channel)
    if ch == "all":
        return True
    if ch == "disable":
        return False
    try:
        xv = float(x_val)
    except Exception:
        return True
    if ch == "standard":
        return xv in DM_ALERT_DIP_STANDARD_LEVELS
    if ch == "major":
        return xv in DM_ALERT_DIP_MAJOR_LEVELS
    return True



# ═══════════════════════════════════════════════════════════════════════════
# XALERTBOT core - everything below is xalertbot-specific (not extracted).
# The extracted posting pipeline above calls the *_save_* / award / points
# helpers defined here; they NEVER write the main bot's data files - they
# write small outbox records the main bot applies (Model B).
# ═══════════════════════════════════════════════════════════════════════════
import sys as _sys
if os.path.isdir("/opt/bot"):
    # Champion-video modules (champion_video, champion_rocket_neon, ...) live
    # in /opt/bot; _champion_video_try_send imports them. Read-only import.
    _sys.path.insert(0, "/opt/bot")

from types import SimpleNamespace as _SimpleNamespace

# --- outbox ------------------------------------------------------------------
def _xalert_outbox_write(obj):
    """Append one bookkeeping record for the main bot. Atomic create, never
    overwrites. Returns the file name or None."""
    try:
        os.makedirs(XALERT_OUTBOX_DIR, exist_ok=True)
        name = "%d_%d_%06d.json" % (
            int(time.time() * 1000), os.getpid(), random.randint(0, 999999))
        path = os.path.join(XALERT_OUTBOX_DIR, name)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        try:
            os.write(fd, json.dumps(obj, ensure_ascii=False).encode("utf-8"))
        finally:
            os.close(fd)
        return path
    except Exception as e:
        logger.error(f"xalert outbox write failed {obj.get('type')}: {e}")
        return None


# --- snapshots for diff-based saves ------------------------------------------
_sealed_snapshot = {}
_posts_snapshot = {}
_times_snapshot = {}
_own_pending_seals = set()     # (call_key, ms) we sealed, ack not yet seen
_own_pending_unseals = set()   # (call_key, ms) we unsealed, ack not yet seen


def _save_milestones():
    # XALERTBOT: never writes sent_milestones.json. Diff the in-memory dict
    # against the last snapshot and emit seal/unseal outbox records for the
    # delta. The main bot applies them and flushes (that is the seal ack).
    global _sealed_snapshot
    try:
        cur = {k: set(v) for k, v in sent_milestones.items()}
        for k, levels in cur.items():
            for ms in levels - _sealed_snapshot.get(k, set()):
                _xalert_outbox_write({"type": "seal", "call_key": k, "ms": int(ms)})
                _own_pending_seals.add((k, int(ms)))
        for k, levels in _sealed_snapshot.items():
            for ms in levels - cur.get(k, set()):
                _xalert_outbox_write({"type": "unseal", "call_key": k, "ms": int(ms)})
                _own_pending_unseals.add((k, int(ms)))
        _sealed_snapshot = cur
    except Exception as e:
        logger.warning(f"xalert _save_milestones diff failed: {e}")


def _save_milestone_posts():
    # XALERTBOT: emit an outbox record per new/changed post id.
    global _posts_snapshot
    try:
        cur = {k: dict(v) for k, v in milestone_posts.items()}
        for k, posts in cur.items():
            for xv, pid in posts.items():
                if _posts_snapshot.get(k, {}).get(xv) != pid:
                    _xalert_outbox_write({"type": "milestone_post", "call_key": k,
                                          "x_val": xv, "post_id": pid})
        _posts_snapshot = cur
    except Exception as e:
        logger.warning(f"xalert _save_milestone_posts diff failed: {e}")


def _save_milestone_times():
    global _times_snapshot
    try:
        cur = {k: dict(v) for k, v in milestone_times.items()}
        for k, times in cur.items():
            for ms, at in times.items():
                if _times_snapshot.get(k, {}).get(ms) != at:
                    _xalert_outbox_write({"type": "milestone_time", "call_key": k,
                                          "ms": ms, "at": at})
        _times_snapshot = cur
    except Exception as e:
        logger.warning(f"xalert _save_milestone_times diff failed: {e}")


def _record_milestone_time(call_key, ms):
    try:
        milestone_times.setdefault(str(call_key), {})[str(int(ms))] = datetime.utcnow().isoformat()
        _save_milestone_times()
    except Exception:
        pass


async def award_points_for_milestone(channel, call_key, x_val):
    # XALERTBOT: points are awarded by the MAIN BOT (it owns
    # channel_points.json). Queue the request; return 0 so the caller does
    # not log a fake "+N points" line.
    _xalert_outbox_write({"type": "award_points", "channel": channel,
                          "call_key": call_key, "x_val": x_val})
    logger.info(f"xalert: points award queued for main bot: @{channel} {x_val}X")
    return 0


def _inc_channel_post_count():
    # XALERTBOT: the counter lives on the main bot (it posts hashtag/promo
    # when due). Queue the count; return 1 so no hashtag/promo task fires
    # here.
    _xalert_outbox_write({"type": "alert_posted"})
    return 1


async def _update_leaderboard_with_premium_emojis(bot):
    # XALERTBOT no-op: the main bot's periodic jobs refresh the leaderboard
    # and champions posts within seconds.
    return None


async def _update_champions_with_premium_emojis(bot):
    return None


async def _post_hashtag_to_channel(bot):
    return None


async def _post_promo_to_channel(bot):
    return None


def _pick_userbot():
    return userbot_client


def _userbot_clients():
    return [userbot_client] if userbot_client else []


# --- pending media (xalertbot's OWN file, not the main bot's) -----------------
def _load_pending_media():
    global pending_media_alerts
    d = load_json(XALERT_PENDING_FILE, {})
    pending_media_alerts = defaultdict(set, {k: set(v) for k, v in d.items()}) if isinstance(d, dict) else defaultdict(set)


def _save_pending_media():
    try:
        save_json(XALERT_PENDING_FILE,
                  {k: sorted(v) for k, v in pending_media_alerts.items()})
    except Exception as e:
        logger.warning(f"xalert _save_pending_media failed: {e}")


def _add_pending_media(call_key, x_val):
    try:
        pending_media_alerts.setdefault(call_key, set()).add(x_val)
        _save_pending_media()
    except Exception as e:
        logger.warning(f"xalert _add_pending_media failed: {e}")


# --- hot streak state (read main bot file, write via outbox) ------------------
def _hs_load():
    return load_json(_dp("hot_streak_state.json"), {})


def _hs_save(d):
    # Full-state outbox record; the main bot MERGES it by channel key (so a
    # concurrent update from another channel is not lost) and saves.
    _xalert_outbox_write({"type": "hs_state", "state": d})


# --- main-bot data reload (read-only, mtime-gated) -----------------------------
_mtimes = {"tracked": 0.0, "sent": 0.0, "posts": 0.0, "times": 0.0}


def _xalert_reload_main_data(force=False):
    global tracked_calls, sent_milestones, milestone_posts, milestone_times
    global _sealed_snapshot, _posts_snapshot, _times_snapshot
    global _own_pending_seals, _own_pending_unseals
    try:
        p = os.path.join(MAIN_DATA_DIR, "tracked_calls.json")
        mt = os.path.getmtime(p)
        if force or mt != _mtimes["tracked"]:
            d = load_json(p, {})
            if isinstance(d, dict) and d:
                tracked_calls = d
            _mtimes["tracked"] = mt
    except Exception:
        pass
    try:
        p = os.path.join(MAIN_DATA_DIR, "sent_milestones.json")
        mt = os.path.getmtime(p)
        if force or mt != _mtimes["sent"]:
            d = load_json(p, {})
            if isinstance(d, dict):
                # Disk is authoritative, EXCEPT our own un-acked seals/unseals.
                new = {k: set(v) for k, v in d.items()}
                for (k, ms) in _own_pending_seals:
                    new.setdefault(k, set()).add(ms)
                for (k, ms) in _own_pending_unseals:
                    if k in new:
                        new[k].discard(ms)
                sent_milestones = defaultdict(set, new)
                _sealed_snapshot = {k: set(v) for k, v in new.items()}
                # Drop pending seals the disk now shows (acked).
                _own_pending_seals = {(k, ms) for (k, ms) in _own_pending_seals
                                      if ms not in new.get(k, set())}
                _own_pending_unseals = {(k, ms) for (k, ms) in _own_pending_unseals
                                        if ms in new.get(k, set())}
            _mtimes["sent"] = mt
    except Exception:
        pass
    try:
        p = os.path.join(MAIN_DATA_DIR, "milestone_posts.json")
        mt = os.path.getmtime(p)
        if force or mt != _mtimes["posts"]:
            d = load_json(p, {})
            if isinstance(d, dict):
                milestone_posts = d
                _posts_snapshot = {k: dict(v) for k, v in d.items()}
            _mtimes["posts"] = mt
    except Exception:
        pass
    try:
        p = os.path.join(MAIN_DATA_DIR, "milestone_times.json")
        mt = os.path.getmtime(p)
        if force or mt != _mtimes["times"]:
            d = load_json(p, {})
            if isinstance(d, dict):
                milestone_times = d
                _times_snapshot = {k: dict(v) for k, v in d.items()}
            _mtimes["times"] = mt
    except Exception:
        pass


def _xalert_seal_visible(call_key, ms):
    # Fresh direct read (bypasses the mtime cache): is this level sealed
    # in the main bot's sent_milestones.json right now?
    try:
        d = load_json(os.path.join(MAIN_DATA_DIR, "sent_milestones.json"), {})
        return int(ms) in [int(x) for x in (d.get(call_key) or [])]
    except Exception:
        return False


async def _xalert_wait_seal(call_key, ms, timeout=5.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if _xalert_seal_visible(call_key, ms):
            return True
        await asyncio.sleep(0.3)
    return _xalert_seal_visible(call_key, ms)


# --- claims (exactly-once across restarts) -------------------------------------
_state = {"claims": {}, "intake_offset": 0}


def _xalert_state_load():
    global _state
    d = load_json(XALERT_STATE_FILE, {})
    if isinstance(d, dict):
        _state = {"claims": d.get("claims", {}) or {},
                  "intake_offset": d.get("intake_offset", 0) or 0}


def _xalert_state_save():
    try:
        save_json(XALERT_STATE_FILE, _state)
    except Exception as e:
        logger.warning(f"xalert state save failed: {e}")


def _xalert_claim_get(queue_key):
    return _state["claims"].get(queue_key)


def _xalert_claim_set(queue_key, state, extra=None):
    rec = {"state": state, "at": time.time()}
    if extra:
        rec.update(extra)
    _state["claims"][queue_key] = rec
    _xalert_state_save()


# --- userbot (OWN session - never the main bot's @yowziuh session) ------------
_userbot_init_lock = None


def _xalert_session_string():
    if XALERTBOT_SESSION:
        return XALERTBOT_SESSION
    p = os.path.join(XALERT_DATA_DIR, "userbot_session.txt")
    try:
        with open(p, encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


async def _xalert_init_userbot():
    global userbot_client, _userbot_init_lock
    session_str = _xalert_session_string()
    if not session_str or not XALERTBOT_API_ID or not XALERTBOT_API_HASH:
        logger.warning("xalertbot: NO userbot session configured - Bot API "
                       "fallback mode (video captions lose premium emojis; "
                       "text alerts keep them). See xalertbot-deploy.md.")
        userbot_client = None
        return False
    if _userbot_init_lock is None:
        _userbot_init_lock = asyncio.Lock()
    async with _userbot_init_lock:
        if userbot_client and userbot_client.is_connected():
            return True
        candidate = None
        try:
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            candidate = TelegramClient(
                StringSession(session_str),
                XALERTBOT_API_ID, XALERTBOT_API_HASH,
                connection_retries=3, retry_delay=3, auto_reconnect=True,
                catch_up=False, receive_updates=True, flood_sleep_threshold=60)
            await asyncio.wait_for(candidate.connect(), timeout=45)
            if not await asyncio.wait_for(candidate.is_user_authorized(), timeout=15):
                logger.warning("xalertbot userbot: session exists but not authorized")
                await candidate.disconnect()
                userbot_client = None
                return False
            me = await asyncio.wait_for(candidate.get_me(), timeout=15)
            userbot_client = candidate
            logger.info(f"xalertbot userbot connected: @{me.username or me.id}")
            return True
        except Exception as e:
            logger.error(f"xalertbot userbot init failed: {type(e).__name__}: {e}")
            if candidate:
                try:
                    await candidate.disconnect()
                except Exception:
                    pass
            userbot_client = None
            return False


# --- dispatch ------------------------------------------------------------------
_call_locks = {}


def _xalert_call_lock(call_key):
    lock = _call_locks.get(call_key)
    if lock is None:
        lock = asyncio.Lock()
        _call_locks[call_key] = lock
    return lock


async def _xalert_send_posted(bot, call_key, call, x_val, cur_fmt, record_only):
    """Call send_alert and handle the result like the old RT dispatcher:
    truthy -> milestone time + hot streak (ms>=2); False + not held ->
    caller retries via the claims recovery pass."""
    try:
        res = await asyncio.wait_for(
            send_alert(bot, call.get("channel", ""), call.get("msg_id", 0),
                       x_val, call.get("chain", "SOL"),
                       call.get("entry_fmt", "N/A"), cur_fmt,
                       call.get("ca", ""), call.get("symbol", ""),
                       record_only=record_only),
            timeout=RT_SEND_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        logger.warning(f"xalert: send_alert timeout {x_val}X {call_key}")
        return False
    except Exception as e:
        logger.warning(f"xalert: send_alert failed {x_val}X {call_key}: {e}")
        return False
    if res:
        try:
            _record_milestone_time(call_key, x_val)
        except Exception:
            pass
        if x_val >= 2:
            try:
                await _hs_on_call_hit(bot, call.get("channel", ""), call_key, x_val)
            except Exception as e:
                logger.warning(f"xalert: hot streak hook failed {call_key}: {e}")
        return True
    return False


async def _xalert_process_entry(bot, entry):
    """One monbot queue entry: GUARD + seal (via main bot) + post."""
    try:
        if not isinstance(entry, dict):
            return False
        call_key = entry.get("call_key", "")
        typ = entry.get("type", "x")
        if typ != "x":
            return False  # dip entries stay with the main bot
        try:
            x_val = int(entry.get("x_val", 0) or 0)
        except (TypeError, ValueError):
            return False
        if not call_key or x_val <= 0:
            return False
        queue_key = f"{call_key}:{x_val}:x"
        claim = _xalert_claim_get(queue_key)
        if claim and claim.get("state") == "posted":
            return False
        _xalert_reload_main_data()
        call = tracked_calls.get(call_key)
        if not call:
            return False
        if x_val in sent_milestones.get(call_key, set()):
            # Sealed already. If it was never posted, the recovery pass
            # re-posts it (claims file); here we just skip the queue entry.
            return False
        record_only = bool(entry.get("record_only", False))
        cur_fmt = entry.get("ms_mc_fmt", "N/A")
        _xalert_claim_set(queue_key, "sealing", {"x_val": x_val})
        # Seal FIRST (outbox) + GUARD fetch in parallel; post only after
        # the seal ack lands (or give up this pass and retry - never post
        # unsealed).
        sent_milestones.setdefault(call_key, set()).add(x_val)
        _save_milestones()
        guard_task = asyncio.create_task(asyncio.wait_for(
            fetch_dexscreener(call.get("ca", ""), allow_alt=False,
                              retries=1, min_liquidity=0), timeout=4))
        acked = await _xalert_wait_seal(call_key, x_val, timeout=5.0)
        try:
            live = await guard_task
        except Exception:
            live = None
        if live:
            try:
                live_mc = float((live or {}).get("mcap", 0) or 0)
                entry_mc = float(call.get("entry_mc", 0) or 0)
            except (TypeError, ValueError):
                live_mc, entry_mc = 0.0, 0.0
            if entry_mc > 0 and live_mc > 0 and (live_mc / entry_mc) < x_val:
                logger.info(f"xalert: GUARD discard {x_val}X "
                            f"{call.get('symbol', '?')} live {live_mc / entry_mc:.2f}x")
                _xalert_claim_set(queue_key, "discarded")
                return False
        if not acked:
            # Main bot has not applied the seal yet (down / slow). Do NOT
            # post. The recovery pass retries; the seal request is already
            # in the outbox and is idempotent.
            logger.warning(f"xalert: seal ack timeout {x_val}X {call_key} - retry next pass")
            return False
        ok = await _xalert_send_posted(bot, call_key, call, x_val, cur_fmt, record_only)
        if ok:
            _xalert_claim_set(queue_key, "posted", {"x_val": x_val})
            logger.info(f"xalert: posted {x_val}X {call.get('symbol', '?')} "
                        f"@{call.get('channel', '?')}")
            return True
        # send failed. Held in pending_media_alerts -> the 15s retry loop
        # owns it now. Otherwise the recovery pass retries it (up to 3x),
        # then unseals so monbot re-detects it.
        if x_val in (pending_media_alerts.get(call_key) or set()):
            _xalert_claim_set(queue_key, "held", {"x_val": x_val})
            return False
        tries = int((claim or {}).get("tries", 0)) + 1
        _xalert_claim_set(queue_key, "sealing", {"x_val": x_val, "tries": tries})
        return False
    except Exception as e:
        logger.warning(f"xalert: process entry failed: {e}")
        return False


async def _xalert_recovery_pass(bot):
    """Stale claims: sealed-but-unposted (crash between seal and post) and
    failed sends. Re-post if sealed and no post recorded yet; retry the
    send up to 3 times, then unseal so monbot re-detects it."""
    try:
        now = time.time()
        _xalert_reload_main_data()
        for queue_key, claim in list(_state["claims"].items()):
            if not isinstance(claim, dict):
                continue
            st = claim.get("state")
            if st == "posted" or st == "discarded":
                continue
            if now - float(claim.get("at", 0) or 0) < 10:
                continue
            try:
                call_key, _x, _t = queue_key.rsplit(":", 2)
                x_val = int(claim.get("x_val") or _x)
            except Exception:
                continue
            call = tracked_calls.get(call_key)
            if not call:
                _xalert_claim_set(queue_key, "discarded")
                continue
            if st == "held":
                continue  # pending-media loop owns it
            # Already posted (post id recorded by the main bot)? Then done.
            posts = milestone_posts.get(call_key, {}) or {}
            if str(x_val) in posts or x_val in posts:
                _xalert_claim_set(queue_key, "posted", {"x_val": x_val})
                continue
            tries = int(claim.get("tries", 0))
            if tries >= 3:
                # Give up: unseal so monbot re-detects + re-queues it.
                try:
                    sent_milestones.get(call_key, set()).discard(x_val)
                    _save_milestones()
                except Exception:
                    pass
                _xalert_claim_set(queue_key, "given_up", {"x_val": x_val})
                logger.warning(f"xalert: gave up {x_val}X {call_key} - unsealed for re-detect")
                continue
            if x_val not in sent_milestones.get(call_key, set()):
                # Not sealed (any more): re-run the normal flow next pass.
                _xalert_claim_set(queue_key, "sealing",
                                  {"x_val": x_val, "tries": tries + 1})
                continue
            ok = await _xalert_send_posted(
                bot, call_key, call, x_val,
                fmt_mc(float(call.get("peak_mc", 0) or 0)) if float(call.get("peak_mc", 0) or 0) > 0 else "N/A",
                False)
            if ok:
                _xalert_claim_set(queue_key, "posted", {"x_val": x_val})
                logger.info(f"xalert: recovery posted {x_val}X {call_key}")
            else:
                _xalert_claim_set(queue_key, "sealing",
                                  {"x_val": x_val, "tries": tries + 1})
    except Exception as e:
        logger.warning(f"xalert: recovery pass failed: {e}")


async def _xalert_process_intake(bot):
    """Lines the main bot appended (/forcex etc.): seal + post, no GUARD
    (owner override)."""
    try:
        off = int(_state.get("intake_offset", 0) or 0)
        if not os.path.exists(XALERT_INTAKE_FILE):
            return
        with open(XALERT_INTAKE_FILE, encoding="utf-8") as f:
            f.seek(off)
            data = f.read()
            off = f.tell()
        if not data.strip():
            _state["intake_offset"] = off
            _xalert_state_save()
            return
        _xalert_reload_main_data()
        for line in data.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                req = json.loads(line)
            except Exception:
                continue
            if not isinstance(req, dict):
                continue
            typ = req.get("type", "")
            if typ not in ("forcex", "fixoldcall", "postx"):
                continue
            call_key = req.get("call_key", "")
            try:
                x_val = int(req.get("x_val", 0) or 0)
            except (TypeError, ValueError):
                continue
            call = tracked_calls.get(call_key)
            if not call or x_val <= 0:
                continue
            # Seal: forcex seals every milestone <= x_val (like the old
            # /forcex); postx seals ONLY x_val (one level, e.g. /recheckx).
            # Points are awarded only for levels sealed HERE - if the main
            # bot already sealed + awarded a level (/addmissedcall,
            # /recheckx do it inline), it is skipped, so no double award.
            try:
                if typ == "postx":
                    todo = [x_val]
                else:
                    todo = [ms for ms in get_milestones() if ms <= x_val]
                for ms in todo:
                    if ms not in sent_milestones.get(call_key, set()):
                        sent_milestones.setdefault(call_key, set()).add(ms)
                        _xalert_outbox_write({"type": "award_points",
                                              "channel": call.get("channel", ""),
                                              "call_key": call_key, "x_val": ms})
                _save_milestones()
            except Exception as e:
                logger.warning(f"xalert: {typ} seal failed {call_key}: {e}")
            acked = await _xalert_wait_seal(call_key, x_val, timeout=5.0)
            if not acked:
                logger.warning(f"xalert: forcex seal ack timeout {x_val}X {call_key}")
                continue
            peak_mc = float(req.get("peak_mc", 0) or 0)
            cur_fmt = fmt_mc(peak_mc) if peak_mc > 0 else "N/A"
            ok = await _xalert_send_posted(bot, call_key, call, x_val, cur_fmt, False)
            if ok:
                _xalert_claim_set(f"{call_key}:{x_val}:x", "posted", {"x_val": x_val})
                logger.info(f"xalert: {typ} posted {x_val}X {call_key}")
        _state["intake_offset"] = off
        _xalert_state_save()
    except Exception as e:
        logger.warning(f"xalert: intake failed: {e}")


# --- loops ---------------------------------------------------------------------
async def _xalert_queue_loop(bot, work_q):
    while True:
        try:
            try:
                with open(MONBOT_QUEUE_FILE, encoding="utf-8") as f:
                    queue = json.load(f) or {}
            except FileNotFoundError:
                queue = {}
            except Exception as e:
                logger.warning(f"xalert: queue read failed: {e}")
                queue = {}
            if isinstance(queue, dict) and queue:
                try:
                    entries = sorted(queue.values(),
                                     key=lambda e: str(e.get("detected_at", "")))
                except Exception:
                    entries = list(queue.values())
                for entry in entries[:10]:
                    if not isinstance(entry, dict):
                        continue
                    if entry.get("type", "x") != "x":
                        continue
                    ck = f"{entry.get('call_key', '')}:{entry.get('x_val', 0)}:x"
                    if _xalert_claim_get(ck) and _xalert_claim_get(ck).get("state") == "posted":
                        continue
                    try:
                        work_q.put_nowait(entry)
                    except asyncio.QueueFull:
                        break
        except Exception as e:
            logger.warning(f"xalert: queue loop failed: {e}")
        await asyncio.sleep(1.0)


async def _xalert_worker(bot, work_q, in_flight):
    while True:
        entry = await work_q.get()
        try:
            call_key = entry.get("call_key", "")
            lock = _xalert_call_lock(call_key)
            async with lock:
                await _xalert_process_entry(bot, entry)
        except Exception as e:
            logger.warning(f"xalert: worker failed: {e}")
        finally:
            try:
                in_flight.discard(f"{entry.get('call_key', '')}:{entry.get('x_val', 0)}:x")
            except Exception:
                pass
            work_q.task_done()


async def _xalert_intake_loop(bot):
    while True:
        try:
            await _xalert_process_intake(bot)
        except Exception as e:
            logger.warning(f"xalert: intake loop failed: {e}")
        await asyncio.sleep(2.0)


async def _xalert_pending_loop(bot):
    while True:
        await asyncio.sleep(15.0)
        try:
            _xalert_reload_main_data()
            await pending_media_job(_SimpleNamespace(bot=bot))
        except Exception as e:
            logger.warning(f"xalert: pending-media loop failed: {e}")


async def _xalert_recovery_loop(bot):
    while True:
        await asyncio.sleep(30.0)
        try:
            await _xalert_recovery_pass(bot)
        except Exception as e:
            logger.warning(f"xalert: recovery loop failed: {e}")


async def _xalert_userbot_watchdog():
    while True:
        await asyncio.sleep(45.0)
        try:
            if XALERT_NO_USERBOT:
                continue
            if not userbot_client or not userbot_client.is_connected():
                logger.warning("xalertbot userbot disconnected - reconnecting")
                await _xalert_init_userbot()
        except Exception as e:
            logger.warning(f"xalertbot userbot watchdog failed: {e}")


async def _xalert_reload_loop():
    while True:
        await asyncio.sleep(5.0)
        try:
            _xalert_reload_main_data()
        except Exception:
            pass


async def _amain():
    global _sealed_snapshot, _posts_snapshot, _times_snapshot
    os.makedirs(XALERT_DATA_DIR, exist_ok=True)
    os.makedirs(XALERT_OUTBOX_DIR, exist_ok=True)
    _xalert_state_load()
    _load_pending_media()
    _xalert_reload_main_data(force=True)
    _sealed_snapshot = {k: set(v) for k, v in sent_milestones.items()}
    _posts_snapshot = {k: dict(v) for k, v in milestone_posts.items()}
    _times_snapshot = {k: dict(v) for k, v in milestone_times.items()}
    if not XALERTBOT_TOKEN:
        logger.error("xalertbot: XALERTBOT_TOKEN missing in .env - cannot post. Exiting.")
        return
    bot = _TG_Bot(token=XALERTBOT_TOKEN)
    try:
        me = await bot.get_me()
        logger.info(f"xalertbot: Bot API ready as @{me.username or me.id} (no polling, sends only)")
    except Exception as e:
        logger.error(f"xalertbot: Bot API get_me failed: {e} - cannot post. Exiting.")
        return
    if XALERT_NO_USERBOT:
        logger.warning("xalertbot: running WITHOUT a userbot session - video "
                       "captions will NOT have premium emojis (Bot API limit). "
                       "Set XALERTBOT_SESSION in .env (see deploy doc).")
    else:
        await _xalert_init_userbot()
    # Startup recovery: sealed-but-unposted claims from a previous run.
    try:
        await _xalert_recovery_pass(bot)
    except Exception as e:
        logger.warning(f"xalertbot: startup recovery failed: {e}")
    work_q = asyncio.Queue(maxsize=50)
    in_flight = set()
    logger.info("xalertbot LIVE - X-alert posting off the main bot "
                f"(queue={MONBOT_QUEUE_FILE}, userbot={'on' if userbot_client else 'OFF - Bot API fallback'})")
    await asyncio.gather(
        _xalert_queue_loop(bot, work_q),
        _xalert_worker(bot, work_q, in_flight),
        _xalert_worker(bot, work_q, in_flight),
        _xalert_intake_loop(bot),
        _xalert_pending_loop(bot),
        _xalert_recovery_loop(bot),
        _xalert_userbot_watchdog(),
        _xalert_reload_loop(),
    )


def main():
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()

