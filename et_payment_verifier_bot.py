"""
Ethiopian Payment-Verified Telegram Channel Subscription Bot
Single-file architecture — SQLite storage, async httpx API, APScheduler jobs.

Run:  python et_payment_verifier_bot.py
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
import re
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any, Optional

import aiosqlite
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ── Config ────────────────────────────────────────────────────────────────────

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
BOT_USERNAME = os.getenv("BOT_USERNAME", "").lstrip("@").strip()
API_KEY = os.getenv("VERIFIER_API_KEY", "").strip()
API_BASE = os.getenv("VERIFIER_API_BASE", "https://verifyapi.leulzenebe.pro").rstrip("/")
DB_PATH = os.getenv("DATABASE_PATH", "bot.db")
LEGACY_JSON = os.getenv("STORAGE_FILE", "paid_channels.json")
ADMIN_ALERT_CHAT_ID = os.getenv("ADMIN_ALERT_CHAT_ID", "").strip()
AMOUNT_TOLERANCE_PCT = float(os.getenv("AMOUNT_TOLERANCE_PCT", "2"))
TXN_MAX_AGE_HOURS = int(os.getenv("TXN_MAX_AGE_HOURS", "24"))
INVITE_LINK_EXPIRY_HOURS = int(os.getenv("INVITE_LINK_EXPIRY_HOURS", "1"))
MAX_CHANNELS_PER_ADMIN = int(os.getenv("MAX_CHANNELS_PER_ADMIN", "10"))
WARNING_INTERVAL_HOURS = int(os.getenv("WARNING_CHECK_INTERVAL_HOURS", "6"))
KICK_INTERVAL_HOURS = int(os.getenv("KICK_CHECK_INTERVAL_HOURS", "1"))

# ── Logging ───────────────────────────────────────────────────────────────────

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"))

logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[handler],
    encoding='utf-8',
)
logger = logging.getLogger("payment_bot")

# Silence overly verbose PTB logs
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)

# ── Constants ─────────────────────────────────────────────────────────────────

BANK_CBE, BANK_TELEBIRR, BANK_DASHEN = "cbe", "telebirr", "dashen"
BANK_ABYSSINIA, BANK_CBEBIRR, BANK_IMAGE = "abyssinia", "cbebirr", "image"
VALID_BANKS = {BANK_CBE, BANK_TELEBIRR, BANK_DASHEN, BANK_ABYSSINIA, BANK_CBEBIRR, BANK_IMAGE}

BANK_LABELS = {
    BANK_CBE: "🏦 CBE",
    BANK_TELEBIRR: "📱 Telebirr",
    BANK_DASHEN: "🏦 Dashen Bank",
    BANK_ABYSSINIA: "🏦 Bank of Abyssinia",
    BANK_CBEBIRR: "📱 CBE Birr",
    BANK_IMAGE: "🖼️ Screenshot",
}

# ── States ────────────────────────────────────────────────────────────────────
S_IDLE, S_CHOOSE, S_REF, S_SUFFIX, S_PHONE, S_IMAGE = (
    "idle", "choose_bank", "await_reference", "await_suffix", "await_phone", "await_image",
)
S_JOIN_CHOOSE, S_JOIN_TXN, S_JOIN_SUFFIX, S_JOIN_PHONE, S_JOIN_IMAGE = (
    "join_choose_channel", "join_await_txn", "join_await_suffix", "join_await_phone", "join_await_image",
)
S_SETUP_ID, S_SETUP_PRICE, S_SETUP_DAYS, S_SETUP_BANK = (
    "setup_channel_id", "setup_price", "setup_billing_days", "setup_bank",
)
S_SETUP_ACCOUNT, S_SETUP_ACCNAME = "setup_account_number", "setup_account_name"
S_EDIT_CHOOSE, S_EDIT_PRICE, S_EDIT_DAYS, S_EDIT_BANK = (
    "edit_choose_field", "edit_price", "edit_days", "edit_bank",
)
S_EDIT_ACCOUNT, S_EDIT_ACCNAME = "edit_account", "edit_account_name"
S_BROADCAST_MSG, S_BROADCAST_CONFIRM = "broadcast_message", "broadcast_confirm"
S_ADD_ADMIN = "add_admin"

RATE_LIMITS = {
    "verify_attempt": (5, 3600),
    "join_request": (10, 3600),
    "setup_channel": (3, 86400),
    "general_commands": (30, 60),
}

WARNING_TYPES = {4: "warning_4d", 3: "warning_3d", 2: "warning_2d", 1: "warning_1d", 0: "warning_today"}
WARNING_EMOJI = {
    "warning_4d": "⚠️", "warning_3d": "⚠️⚠️", "warning_2d": "🚨",
    "warning_1d": "🔴 URGENT", "warning_today": "🔴🔴 LAST DAY",
}

# In-memory pending join requests (safe to lose on restart — rare edge case)
_pending_join: dict[str, str] = {}


# ── Database ──────────────────────────────────────────────────────────────────

class Database:
    def __init__(self, path: str):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._init_schema()

    async def close(self):
        if self._conn:
            await self._conn.close()

    @asynccontextmanager
    async def transaction(self):
        try:
            yield self._conn
            await self._conn.commit()
        except Exception:
            await self._conn.rollback()
            raise

    async def _init_schema(self):
        await self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS channels (
                id              INTEGER PRIMARY KEY,
                title           TEXT NOT NULL,
                admin_id        INTEGER NOT NULL,
                price           REAL NOT NULL CHECK (price > 0),
                billing_days    INTEGER NOT NULL DEFAULT 30 CHECK (billing_days > 0),
                bank            TEXT NOT NULL,
                account_number  TEXT NOT NULL,
                account_name    TEXT NOT NULL,
                join_link       TEXT,
                is_active       INTEGER NOT NULL DEFAULT 1,
                commission_pct  REAL DEFAULT 0,
                created_at      TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_channels_admin ON channels(admin_id);
            CREATE INDEX IF NOT EXISTS idx_channels_active ON channels(is_active);

            CREATE TABLE IF NOT EXISTS members (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id      INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
                user_id         INTEGER NOT NULL,
                username        TEXT DEFAULT '',
                first_name      TEXT DEFAULT '',
                expires_at      TEXT NOT NULL,
                txn_id          TEXT NOT NULL,
                paid_amount     REAL,
                paid_at         TEXT NOT NULL DEFAULT (datetime('now')),
                is_active       INTEGER NOT NULL DEFAULT 1,
                created_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_members_active_unique
                ON members(channel_id, user_id) WHERE is_active = 1;
            CREATE INDEX IF NOT EXISTS idx_members_expires ON members(expires_at);
            CREATE INDEX IF NOT EXISTS idx_members_channel ON members(channel_id);
            CREATE INDEX IF NOT EXISTS idx_members_user ON members(user_id);

            CREATE TABLE IF NOT EXISTS transactions (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id         INTEGER NOT NULL,
                channel_id      INTEGER,
                txn_id          TEXT,
                bank            TEXT NOT NULL,
                amount_verified REAL,
                status          TEXT NOT NULL DEFAULT 'pending',
                raw_response    TEXT,
                verified_at     TEXT,
                created_at      TEXT NOT NULL DEFAULT (datetime('now'))
            );
            CREATE INDEX IF NOT EXISTS idx_transactions_txn ON transactions(txn_id);
            CREATE INDEX IF NOT EXISTS idx_transactions_user ON transactions(user_id);
            CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);

            CREATE TABLE IF NOT EXISTS channel_admins (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id  INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
                user_id     INTEGER NOT NULL,
                role        TEXT NOT NULL DEFAULT 'owner',
                added_at    TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(channel_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS notifications (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                member_id   INTEGER NOT NULL REFERENCES members(id) ON DELETE CASCADE,
                notify_type TEXT NOT NULL,
                sent_at     TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(member_id, notify_type)
            );

            CREATE TABLE IF NOT EXISTS user_sessions (
                user_id     INTEGER PRIMARY KEY,
                state       TEXT NOT NULL DEFAULT 'idle',
                data        TEXT NOT NULL DEFAULT '{}',
                updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS rate_limits (
                user_id     INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                count       INTEGER NOT NULL DEFAULT 1,
                window_start REAL NOT NULL,
                PRIMARY KEY (user_id, action_type)
            );
        """)
        await self._conn.commit()

    async def migrate_legacy_json(self):
        """Migrate data from the old paid_channels.json format."""
        if not os.path.exists(LEGACY_JSON):
            return
        cur = await self._conn.execute("SELECT COUNT(*) FROM channels")
        if (await cur.fetchone())[0] > 0:
            logger.info("Database already has channels, skipping JSON migration.")
            return
        try:
            with open(LEGACY_JSON, encoding="utf-8") as f:
                old = json.load(f)
        except Exception as e:
            logger.warning("Legacy JSON migration skipped: %s", e)
            return

        migrated_channels = 0
        migrated_members = 0
        for cid, ch in old.get("channels", {}).items():
            await self._conn.execute(
                """INSERT OR IGNORE INTO channels
                   (id, title, admin_id, price, billing_days, bank, account_number,
                    account_name, join_link, is_active)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    int(cid), ch["title"], ch["admin_id"], ch["price"],
                    ch.get("billing_days", 30), ch["bank"], ch.get("account", ""),
                    ch.get("account_name", ""), ch.get("join_request_link", ""),
                    1 if ch.get("active", True) else 0,
                ),
            )
            await self._conn.execute(
                "INSERT OR IGNORE INTO channel_admins (channel_id, user_id, role) VALUES (?,?,?)",
                (int(cid), ch["admin_id"], "owner"),
            )
            migrated_channels += 1

        for cid, members in old.get("members", {}).items():
            for uid, m in members.items():
                exp = m.get("expires_at", -1)
                if exp < 0:
                    exp_dt = "9999-12-31 23:59:59"
                else:
                    exp_dt = datetime.fromtimestamp(exp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                paid = datetime.fromtimestamp(
                    m.get("paid_at", time.time()), tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S")
                await self._conn.execute(
                    """INSERT OR IGNORE INTO members
                       (channel_id, user_id, username, first_name, expires_at, txn_id, paid_at, is_active)
                       VALUES (?,?,?,?,?,?,?,1)""",
                    (int(cid), int(uid), m.get("username", ""), m.get("first_name", ""),
                     exp_dt, m.get("txn_id", "migrated"), paid),
                )
                migrated_members += 1

        await self._conn.commit()
        logger.info("Migrated %d channels and %d members from %s", migrated_channels, migrated_members, LEGACY_JSON)


db = Database(DB_PATH)


# ── Session manager (DB-backed) ───────────────────────────────────────────────

async def session_get(user_id: int) -> tuple[str, dict]:
    cur = await db._conn.execute(
        "SELECT state, data FROM user_sessions WHERE user_id = ?", (user_id,),
    )
    row = await cur.fetchone()
    if row:
        try:
            return row["state"], json.loads(row["data"])
        except json.JSONDecodeError:
            return S_IDLE, {}
    return S_IDLE, {}


async def session_set(user_id: int, state: str, data: dict | None = None):
    _, existing = await session_get(user_id)
    merged = {**existing, **(data or {})}
    await db._conn.execute(
        """INSERT INTO user_sessions (user_id, state, data, updated_at)
           VALUES (?,?,?,datetime('now'))
           ON CONFLICT(user_id) DO UPDATE SET state=excluded.state, data=excluded.data,
           updated_at=datetime('now')""",
        (user_id, state, json.dumps(merged)),
    )
    await db._conn.commit()


async def session_clear(user_id: int):
    await db._conn.execute("DELETE FROM user_sessions WHERE user_id = ?", (user_id,))
    await db._conn.commit()


async def session_update_data(user_id: int, **kwargs):
    state, data = await session_get(user_id)
    data.update(kwargs)
    await session_set(user_id, state, data)


# ── Rate limiting ─────────────────────────────────────────────────────────────

async def check_rate_limit(user_id: int, action: str) -> tuple[bool, int]:
    max_count, window = RATE_LIMITS.get(action, (30, 60))
    now = time.time()
    cur = await db._conn.execute(
        "SELECT count, window_start FROM rate_limits WHERE user_id=? AND action_type=?",
        (user_id, action),
    )
    row = await cur.fetchone()
    if not row or now - row["window_start"] > window:
        await db._conn.execute(
            """INSERT INTO rate_limits (user_id, action_type, count, window_start)
               VALUES (?,?,1,?) ON CONFLICT(user_id, action_type)
               DO UPDATE SET count=1, window_start=?""",
            (user_id, action, now, now),
        )
        await db._conn.commit()
        return True, 0
    if row["count"] >= max_count:
        wait = int(window - (now - row["window_start"]))
        return False, max(wait, 60)
    await db._conn.execute(
        "UPDATE rate_limits SET count = count + 1 WHERE user_id=? AND action_type=?",
        (user_id, action),
    )
    await db._conn.commit()
    return True, 0


# ── Verification API (async httpx) ────────────────────────────────────────────

def humanize_api_error(error: str, bank: str = "") -> str:
    """Turn raw API errors into actionable user messages."""
    if not error:
        return "Verification failed. Please try again."
    low = error.lower()
    if "pdf" in low or "puppeteer" in low:
        if bank in (BANK_CBE, "cbe"):
            return (
                "Could not verify this CBE transaction.\n\n"
                "*Please check:*\n"
                "• Transaction ID is correct (e.g. `FT260866LG32`)\n"
                "• Wait 2–5 minutes after paying before submitting\n"
                "• Use the last 8 digits of the account *you paid from*\n"
                "• Or tap *📷 Send Screenshot* below and upload your receipt"
            )
        if bank in (BANK_ABYSSINIA, "abyssinia"):
            return (
                "Could not verify this Abyssinia transaction.\n\n"
                "Check your reference number and enter the last 5 digits "
                "of the account you paid from, or send a screenshot."
            )
        return (
            "Could not fetch the receipt from the bank.\n\n"
            "Try again in a few minutes, or send a screenshot of your receipt."
        )
    if "not found" in low:
        return (
            "Transaction not found.\n\n"
            "• Double-check the Transaction ID on your receipt\n"
            "• Make sure the payment completed successfully\n"
            "• Wait a few minutes if you just paid"
        )
    return error


class VerificationService:
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=10.0),
            headers={"x-api-key": API_KEY},
        )

    async def close(self):
        await self.client.aclose()

    async def _post(self, endpoint: str, payload: dict, bank: str = "") -> dict:
        try:
            r = await self.client.post(f"{API_BASE}/{endpoint}", json=payload)
            r.raise_for_status()
            data = r.json()
            if not data.get("success"):
                raw_err = data.get("error") or data.get("message") or "Verification failed"
                b = bank or endpoint.replace("verify-", "")
                return {"success": False, "error": humanize_api_error(raw_err, b), "raw_error": raw_err}
            return data
        except httpx.HTTPStatusError as e:
            logger.error("API %s %s: %s", endpoint, e.response.status_code, e.response.text[:200])
            code = e.response.status_code
            if code == 401:
                return {"success": False, "error": "Invalid API key. Check VERIFIER_API_KEY."}
            if code == 429:
                return {"success": False, "error": "Verification service rate limit. Try in a moment."}
            if code == 400:
                return {"success": False, "error": "❌ Invalid payment details.\n\n*Please check:*\n• Transaction ID is correct & not expired\n• For CBE: Ensure you used the correct account\n• For Telebirr: Check receipt number format\n\n_Make a new payment if needed._"}
            if code == 404:
                return {"success": False, "error": "❌ Transaction not found.\n\nPlease verify:\n• The transaction ID is correct\n• The payment was completed\n• Check your bank receipt\n\n_Try submitting again or contact support._"}
            if code == 500:
                return {"success": False, "error": "❌ Verification service error.\n\n_This is temporary. Please try again in a moment._"}
            return {"success": False, "error": f"❌ Verification service temporarily unavailable (error {code}).\n\n_Please try again in a few moments._"}
        except httpx.TimeoutException:
            logger.warning("API timeout on %s", endpoint)
            return {"success": False, "error": "❌ Verification taking too long.\n\n_The service is slow. Please try again in a moment._"}
        except httpx.RequestError as e:
            logger.error("API request failed: %s", e)
            return {"success": False, "error": "❌ Cannot connect to verification service.\n\n*Check:*\n• Your internet connection\n• Try again in a few moments\n• Contact support if problem persists"}

    async def _post_file(self, endpoint: str, file_bytes: bytes, filename: str, extra: dict | None = None) -> dict:
        try:
            files = {"file": (filename, BytesIO(file_bytes), "image/jpeg")}
            data = extra or {}
            r = await self.client.post(
                f"{API_BASE}/{endpoint}",
                files=files, data=data,
                headers={"x-api-key": API_KEY},
            )
            r.raise_for_status()
            return r.json()
        except httpx.TimeoutException:
            return {"success": False, "error": "❌ Image analysis took too long.\n\n_Please try again. If the image is large, send a smaller file._"}
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            logger.error("Image API error: %s", e)
            try:
                response_json = e.response.json()
                error_msg = response_json.get("error", "")
            except Exception:
                error_msg = ""
            
            if "PDF" in error_msg or "Puppeteer" in error_msg:
                return {"success": False, "error": "❌ Cannot recognize payment receipt in image.\n\n*Please:*\n• Send a clear screenshot of the receipt\n• Make sure the entire receipt is visible\n• Ensure text is readable (good lighting)\n• Don't crop out important details\n• Try a different payment receipt if this one is unclear"}
            
            if code == 400:
                return {"success": False, "error": "❌ Cannot read payment receipt.\n\n*Please make sure:*\n• Image is clear and readable\n• Receipt shows full transaction details\n• Try a screenshot instead of photo\n• Ensure lighting is good"}
            if code == 404:
                return {"success": False, "error": "❌ Transaction details not found in image.\n\n*Check if:*\n• You uploaded the correct receipt\n• The receipt shows the amount & date clearly\n• No parts of the receipt are cut off"}
            return {"success": False, "error": "❌ Image verification service error.\n\n_Please try again in a moment._"}
        except httpx.RequestError as e:
            logger.error("Image API error: %s", e)
            return {"success": False, "error": "❌ Cannot reach image verification service.\n\n_Check your internet and try again._"}

    async def verify(self, bank: str, params: dict, channel: dict | None = None) -> dict:
        if bank == BANK_CBE:
            ref = params["reference"]
            payer_suffix = params.get("suffix", "").strip()
            # Try payer account suffix first, then receiver (channel) account suffix
            receiver_acct = (channel or {}).get("account_number", "").strip()
            receiver_suffix = receiver_acct[-8:] if len(receiver_acct) >= 8 else receiver_acct
            suffixes = []
            for s in (payer_suffix, receiver_suffix):
                if s and s not in suffixes:
                    suffixes.append(s)
            if not suffixes:
                return {
                    "success": False,
                    "error": (
                        "CBE verification needs the last 8 digits of your account.\n\n"
                        "Enter them when prompted after your Transaction ID."
                    ),
                }
            last = None
            for suffix in suffixes:
                last = await self._post(
                    "verify-cbe",
                    {"reference": ref, "accountSuffix": suffix},
                    bank=BANK_CBE,
                )
                if last.get("success"):
                    return last
            return last or {"success": False, "error": "CBE verification failed."}
        if bank == BANK_TELEBIRR:
            return await self._post("verify-telebirr", {"reference": params["reference"]}, bank=BANK_TELEBIRR)
        if bank == BANK_DASHEN:
            return await self._post("verify-dashen", {"reference": params["reference"]}, bank=BANK_DASHEN)
        if bank == BANK_ABYSSINIA:
            ref = params["reference"]
            payer_suffix = params.get("suffix", "").strip()
            receiver_acct = (channel or {}).get("account_number", "").strip()
            receiver_suffix = receiver_acct[-5:] if len(receiver_acct) >= 5 else receiver_acct
            suffixes = []
            for s in (payer_suffix, receiver_suffix):
                if s and s not in suffixes:
                    suffixes.append(s)
            if not suffixes:
                return {"success": False, "error": "Enter the last 5 digits of your Abyssinia account."}
            last = None
            for suffix in suffixes:
                last = await self._post(
                    "verify-abyssinia",
                    {"reference": ref, "suffix": suffix},
                    bank=BANK_ABYSSINIA,
                )
                if last.get("success"):
                    return last
            return last or {"success": False, "error": "Abyssinia verification failed."}
        if bank == BANK_CBEBIRR:
            return await self._post("verify-cbebirr", {
                "receiptNumber": params["reference"],
                "phoneNumber": params.get("phone", ""),
            }, bank=BANK_CBEBIRR)
        if bank == BANK_IMAGE:
            extra = {"autoVerify": "true"}
            if params.get("suffix"):
                extra["suffix"] = params["suffix"]
            return await self._post_file("verify-image", params["image_bytes"], params["filename"], extra)
        return {"success": False, "error": "Unknown payment method."}


verifier = VerificationService()


@dataclass
class VerificationResult:
    success: bool
    amount: Optional[float] = None
    txn_id: str = ""
    error: Optional[str] = None
    status: str = "failed"
    raw: dict | None = None


def extract_amount(raw: dict) -> Optional[float]:
    d = raw.get("data", raw)
    for key in ("amount", "settledAmount", "transactionAmount", "totalPaidAmount"):
        val = d.get(key)
        if val is not None:
            try:
                return float(str(val).replace(",", "").strip())
            except (ValueError, TypeError):
                pass
    return None


def extract_txn_id(raw: dict, bank: str, ref: str) -> str:
    d = raw.get("data", raw)
    mapping = {
        BANK_CBE: "reference",
        BANK_TELEBIRR: "receiptNo",
        BANK_DASHEN: "transactionReference",
        BANK_ABYSSINIA: "reference",
        BANK_CBEBIRR: "receiptNumber",
    }
    return (d.get(mapping.get(bank, "reference"), ref) or ref).strip()


def parse_txn_date(raw: dict) -> Optional[datetime]:
    d = raw.get("data", raw)
    for key in ("date", "paymentDate", "transactionDate", "createdAt"):
        val = d.get(key)
        if not val:
            continue
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(str(val).strip()[:19], fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


async def is_duplicate_txn(txn_id: str, bank: str, channel_id: int | None = None) -> bool:
    txn_id = txn_id.strip().upper()
    if channel_id:
        cur = await db._conn.execute(
            """SELECT 1 FROM transactions
               WHERE UPPER(txn_id)=? AND bank=? AND channel_id=? AND status='verified'
               AND created_at > datetime('now', '-24 hours') LIMIT 1""",
            (txn_id, bank, channel_id),
        )
    else:
        cur = await db._conn.execute(
            """SELECT 1 FROM transactions
               WHERE UPPER(txn_id)=? AND bank=? AND status='verified'
               AND created_at > datetime('now', '-24 hours') LIMIT 1""",
            (txn_id, bank),
        )
    return await cur.fetchone() is not None


async def log_transaction(
    user_id: int, channel_id: int | None, txn_id: str, bank: str,
    amount: float | None, status: str, raw: dict,
):
    await db._conn.execute(
        """INSERT INTO transactions
           (user_id, channel_id, txn_id, bank, amount_verified, status, raw_response, verified_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (
            user_id, channel_id, txn_id, bank, amount, status,
            json.dumps(raw)[:8000],
            datetime.now(timezone.utc).isoformat() if status == "verified" else None,
        ),
    )
    await db._conn.commit()


async def verify_for_channel(
    user_id: int, bank: str, channel: dict, params: dict,
) -> VerificationResult:
    ok, wait = await check_rate_limit(user_id, "verify_attempt")
    if not ok:
        mins = wait // 60
        return VerificationResult(
            False,
            error=f"Too many attempts. Please wait {mins} minute{'s' if mins != 1 else ''}.",
            status="failed",
        )

    raw = await verifier.verify(bank, params, channel=channel)
    ref = params.get("reference", params.get("filename", "image"))

    if not raw.get("success"):
        err = raw.get("error") or raw.get("message") or "❌ Verification failed. Please check your transaction details and try again."
        await log_transaction(user_id, channel["id"], ref, bank, None, "failed", raw)
        return VerificationResult(False, error=err, status="failed", raw=raw)

    amount = extract_amount(raw)
    txn_id = extract_txn_id(raw, bank, ref)

    if await is_duplicate_txn(txn_id, bank, channel["id"]):
        await log_transaction(user_id, channel["id"], txn_id, bank, amount, "duplicate", raw)
        return VerificationResult(
            False,
            error="❌ This transaction was already used for a subscription. Each payment can only be used once.",
            status="duplicate",
            raw=raw,
        )

    txn_date = parse_txn_date(raw)
    if txn_date and datetime.now(timezone.utc) - txn_date > timedelta(hours=TXN_MAX_AGE_HOURS):
        await log_transaction(user_id, channel["id"], txn_id, bank, amount, "expired_txn", raw)
        return VerificationResult(
            False,
            error=f"Transaction is older than {TXN_MAX_AGE_HOURS} hours. Please make a fresh payment.",
            status="expired_txn",
            raw=raw,
        )

    # Check receiver account suffix matches channel admin's account
    d = raw.get("data", raw)
    receiver_account = ""
    for key in ("receiverAccount", "receiveraccount", "receiver_account"):
        if key in d:
            receiver_account = str(d[key]).replace(" ", "").replace("-", "")
            break
    
    channel_account = channel.get("account_number", "").replace(" ", "").replace("-", "")
    if receiver_account and channel_account:
        # Get last few digits for comparison (8 for CBE, 5 for others)
        suffix_len = 8 if bank == BANK_CBE else 5
        receiver_suffix = receiver_account[-suffix_len:] if len(receiver_account) >= suffix_len else receiver_account
        channel_suffix = channel_account[-suffix_len:] if len(channel_account) >= suffix_len else channel_account
        
        if receiver_suffix != channel_suffix:
            await log_transaction(user_id, channel["id"], txn_id, bank, amount, "account_mismatch", raw)
            return VerificationResult(
                False,
                error=f"Account mismatch: The payment was made to a different account. Please pay to the correct account ending in `{channel_suffix}`.",
                status="account_mismatch",
                raw=raw,
            )

    # Check receiver name matches channel account name (case-insensitive)
    receiver_name = ""
    for key in ("receiverName", "receivername", "receiver_name", "receiver"):
        if key in d:
            receiver_name = str(d[key]).strip().lower()
            break
    
    channel_name = channel.get("account_name", "").strip().lower()
    if receiver_name and channel_name:
        # Simple comparison - check if channel name is contained in receiver name or vice versa
        # This handles cases like "John Doe" vs "JOHN DOE" or "John Doe" vs "Doe John"
        if channel_name not in receiver_name and receiver_name not in channel_name:
            await log_transaction(user_id, channel["id"], txn_id, bank, amount, "name_mismatch", raw)
            return VerificationResult(
                False,
                error=f"Name mismatch: The payment was made to a different name. Please pay to the correct account name: `{channel.get('account_name')}`.",
                status="name_mismatch",
                raw=raw,
            )

    # Check exact amount match (no tolerance)
    price = channel["price"]
    if amount is not None:
        if abs(amount - price) > 0.01:  # Allow tiny floating point differences
            await log_transaction(user_id, channel["id"], txn_id, bank, amount, "amount_mismatch", raw)
            return VerificationResult(
                False,
                amount=amount,
                error=(
                    f"Amount mismatch: you paid {amount:,.2f} ETB but "
                    f"the subscription requires exactly {price:,.0f} ETB."
                ),
                status="amount_mismatch",
                raw=raw,
            )

    await log_transaction(user_id, channel["id"], txn_id, bank, amount, "verified", raw)
    return VerificationResult(True, amount=amount, txn_id=txn_id, status="verified", raw=raw)


# ── Channel / member helpers ──────────────────────────────────────────────────

async def get_channel(channel_id: int | str) -> dict | None:
    try:
        cur = await db._conn.execute("SELECT * FROM channels WHERE id = ?", (int(channel_id),))
        row = await cur.fetchone()
        return dict(row) if row else None
    except (ValueError, TypeError):
        return None


async def is_channel_admin(channel_id: int, user_id: int) -> bool:
    cur = await db._conn.execute(
        "SELECT 1 FROM channel_admins WHERE channel_id=? AND user_id=?", (channel_id, user_id),
    )
    return await cur.fetchone() is not None


async def admin_channels(user_id: int) -> list[dict]:
    cur = await db._conn.execute(
        """SELECT c.* FROM channels c
           JOIN channel_admins ca ON c.id = ca.channel_id
           WHERE ca.user_id = ? ORDER BY c.title""",
        (user_id,),
    )
    return [dict(r) for r in await cur.fetchall()]


async def get_active_member(channel_id: int, user_id: int) -> dict | None:
    cur = await db._conn.execute(
        """SELECT * FROM members
           WHERE channel_id=? AND user_id=? AND is_active=1
           AND expires_at > datetime('now') LIMIT 1""",
        (channel_id, user_id),
    )
    row = await cur.fetchone()
    return dict(row) if row else None


async def get_user_subscriptions(user_id: int) -> list[dict]:
    cur = await db._conn.execute(
        """SELECT m.*, c.title, c.join_link, c.price, c.billing_days
           FROM members m JOIN channels c ON m.channel_id = c.id
           WHERE m.user_id=? AND m.is_active=1 AND m.expires_at > datetime('now')
           ORDER BY m.expires_at""",
        (user_id,),
    )
    return [dict(r) for r in await cur.fetchall()]


async def get_channel_members(channel_id: int) -> list[dict]:
    cur = await db._conn.execute(
        """SELECT * FROM members WHERE channel_id=? AND is_active=1
           ORDER BY expires_at""",
        (channel_id,),
    )
    return [dict(r) for r in await cur.fetchall()]


async def get_channel_transactions(
    channel_id: int, limit: int = 50, status: str | None = None
) -> list[dict]:
    """All transactions for a channel, newest first."""
    if status:
        cur = await db._conn.execute(
            """SELECT * FROM transactions
               WHERE channel_id=? AND status=?
               ORDER BY created_at DESC LIMIT ?""",
            (channel_id, status, limit),
        )
    else:
        cur = await db._conn.execute(
            """SELECT * FROM transactions
               WHERE channel_id=?
               ORDER BY created_at DESC LIMIT ?""",
            (channel_id, limit),
        )
    return [dict(r) for r in await cur.fetchall()]


async def get_channel_transactions_for_user(
    user_id: int, channel_id: int, limit: int = 10
) -> list[dict]:
    """All transactions a specific user has made for a channel."""
    cur = await db._conn.execute(
        """SELECT * FROM transactions
           WHERE user_id=? AND channel_id=?
           ORDER BY created_at DESC LIMIT ?""",
        (user_id, channel_id, limit),
    )
    return [dict(r) for r in await cur.fetchall()]


async def get_transaction_by_db_id(db_id: int) -> dict | None:
    cur = await db._conn.execute("SELECT * FROM transactions WHERE id=?", (db_id,))
    row = await cur.fetchone()
    return dict(row) if row else None


def build_deep_link(channel_id: int | str) -> str:
    if not BOT_USERNAME:
        return ""
    cid_safe = str(channel_id).lstrip("-")
    return f"https://t.me/{BOT_USERNAME}?start=chn{cid_safe}"


async def sync_bot_username(bot) -> str:
    """Fetch the real bot username from Telegram (don't trust .env blindly)."""
    global BOT_USERNAME
    try:
        me = await bot.get_me()
        actual = (me.username or "").lstrip("@")
        if not actual:
            logger.warning("Bot has no username — set one in @BotFather to enable join links.")
            return BOT_USERNAME
        if BOT_USERNAME and BOT_USERNAME.lower() != actual.lower():
            logger.warning(
                "BOT_USERNAME in .env ('%s') differs from Telegram ('@%s') — using @%s",
                BOT_USERNAME, actual, actual,
            )
        BOT_USERNAME = actual
    except TelegramError as e:
        logger.error("Could not fetch bot username: %s", e)
    return BOT_USERNAME


def parse_deep_link(arg: str) -> int | None:
    if arg.startswith("chn") and arg[3:].isdigit():
        return int(f"-{arg[3:]}")
    return None


def fmt_expiry(iso_str: str) -> str:
    if not iso_str or iso_str.startswith("9999"):
        return "♾️ Lifetime"
    try:
        # Handle both ISO and space-separated formats
        clean = iso_str.replace("T", " ")[:16]
        dt = datetime.strptime(clean, "%Y-%m-%d %H:%M")
        return dt.strftime("%d %b %Y, %H:%M")
    except Exception:
        return iso_str[:16]


def days_until(iso_str: str) -> int:
    """Return days remaining until expiry (negative if expired)."""
    try:
        clean = iso_str.replace("T", " ")[:19]
        dt = datetime.strptime(clean, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return (dt.date() - datetime.now(timezone.utc).date()).days
    except Exception:
        return 0


async def ensure_join_links():
    """Rebuild join links if BOT_USERNAME changed."""
    if not BOT_USERNAME:
        return
    cur = await db._conn.execute("SELECT id, join_link FROM channels")
    updated = 0
    for row in await cur.fetchall():
        expected = build_deep_link(row["id"])
        if row["join_link"] != expected:
            await db._conn.execute(
                "UPDATE channels SET join_link=?, updated_at=datetime('now') WHERE id=?",
                (expected, row["id"]),
            )
            updated += 1
    if updated:
        await db._conn.commit()
        logger.info("Updated %d join links for @%s", updated, BOT_USERNAME)


async def grant_access(
    app: Application, channel: dict, user, txn_id: str, amount: float | None
) -> tuple[str, str]:
    """Grant or renew subscription. Returns (invite_link_url, expiry_display_str)."""
    cid = channel["id"]
    uid = user.id
    existing = await get_active_member(cid, uid)
    now = datetime.now(timezone.utc)

    if existing:
        # Renewal: extend from current expiry, not from now
        old_exp_str = existing["expires_at"].replace("Z", "+00:00").replace(" ", "T")
        try:
            old_exp = datetime.fromisoformat(old_exp_str)
            if old_exp.tzinfo is None:
                old_exp = old_exp.replace(tzinfo=timezone.utc)
        except ValueError:
            old_exp = now
        base = max(old_exp, now)
        new_exp = base + timedelta(days=channel["billing_days"])
        await db._conn.execute(
            """UPDATE members SET expires_at=?, txn_id=?, paid_amount=?, paid_at=datetime('now')
               WHERE id=?""",
            (new_exp.strftime("%Y-%m-%d %H:%M:%S"), txn_id, amount, existing["id"]),
        )
    else:
        new_exp = now + timedelta(days=channel["billing_days"])
        await db._conn.execute(
            """INSERT INTO members
               (channel_id, user_id, username, first_name, expires_at, txn_id, paid_amount, is_active)
               VALUES (?,?,?,?,?,?,?,1)""",
            (
                cid, uid,
                (user.username or "")[:100],
                (user.first_name or "")[:100],
                new_exp.strftime("%Y-%m-%d %H:%M:%S"),
                txn_id, amount,
            ),
        )

    await db._conn.commit()

    # Approve pending join request if any
    pending_cid = _pending_join.get(str(uid))
    if pending_cid and str(pending_cid) == str(cid):
        try:
            await app.bot.approve_chat_join_request(cid, uid)
        except TelegramError:
            pass
        _pending_join.pop(str(uid), None)

    # Create single-use invite link
    invite = await app.bot.create_chat_invite_link(
        chat_id=cid,
        member_limit=1,
        expire_date=now + timedelta(hours=INVITE_LINK_EXPIRY_HOURS),
    )
    return invite.invite_link, fmt_expiry(new_exp.strftime("%Y-%m-%d %H:%M:%S"))


# ── Formatters ────────────────────────────────────────────────────────────────

def _fmt_etb(amount) -> str:
    try:
        return f"{float(amount):,.2f} ETB"
    except Exception:
        return str(amount)


def _fmt_field_value(key: str, value) -> str:
    """Format a single field value for display, detecting amounts and dates."""
    if value is None or value == "":
        return "N/A"
    amount_keys = {"amount", "settledamount", "transactionamount", "totalpaidamount",
                   "paidamount", "transferamount", "fee", "charge", "balance"}
    if key.lower().replace("_", "").replace(" ", "") in amount_keys:
        try:
            return f"{float(str(value).replace(',', '')):,.2f} ETB"
        except (ValueError, TypeError):
            pass
    return str(value)


# Field display order hints per bank (fields listed first; remainder appended alphabetically)
_FIELD_ORDER = {
    BANK_CBE:       ["reference", "date", "amount", "payer", "payerAccount",
                     "receiver", "receiverAccount", "description", "type", "status"],
    BANK_TELEBIRR:  ["receiptNo", "paymentDate", "settledAmount", "payerName",
                     "payerPhone", "receiverName", "receiverPhone", "description", "status"],
    BANK_DASHEN:    ["transactionReference", "transactionDate", "transactionAmount",
                     "senderName", "senderAccount", "receiverName", "receiverAccount",
                     "narration", "status"],
    BANK_ABYSSINIA: ["reference", "date", "amount", "payer", "payerAccount",
                     "receiver", "receiverAccount", "description", "status"],
    BANK_CBEBIRR:   ["receiptNumber", "paymentDate", "amount", "payerName",
                     "payerPhone", "receiverName", "receiverPhone", "description", "status"],
}

_BANK_HEADERS = {
    BANK_CBE:       "🏦 *CBE Payment Verified*",
    BANK_TELEBIRR:  "📱 *Telebirr Payment Verified*",
    BANK_DASHEN:    "🏦 *Dashen Bank Payment Verified*",
    BANK_ABYSSINIA: "🏦 *Bank of Abyssinia Payment Verified*",
    BANK_CBEBIRR:   "📱 *CBE Birr Payment Verified*",
}

_SKIP_KEYS = {"success", "detectedtype", "type"}

_FIELD_EMOJI = {
    "reference": "📋", "receiptno": "📋", "receiptnumber": "📋",
    "transactionreference": "📋", "txnid": "📋",
    "date": "📅", "paymentdate": "📅", "transactiondate": "📅", "createdat": "📅",
    "amount": "💰", "settledamount": "💰", "transactionamount": "💰",
    "totalpaidamount": "💰", "fee": "💸", "charge": "💸",
    "payer": "👤", "payername": "👤", "sendername": "👤",
    "payeraccount": "🔢", "senderaccount": "🔢",
    "payerphone": "📞", "senderphone": "📞",
    "receiver": "🏦", "receivername": "🏦",
    "receiveraccount": "🔢",
    "receiverphone": "📞",
    "description": "📝", "narration": "📝", "remark": "📝",
    "status": "🔵", "state": "🔵",
    "balance": "💳", "type": "🏷️",
}


def _all_fields_block(bank: str, d: dict) -> list[str]:
    """Return formatted lines for ALL fields in `d`, ordered sensibly."""
    order = _FIELD_ORDER.get(bank, [])
    seen: set[str] = set()
    lines: list[str] = []

    def _add(k: str, v):
        norm = k.lower().replace("_", "").replace(" ", "")
        if norm in _SKIP_KEYS or norm in seen or v is None or v == "":
            return
        seen.add(norm)
        emoji = _FIELD_EMOJI.get(norm, "📌")
        label = k.replace("_", " ").replace("-", " ").title()
        lines.append(f"{emoji} *{label}:* {_fmt_field_value(k, v)}")

    # Priority fields first
    for key in order:
        if key in d:
            _add(key, d[key])
    # Remaining fields alphabetically
    for key in sorted(d.keys()):
        _add(key, d[key])

    return lines


def fmt_bank_result(bank: str, raw: dict, channel: dict | None = None) -> str:
    """Format full verification result, showing every field the API returned."""
    d = raw.get("data", raw)

    if bank == BANK_IMAGE:
        detected = str(d.get("detectedType", d.get("type", "Screenshot"))).upper()
        header = f"✅ *Receipt Verified ({detected})*"
    else:
        header = "✅ " + _BANK_HEADERS.get(bank, f"*{bank.upper()} Payment Verified*")[2:]

    lines = [header, "━━━━━━━━━━━━━━━━━"]
    lines += _all_fields_block(bank, d)

    # Always append receiver info from channel when available
    if channel:
        lines.append("━━━━━━━━━━━━━━━━━")
        lines.append(f"🏦 *Receiver Account:* `{channel.get('account_number', 'N/A')}`")
        lines.append(f"👤 *Receiver Name:* {channel.get('account_name', 'N/A')}")

    return "\n".join(lines)


def fmt_error(bank: str, raw: dict) -> str:
    err = raw.get("error") or raw.get("message") or "Unknown error."
    bank_label = BANK_LABELS.get(bank, bank)
    return (
        f"❌ *Verification Failed*\n\n"
        f"Bank: {bank_label}\n"
        f"Reason: {err}\n\n"
        f"💡 _Double-check your transaction ID and try again._"
    )


_TXN_STATUS_EMOJI = {
    "verified": "✅", "failed": "❌", "duplicate": "🔁",
    "amount_mismatch": "⚠️", "expired_txn": "⏰", "pending": "⏳",
}


def fmt_transaction_detail(txn: dict, channel: dict | None = None) -> str:
    """Format a full transaction record from the DB, showing every stored API field."""
    bank = txn.get("bank", "unknown")
    se = _TXN_STATUS_EMOJI.get(txn.get("status", ""), "❓")
    lines = [
        f"{se} *Transaction Detail*",
        "━━━━━━━━━━━━━━━━━",
        f"🆔 *Record ID:* `{txn.get('id', 'N/A')}`",
        f"🏷️ *Bank:* {BANK_LABELS.get(bank, bank)}",
        f"🔑 *Txn ID:* `{txn.get('txn_id') or 'N/A'}`",
        f"💰 *Amount:* {_fmt_etb(txn.get('amount_verified'))}",
        f"📶 *Status:* {txn.get('status', 'N/A').replace('_', ' ').title()}",
        f"📅 *Created:* {(txn.get('created_at') or 'N/A')[:19]}",
    ]
    if txn.get("verified_at"):
        lines.append(f"✅ *Verified At:* {txn['verified_at'][:19]}")

    # Parse raw_response and show EVERYTHING the API returned
    raw_str = txn.get("raw_response", "")
    if raw_str:
        try:
            raw = json.loads(raw_str)
            d = raw.get("data", raw)
            clean = {k: v for k, v in d.items()
                     if k.lower() not in ("success",) and v not in (None, "")}
            if clean:
                lines.append("━━━━━━━━━━━━━━━━━")
                lines.append("📋 *Full API Response:*")
                lines.extend(f"  {fl}" for fl in _all_fields_block(bank, clean))
            # Top-level extras outside 'data'
            top_extra = {
                k: v for k, v in raw.items()
                if k not in ("success", "data", "message", "error") and v not in (None, "")
            }
            if top_extra:
                lines.append("📦 *Extra API Fields:*")
                for k, v in sorted(top_extra.items()):
                    lines.append(f"  📌 *{k}:* {v}")
        except (json.JSONDecodeError, TypeError):
            lines.append("\n_Raw response not parseable_")

    # Receiver info from channel record
    if channel:
        lines.append("━━━━━━━━━━━━━━━━━")
        lines.append(f"🏦 *Receiver Account:* `{channel.get('account_number', 'N/A')}`")
        lines.append(f"👤 *Receiver Name:* {channel.get('account_name', 'N/A')}")

    return "\n".join(lines)


# ── Keyboards ─────────────────────────────────────────────────────────────────

def kb_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="act:cancel")]])


def kb_join_payment(channel_id: int | None = None) -> InlineKeyboardMarkup:
    """Join-flow keyboard: screenshot fallback + cancel."""
    rows = []
    if channel_id is not None:
        rows.append([InlineKeyboardButton("📷 Send Screenshot", callback_data=f"joinimg:{channel_id}")])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="act:cancel")])
    return InlineKeyboardMarkup(rows)


def kb_again() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 Verify Another", callback_data="act:again"),
        InlineKeyboardButton("✅ Done", callback_data="act:done"),
    ]])


def kb_banks() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏦 CBE", callback_data=f"bank:{BANK_CBE}"),
         InlineKeyboardButton("📱 Telebirr", callback_data=f"bank:{BANK_TELEBIRR}")],
        [InlineKeyboardButton("🏦 Dashen", callback_data=f"bank:{BANK_DASHEN}"),
         InlineKeyboardButton("🏦 Abyssinia", callback_data=f"bank:{BANK_ABYSSINIA}")],
        [InlineKeyboardButton("📱 CBE Birr", callback_data=f"bank:{BANK_CBEBIRR}"),
         InlineKeyboardButton("🖼️ Screenshot", callback_data=f"bank:{BANK_IMAGE}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="act:cancel")],
    ])


def kb_setup_banks() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏦 CBE", callback_data=f"sbank:{BANK_CBE}"),
         InlineKeyboardButton("📱 Telebirr", callback_data=f"sbank:{BANK_TELEBIRR}")],
        [InlineKeyboardButton("🏦 Dashen", callback_data=f"sbank:{BANK_DASHEN}"),
         InlineKeyboardButton("🏦 Abyssinia", callback_data=f"sbank:{BANK_ABYSSINIA}")],
        [InlineKeyboardButton("📱 CBE Birr", callback_data=f"sbank:{BANK_CBEBIRR}"),
         InlineKeyboardButton("🖼️ Screenshot", callback_data=f"sbank:{BANK_IMAGE}")],
        [InlineKeyboardButton("❌ Cancel", callback_data="act:cancel")],
    ])


def kb_channels(channels: list[dict], prefix: str = "join") -> InlineKeyboardMarkup:
    rows = []
    for ch in channels:
        status = "✅" if ch.get("is_active") else "⏸️"
        label = f"{status} {ch['title']} — {ch['price']:.0f} ETB/{ch['billing_days']}d"
        rows.append([InlineKeyboardButton(label, callback_data=f"{prefix}:{ch['id']}")])
    rows.append([InlineKeyboardButton("❌ Cancel", callback_data="act:cancel")])
    return InlineKeyboardMarkup(rows)


def kb_admin_channels(channels: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for ch in channels:
        icon = "✅" if ch["is_active"] else "⏸️"
        rows.append([InlineKeyboardButton(
            f"{icon} {ch['title']}",
            callback_data=f"adm:{ch['id']}",
        )])
    rows.append([InlineKeyboardButton("➕ Register New Channel", callback_data="act:setup")])
    rows.append([InlineKeyboardButton("❌ Close", callback_data="act:done")])
    return InlineKeyboardMarkup(rows)


def kb_channel_detail(cid: int, active: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏸️ Pause" if active else "▶️ Activate", callback_data=f"tog:{cid}"),
         InlineKeyboardButton("👥 Members", callback_data=f"mem:{cid}")],
        [InlineKeyboardButton("✏️ Edit", callback_data=f"edit:{cid}"),
         InlineKeyboardButton("📊 Stats", callback_data=f"stat:{cid}")],
        [InlineKeyboardButton("📜 Transactions", callback_data=f"ctxn:{cid}"),
         InlineKeyboardButton("📥 Export CSV", callback_data=f"exp:{cid}")],
        [InlineKeyboardButton("📤 Broadcast", callback_data=f"bc:{cid}"),
         InlineKeyboardButton("🗑️ Delete", callback_data=f"del:{cid}")],
        [InlineKeyboardButton("◀️ Back", callback_data="act:channels")],
    ])


def kb_edit_fields(cid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Price", callback_data=f"efld:{cid}:price"),
         InlineKeyboardButton("📅 Billing Days", callback_data=f"efld:{cid}:days")],
        [InlineKeyboardButton("🏦 Bank", callback_data=f"efld:{cid}:bank"),
         InlineKeyboardButton("🔢 Account Number", callback_data=f"efld:{cid}:account")],
        [InlineKeyboardButton("👤 Account Name", callback_data=f"efld:{cid}:accname"),
         InlineKeyboardButton("➕ Add Co-Admin", callback_data=f"efld:{cid}:addadmin")],
        [InlineKeyboardButton("◀️ Back", callback_data=f"adm:{cid}")],
    ])


def kb_confirm_delete(cid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚠️ Yes, Delete", callback_data=f"delconfirm:{cid}"),
         InlineKeyboardButton("❌ Cancel", callback_data=f"adm:{cid}")],
    ])


# ── Scheduled jobs ────────────────────────────────────────────────────────────

async def job_expiry_warnings(app: Application):
    """Send escalating expiry warnings (4d, 3d, 2d, 1d, today)."""
    cur = await db._conn.execute(
        """SELECT m.*, c.title, c.join_link, c.is_active as ch_active
           FROM members m JOIN channels c ON m.channel_id = c.id
           WHERE m.is_active=1 AND c.is_active=1
           AND m.expires_at > datetime('now')
           AND m.expires_at <= datetime('now', '+4 days')""",
    )
    now = datetime.now(timezone.utc)
    sent_count = 0
    for row in await cur.fetchall():
        try:
            exp = datetime.fromisoformat(
                row["expires_at"].replace("Z", "").replace(" ", "T")
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        days_left = (exp.date() - now.date()).days
        notify_type = WARNING_TYPES.get(days_left)
        if not notify_type:
            continue
        # Check if already notified
        chk = await db._conn.execute(
            "SELECT 1 FROM notifications WHERE member_id=? AND notify_type=?",
            (row["id"], notify_type),
        )
        if await chk.fetchone():
            continue
        emoji = WARNING_EMOJI.get(notify_type, "⚠️")
        link = build_deep_link(row["channel_id"])
        day_word = "day" if days_left == 1 else "days"
        if days_left == 0:
            time_msg = "**expires TODAY!**"
        else:
            time_msg = f"expires in *{days_left} {day_word}*"
        text = (
            f"{emoji} *Subscription Expiry Reminder*\n\n"
            f"Your access to *{row['title']}* {time_msg}\n"
            f"📅 Expiry: {fmt_expiry(row['expires_at'])}\n\n"
            f"Renew now to avoid losing access to the channel."
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💳 Renew Subscription", url=link)],
            [InlineKeyboardButton("📋 My Subscriptions", callback_data="act:mysubs")],
        ])
        try:
            await app.bot.send_message(
                row["user_id"], text,
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=kb,
            )
            await db._conn.execute(
                "INSERT OR IGNORE INTO notifications (member_id, notify_type) VALUES (?,?)",
                (row["id"], notify_type),
            )
            sent_count += 1
        except TelegramError:
            pass  # User blocked bot; they'll be kicked when expiry hits
    if sent_count:
        await db._conn.commit()
        logger.info("Sent %d expiry warning(s)", sent_count)


async def job_kick_expired(app: Application):
    """Kick expired members and notify them."""
    cur = await db._conn.execute(
        """SELECT m.*, c.title, c.admin_id, c.join_link, c.price, c.billing_days
           FROM members m JOIN channels c ON m.channel_id = c.id
           WHERE m.is_active=1 AND m.expires_at <= datetime('now')""",
    )
    rows = await cur.fetchall()
    if not rows:
        return

    kicked_count = 0
    for row in rows:
        try:
            await app.bot.ban_chat_member(row["channel_id"], row["user_id"])
            await asyncio.sleep(0.5)
            await app.bot.unban_chat_member(row["channel_id"], row["user_id"])
        except TelegramError as e:
            err_msg = str(e).lower()
            logger.warning("Kick failed for user %s in channel %s: %s", row["user_id"], row["channel_id"], e)
            if "not enough rights" in err_msg or "administrator" in err_msg:
                await db._conn.execute(
                    "UPDATE channels SET is_active=0 WHERE id=?", (row["channel_id"],),
                )
                try:
                    await app.bot.send_message(
                        row["admin_id"],
                        f"❌ *Bot lost admin access in {row['title']}!*\n\n"
                        f"Re-add me as administrator with Invite & Ban permissions, "
                        f"then use /my\\_channels to reactivate.",
                        parse_mode=ParseMode.MARKDOWN,
                    )
                except TelegramError:
                    pass
            # Still mark as inactive regardless
            await db._conn.execute("UPDATE members SET is_active=0 WHERE id=?", (row["id"],))
            continue

        await db._conn.execute("UPDATE members SET is_active=0 WHERE id=?", (row["id"],))
        link = build_deep_link(row["channel_id"])
        try:
            await app.bot.send_message(
                row["user_id"],
                f"⏰ *Subscription Expired*\n\n"
                f"Your subscription to *{row['title']}* has ended.\n"
                f"You've been removed from the channel.\n\n"
                f"Re-subscribe anytime 👇\n{link}",
                parse_mode=ParseMode.MARKDOWN,
                disable_web_page_preview=True,
            )
        except TelegramError:
            pass
        try:
            uname = f"@{row['username']}" if row.get("username") else f"ID:{row['user_id']}"
            await app.bot.send_message(
                row["admin_id"],
                f"📤 *Member expired & removed*\n"
                f"• User: {row['first_name']} ({uname})\n"
                f"• Channel: {row['title']}",
            )
        except TelegramError:
            pass
        kicked_count += 1
        await asyncio.sleep(0.2)

    await db._conn.commit()
    if kicked_count:
        logger.info("Kicked %d expired member(s)", kicked_count)


async def job_daily_cleanup():
    """Remove stale sessions, rate limit records, and old transactions."""
    await db._conn.execute("DELETE FROM user_sessions WHERE updated_at < datetime('now', '-7 days')")
    await db._conn.execute("DELETE FROM rate_limits WHERE window_start < ?", (time.time() - 86400,))
    await db._conn.execute("DELETE FROM transactions WHERE created_at < datetime('now', '-180 days')")
    await db._conn.commit()
    logger.info("Daily cleanup complete")


async def job_health_check(app: Application):
    """Verify bot still has admin rights in all active channels."""
    cur = await db._conn.execute("SELECT id, title, admin_id FROM channels WHERE is_active=1")
    rows = await cur.fetchall()
    deactivated = 0
    for row in rows:
        try:
            member = await app.bot.get_chat_member(row["id"], app.bot.id)
            if member.status not in ("administrator", "creator"):
                raise TelegramError("not admin")
        except TelegramError:
            await db._conn.execute("UPDATE channels SET is_active=0 WHERE id=?", (row["id"],))
            deactivated += 1
            try:
                await app.bot.send_message(
                    row["admin_id"],
                    f"⚠️ *Bot removed from {row['title']}*\n\n"
                    f"I'm no longer an admin there. Channel subscriptions have been paused.\n"
                    f"Re-add me as admin, then use /my\\_channels to reactivate.",
                    parse_mode=ParseMode.MARKDOWN,
                )
            except TelegramError:
                pass
    if deactivated:
        await db._conn.commit()
        logger.warning("Health check deactivated %d channel(s) (lost admin rights)", deactivated)


# ── Permission check ──────────────────────────────────────────────────────────

async def bot_has_channel_perms(bot, channel_id: int) -> tuple[bool, str]:
    try:
        member = await bot.get_chat_member(channel_id, bot.id)
        if member.status not in ("administrator", "creator"):
            return False, (
                "I'm not an administrator in that channel.\n\n"
                "Please add me as admin with:\n"
                "• Invite Users via Link ✅\n"
                "• Ban/Kick Users ✅"
            )
        if not member.can_invite_users:
            return False, "I need the *Invite Users via Link* permission."
        if not member.can_restrict_members:
            return False, "I need the *Ban/Kick Users* permission."
        return True, ""
    except TelegramError as e:
        err = str(e)
        if "chat not found" in err.lower():
            return False, "Channel not found. Make sure I'm already added as admin."
        return False, f"Cannot verify channel permissions: {e}"


# ── Handlers: commands ────────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await session_clear(user.id)

    if ctx.args:
        cid = parse_deep_link(ctx.args[0])
        if cid:
            ch = await get_channel(cid)
            if not ch:
                await update.message.reply_text("❌ Channel not found. The link may be outdated.")
                return
            if not ch["is_active"]:
                await update.message.reply_text(
                    "⏸️ This channel's subscriptions are currently paused.\n"
                    "Please check back later or contact the channel admin."
                )
                return
            existing = await get_active_member(cid, user.id)
            if existing:
                d = days_until(existing["expires_at"])
                day_msg = f"{d} day{'s' if d != 1 else ''}" if d > 0 else "today"
                await update.message.reply_text(
                    f"✅ *You're already subscribed to {ch['title']}!*\n\n"
                    f"📅 Expires: {fmt_expiry(existing['expires_at'])}\n"
                    f"{'⏳ Expires ' + day_msg if d <= 4 else ''}",
                    parse_mode=ParseMode.MARKDOWN,
                )
                return
            await session_set(user.id, S_JOIN_TXN, {"join_channel_id": cid})
            await _send_payment_instructions(update.message, ch, user.first_name)
            return

    # Welcome screen with main menu buttons
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("💼 Browse Channels", callback_data="act:browsechannels"),
         InlineKeyboardButton("📋 My Subscriptions", callback_data="act:mysubs")],
        [InlineKeyboardButton("🔍 Verify Payment", callback_data="act:verify"),
         InlineKeyboardButton("⚙️ Admin Panel", callback_data="act:adminpanel")],
        [InlineKeyboardButton("❓ Help", callback_data="act:help")],
    ])
    await update.message.reply_text(
        f"👋 *Welcome, {user.first_name}!*\n\n"
        "🇪🇹 Ethiopian payment-verified Telegram channel subscriptions.\n\n"
        "💳 Pay via CBE, Telebirr, Dashen, Abyssinia, CBE Birr, or screenshot — "
        "get instant verified access to private channels.\n\n"
        "Choose an option below:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb,
    )


async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *Ethiopian Payment Verifier Bot — Help*\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "*👤 For Members*\n"
        "/join — Browse paid channels\n"
        "/my\\_subscriptions — View your active subscriptions\n"
        "/verify — Verify any payment (standalone)\n"
        "/support — Get help with issues\n"
        "/cancel — Cancel the current operation\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "*⚙️ For Channel Admins*\n"
        "/setup\\_channel — Register a new paid channel\n"
        "/my\\_channels — Manage your channels\n"
        "/members — View paid subscribers\n"
        "/edit\\_channel — Change price, bank, billing period\n"
        "/stats — Revenue and subscriber stats\n"
        "/transactions — Full transaction history with API details\n"
        "/export — Download members as CSV (includes all API fields)\n"
        "/broadcast — Message all subscribers\n"
        "/regenerate\\_links — Fix join links after username change\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "*🏦 Supported Banks*\n"
        "CBE · Telebirr · Dashen · Abyssinia · CBE Birr · Screenshot",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_support(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🆘 *Support Guide*\n\n"
        "*Payment verification failed?*\n"
        "• Double-check your Transaction/Reference ID\n"
        "• Ensure you paid the *exact* amount required\n"
        "• Transaction must be made within the last 24 hours\n"
        "• Each transaction ID can only be used once\n\n"
        "*Amount mismatch?*\n"
        "• Some banks charge small transfer fees — pay slightly more\n\n"
        "*Still stuck?*\n"
        "Contact the channel admin directly. They can manually verify your payment.\n\n"
        "_Use /verify to test a transaction without joining a channel._",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_verify(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await session_clear(update.effective_user.id)
    await session_set(update.effective_user.id, S_CHOOSE)
    await update.message.reply_text(
        "🔍 *Standalone Payment Verification*\n\nSelect your payment method:",
        reply_markup=kb_banks(),
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await session_clear(update.effective_user.id)
    await update.message.reply_text("✅ Operation cancelled.", reply_markup=ReplyKeyboardRemove())


async def cmd_join(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cur = await db._conn.execute("SELECT * FROM channels WHERE is_active=1 ORDER BY title")
    channels = [dict(r) for r in await cur.fetchall()]
    if not channels:
        await update.message.reply_text(
            "📭 No active paid channels at the moment.\n\nCheck back later!"
        )
        return
    await session_set(update.effective_user.id, S_JOIN_CHOOSE)
    await update.message.reply_text(
        f"💼 *Available Channels* ({len(channels)} active)\n\nTap to join:",
        reply_markup=kb_channels(channels),
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_my_subscriptions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    subs = await get_user_subscriptions(update.effective_user.id)
    if not subs:
        await update.message.reply_text(
            "📭 You have no active subscriptions.\n\nUse /join to browse available channels."
        )
        return
    lines = [f"📋 *Your Active Subscriptions* ({len(subs)})\n"]
    for s in subs:
        d = days_until(s["expires_at"])
        if d <= 1:
            urgency = "🔴"
        elif d <= 3:
            urgency = "🟡"
        else:
            urgency = "🟢"
        lines.append(
            f"{urgency} *{s['title']}*\n"
            f"   📅 Expires: {fmt_expiry(s['expires_at'])}\n"
            f"   💰 {s['price']:.0f} ETB / {s['billing_days']}d\n"
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_setup_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    ok, wait = await check_rate_limit(uid, "setup_channel")
    if not ok:
        await update.message.reply_text(f"⏳ Too many setup attempts. Wait {wait // 60} min.")
        return
    existing = await admin_channels(uid)
    if len(existing) >= MAX_CHANNELS_PER_ADMIN:
        await update.message.reply_text(
            f"⚠️ You've reached the maximum of {MAX_CHANNELS_PER_ADMIN} channels per admin.\n"
            f"Delete an existing channel via /my\\_channels first.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    await session_set(uid, S_SETUP_ID)
    await update.message.reply_text(
        "📡 *Channel Setup Wizard*\n\n"
        "*Step 1 of 5 — Channel ID*\n\n"
        "Enter your channel's ID (e.g. `-1001234567890`) or @username.\n\n"
        "💡 _Make sure you've already added me as admin with Invite & Ban permissions._",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_cancel(),
    )


async def cmd_my_channels(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    channels = await admin_channels(update.effective_user.id)
    if not channels:
        await update.message.reply_text(
            "📭 No channels registered yet.\n\nUse /setup\\_channel to add one.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    await update.message.reply_text(
        f"📡 *Your Channels* ({len(channels)})\n\nTap a channel to manage it:",
        reply_markup=kb_admin_channels(channels),
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_members(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    channels = await admin_channels(update.effective_user.id)
    if not channels:
        await update.message.reply_text("No channels registered.")
        return
    lines = ["👥 *Members Overview*\n"]
    for ch in channels:
        members = await get_channel_members(ch["id"])
        status = "✅" if ch["is_active"] else "⏸️"
        lines.append(f"{status} *{ch['title']}* — {len(members)} active member(s)")
        for m in members[:15]:
            uname = f"@{m['username']}" if m.get("username") else f"ID:{m['user_id']}"
            d = days_until(m["expires_at"])
            urgency = "🔴" if d <= 1 else "🟡" if d <= 3 else ""
            lines.append(f"  • {m['first_name']} ({uname}) — {fmt_expiry(m['expires_at'])} {urgency}")
        if len(members) > 15:
            lines.append(f"  _…and {len(members) - 15} more. Use Export for full list._")
        lines.append("")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_edit_channel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    channels = await admin_channels(update.effective_user.id)
    if not channels:
        await update.message.reply_text("No channels to edit.")
        return
    if len(channels) == 1:
        await session_set(update.effective_user.id, S_EDIT_CHOOSE, {"edit_channel_id": channels[0]["id"]})
        await update.message.reply_text(
            f"✏️ *Editing: {channels[0]['title']}*\n\nWhat would you like to change?",
            reply_markup=kb_edit_fields(channels[0]["id"]),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await update.message.reply_text(
            "Select channel to edit:",
            reply_markup=kb_channels(channels, "editpick"),
            parse_mode=ParseMode.MARKDOWN,
        )


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    channels = await admin_channels(update.effective_user.id)
    if not channels:
        await update.message.reply_text("No channels registered.")
        return
    lines = ["📊 *Channel Statistics*\n"]
    for ch in channels:
        cur = await db._conn.execute(
            """SELECT
                 COUNT(*) as total_subs,
                 COALESCE(SUM(paid_amount), 0) as total_rev,
                 COALESCE(AVG(paid_amount), 0) as avg_payment,
                 COUNT(CASE WHEN is_active=1 AND expires_at > datetime('now') THEN 1 END) as active_subs,
                 COUNT(CASE WHEN paid_at > datetime('now', '-7 days') THEN 1 END) as new_7d,
                 COUNT(CASE WHEN expires_at <= datetime('now', '+7 days')
                       AND expires_at > datetime('now') AND is_active=1 THEN 1 END) as expiring_7d
               FROM members WHERE channel_id=?""",
            (ch["id"],),
        )
        row = await cur.fetchone()
        txn_cur = await db._conn.execute(
            """SELECT bank, COUNT(*) as cnt FROM transactions
               WHERE channel_id=? AND status='verified' GROUP BY bank ORDER BY cnt DESC""",
            (ch["id"],),
        )
        bank_rows = await txn_cur.fetchall()
        bank_str = ", ".join(
            f"{BANK_LABELS.get(r['bank'], r['bank'])} ×{r['cnt']}" for r in bank_rows
        ) or "none"
        status = "✅ Active" if ch["is_active"] else "⏸️ Paused"
        lines.append(
            f"📡 *{ch['title']}* ({status})\n"
            f"   👥 Active: {row['active_subs']}  |  📈 All-time: {row['total_subs']}\n"
            f"   ⚠️ Expiring 7d: {row['expiring_7d']}  |  🆕 New 7d: {row['new_7d']}\n"
            f"   💰 Revenue: {row['total_rev']:,.2f} ETB  |  Avg: {row['avg_payment']:,.2f} ETB\n"
            f"   🏦 {bank_str}\n"
        )
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)


async def cmd_transactions(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Show recent transactions for admin's channels."""
    channels = await admin_channels(update.effective_user.id)
    if not channels:
        await update.message.reply_text("No channels registered.")
        return
    if len(channels) == 1:
        await update.message.reply_text(
            f"📜 Select view for *{channels[0]['title']}*:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📜 All Transactions", callback_data=f"ctxn:{channels[0]['id']}")],
            ]),
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await update.message.reply_text(
            "Select a channel to view transactions:",
            reply_markup=kb_channels(channels, "ctxn"),
            parse_mode=ParseMode.MARKDOWN,
        )


async def cmd_export(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    channels = await admin_channels(update.effective_user.id)
    if len(channels) == 0:
        await update.message.reply_text("No channels registered.")
        return
    if len(channels) == 1:
        await _export_channel(update, channels[0]["id"])
    else:
        await update.message.reply_text(
            "Select channel to export:",
            reply_markup=kb_channels(channels, "exppick"),
            parse_mode=ParseMode.MARKDOWN,
        )


async def cmd_regenerate_links(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Rebuild all join links (use after BOT_USERNAME changes)."""
    await sync_bot_username(ctx.bot)
    await ensure_join_links()
    channels = await admin_channels(update.effective_user.id)
    await update.message.reply_text(
        f"✅ *Join links updated!*\n\n"
        f"Bot: @{BOT_USERNAME}\n"
        f"Your channels: {len(channels)}\n\n"
        f"Open /my\\_channels to copy updated links.",
        parse_mode=ParseMode.MARKDOWN,
    )


async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    channels = await admin_channels(update.effective_user.id)
    if len(channels) == 0:
        await update.message.reply_text("No channels registered.")
        return
    if len(channels) == 1:
        await session_set(update.effective_user.id, S_BROADCAST_MSG, {"bc_channel_id": channels[0]["id"]})
        members = await get_channel_members(channels[0]["id"])
        await update.message.reply_text(
            f"📤 *Broadcast to {channels[0]['title']}*\n\n"
            f"Will be sent to {len(members)} active member(s).\n\n"
            "Type your message:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_cancel(),
        )
    else:
        await update.message.reply_text(
            "Select channel to broadcast to:",
            reply_markup=kb_channels(channels, "bcpick"),
            parse_mode=ParseMode.MARKDOWN,
        )


async def _export_channel(update: Update, channel_id: int):
    ch = await get_channel(channel_id)
    if not ch or not await is_channel_admin(channel_id, update.effective_user.id):
        await update.effective_message.reply_text("❌ Not authorized.")
        return
    members = await get_channel_members(channel_id)

    # Collect all unique raw API keys across verified transactions so we know the columns
    txn_map: dict[str, dict] = {}
    api_keys: list[str] = []
    seen_keys: set[str] = set()

    all_txns = await get_channel_transactions(channel_id, limit=5000, status="verified")
    for t in all_txns:
        txn_map[f"{t['user_id']}"] = t  # keep latest per user (already DESC)
        raw_str = t.get("raw_response", "")
        if raw_str:
            try:
                raw = json.loads(raw_str)
                d = raw.get("data", raw)
                for k in d.keys():
                    if k.lower() not in ("success",) and k not in seen_keys:
                        api_keys.append(k)
                        seen_keys.add(k)
            except (json.JSONDecodeError, TypeError):
                pass

    # Base columns always present
    base_cols = [
        "user_id", "username", "first_name",
        "expires_at", "days_remaining",
        "txn_id", "bank", "paid_amount",
        "txn_status", "verified_at", "txn_created_at",
        "receiver_account", "receiver_name",
    ]
    all_cols = base_cols + api_keys

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(all_cols)

    for m in members:
        d_left = max(0, days_until(m["expires_at"]))
        uid_str = str(m["user_id"])
        txn = txn_map.get(uid_str, {})

        # Parse raw API data for this member's last txn
        api_vals: dict[str, str] = {}
        raw_str = txn.get("raw_response", "")
        if raw_str:
            try:
                raw = json.loads(raw_str)
                d = raw.get("data", raw)
                for k in api_keys:
                    v = d.get(k, "")
                    api_vals[k] = "" if v is None else str(v)
            except (json.JSONDecodeError, TypeError):
                pass

        base_vals = [
            m["user_id"],
            m.get("username", ""),
            m.get("first_name", ""),
            m["expires_at"],
            d_left,
            m.get("txn_id", txn.get("txn_id", "")),
            txn.get("bank", ""),
            m.get("paid_amount") or txn.get("amount_verified", ""),
            txn.get("status", ""),
            txn.get("verified_at", ""),
            txn.get("created_at", ""),
            ch.get("account_number", ""),
            ch.get("account_name", ""),
        ]
        w.writerow(base_vals + [api_vals.get(k, "") for k in api_keys])

    buf.seek(0)
    filename = f"members_{ch['title'].replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    await update.effective_message.reply_document(
        document=buf.getvalue().encode("utf-8"),
        filename=filename,
        caption=(
            f"📥 *{ch['title']}* — {len(members)} active member(s)\n"
            f"Columns: base info + {len(api_keys)} API field(s)"
        ),
        parse_mode=ParseMode.MARKDOWN,
    )


async def _send_payment_instructions(message, ch: dict, first_name: str):
    """Show full payment details to a user about to join a channel."""
    bank_label = BANK_LABELS.get(ch["bank"], ch["bank"])
    cbe_note = (
        "\n\n_For CBE: after your Transaction ID you'll enter the last 8 digits "
        "of the account you paid from._"
    ) if ch["bank"] == BANK_CBE else (
        "\n\n_For Abyssinia: you'll also enter the last 5 digits of your account._"
    ) if ch["bank"] == BANK_ABYSSINIA else ""
    screenshot_note = (
        "\n📷 Or tap *Send Screenshot* below and upload your receipt."
        if ch["bank"] != BANK_IMAGE else ""
    )
    await message.reply_text(
        f"💳 *Join {ch['title']}*\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"💰 Price: *{ch['price']:,.0f} ETB* / {ch['billing_days']} days\n"
        f"🏦 Bank: {bank_label}\n"
        f"🔢 Account: `{ch['account_number']}`\n"
        f"👤 Name: *{ch['account_name']}*\n"
        f"━━━━━━━━━━━━━━━━━━\n\n"
        f"⚠️ *IMPORTANT:* Your payment must be made within the last 24 hours to be verified.\n\n"
        f"1️⃣ Transfer *{ch['price']:,.0f} ETB* to the account above\n"
        f"2️⃣ Paste your *Transaction/Reference ID* here"
        f"{cbe_note}{screenshot_note}\n\n"
        f"_Access is granted instantly after verification._",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_join_payment(ch["id"]),
    )


# ── Join request handler ──────────────────────────────────────────────────────

async def on_join_request(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    req = update.chat_join_request
    user, cid = req.from_user, req.chat.id
    ch = await get_channel(cid)
    if not ch or not ch["is_active"]:
        return

    _pending_join[str(user.id)] = str(cid)
    await session_set(user.id, S_JOIN_TXN, {"join_channel_id": cid})

    try:
        await ctx.bot.send_message(
            user.id,
            f"👋 *Hi {user.first_name}!*\n\n"
            f"You requested to join *{ch['title']}*.\n\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💰 Price: *{ch['price']:,.0f} ETB* / {ch['billing_days']} days\n"
            f"🏦 {BANK_LABELS.get(ch['bank'], ch['bank'])}\n"
            f"🔢 Account: `{ch['account_number']}`\n"
            f"👤 {ch['account_name']}\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"⚠️ *IMPORTANT:* Your payment must be made within the last 24 hours to be verified.\n\n"
            f"Paste your *Transaction ID* after paying.\n\n"
            f"_For CBE you'll also enter the last 8 digits of your paying account._",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_join_payment(cid),
        )
    except TelegramError as e:
        logger.warning("Cannot DM user %s: %s", user.id, e)
        try:
            await ctx.bot.decline_chat_join_request(cid, user.id)
        except TelegramError:
            pass
        _pending_join.pop(str(user.id), None)
        await session_clear(user.id)


# ── Callback handler ──────────────────────────────────────────────────────────

async def on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data, uid = q.data, q.from_user.id
    state, sdata = await session_get(uid)

    # ── Global actions ──
    if data == "act:cancel":
        await session_clear(uid)
        await q.edit_message_text("✅ Cancelled. Use /start to return to the menu.")
        return
    if data == "act:done":
        await session_clear(uid)
        await q.edit_message_text("✅ Done!")
        return
    if data == "act:again":
        await session_set(uid, S_CHOOSE)
        await q.edit_message_text(
            "🔍 *Standalone Payment Verification*\n\nSelect your payment method:",
            reply_markup=kb_banks(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    if data == "act:channels":
        channels = await admin_channels(uid)
        await q.edit_message_text(
            f"📡 *Your Channels* ({len(channels)})\n\nTap to manage:",
            reply_markup=kb_admin_channels(channels),
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    if data == "act:setup":
        await session_set(uid, S_SETUP_ID)
        await q.edit_message_text(
            "📡 *Channel Setup Wizard*\n\n*Step 1 of 5 — Channel ID*\n\nEnter channel ID or @username:",
            reply_markup=kb_cancel(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    if data == "act:mysubs":
        subs = await get_user_subscriptions(uid)
        if not subs:
            await q.edit_message_text(
                "📭 No active subscriptions.\n\nUse /join to browse channels.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("💼 Browse Channels", callback_data="act:browsechannels"),
                ]]),
            )
        else:
            lines = [f"📋 *Your Subscriptions* ({len(subs)})\n"]
            for s in subs:
                d = days_until(s["expires_at"])
                icon = "🔴" if d <= 1 else "🟡" if d <= 3 else "🟢"
                lines.append(f"{icon} *{s['title']}* — {fmt_expiry(s['expires_at'])}")
            await q.edit_message_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)
        return
    if data == "act:browsechannels":
        cur = await db._conn.execute("SELECT * FROM channels WHERE is_active=1 ORDER BY title")
        channels = [dict(r) for r in await cur.fetchall()]
        if not channels:
            await q.edit_message_text("📭 No active channels right now.")
            return
        await session_set(uid, S_JOIN_CHOOSE)
        await q.edit_message_text(
            f"💼 *Available Channels* ({len(channels)})\n\nTap to join:",
            reply_markup=kb_channels(channels),
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    if data == "act:verify":
        await session_set(uid, S_CHOOSE)
        await q.edit_message_text(
            "🔍 *Standalone Payment Verification*\n\nSelect your payment method:",
            reply_markup=kb_banks(),
            parse_mode=ParseMode.MARKDOWN,
        )
        return
    if data == "act:adminpanel":
        channels = await admin_channels(uid)
        if not channels:
            await q.edit_message_text(
                "⚙️ *Admin Panel*\n\nYou haven't registered any channels yet.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Register Channel", callback_data="act:setup")],
                    [InlineKeyboardButton("◀️ Back", callback_data="act:cancel")],
                ]),
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await q.edit_message_text(
                f"⚙️ *Admin Panel*\n\n{len(channels)} channel(s) registered:",
                reply_markup=kb_admin_channels(channels),
                parse_mode=ParseMode.MARKDOWN,
            )
        return
    if data == "act:help":
        await q.edit_message_text(
            "📖 *Help*\n\n"
            "*/join* — Browse paid channels\n"
            "*/my\\_subscriptions* — Your subscriptions\n"
            "*/verify* — Verify a payment\n"
            "*/support* — Troubleshooting\n"
            "*/setup\\_channel* — Register a channel (admins)\n"
            "*/my\\_channels* — Manage your channels\n\n"
            "*Banks supported:*\n"
            "CBE · Telebirr · Dashen · Abyssinia · CBE Birr · Screenshot",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Back to Menu", callback_data="act:cancel"),
            ]]),
        )
        return

    # ── Bank selection (standalone verify) ──
    if data.startswith("bank:"):
        bank = data.split(":")[1]
        if bank not in VALID_BANKS:
            return
        if bank == BANK_IMAGE:
            await session_set(uid, S_IMAGE, {"bank": bank})
            await q.edit_message_text(
                "🖼️ *Screenshot Verification*\n\nSend a clear screenshot of your payment receipt.",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=kb_cancel(),
            )
        else:
            await session_set(uid, S_REF, {"bank": bank})
            prompts = {
                BANK_CBE: "🏦 Enter your CBE *reference number*:",
                BANK_TELEBIRR: "📱 Enter your Telebirr *reference number*:",
                BANK_DASHEN: "🏦 Enter your Dashen *reference number*:",
                BANK_ABYSSINIA: "🏦 Enter your Abyssinia *reference number*:",
                BANK_CBEBIRR: "📱 Enter your CBE Birr *receipt number*:",
            }
            await q.edit_message_text(
                prompts.get(bank, "Enter reference:"),
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=kb_cancel(),
            )
        return

    # ── Bank selection (setup wizard) ──
    if data.startswith("sbank:"):
        bank = data.split(":")[1]
        if bank not in VALID_BANKS:
            return
        state, sdata = await session_get(uid)
        if state == S_EDIT_BANK:
            cid = sdata.get("edit_channel_id")
            if cid and await is_channel_admin(cid, uid):
                await db._conn.execute(
                    "UPDATE channels SET bank=?, updated_at=datetime('now') WHERE id=?", (bank, cid),
                )
                await db._conn.commit()
                await session_clear(uid)
                await q.edit_message_text(f"✅ Bank updated to {BANK_LABELS.get(bank, bank)}.")
            return
        await session_update_data(uid, setup_bank=bank)
        await session_set(uid, S_SETUP_ACCOUNT)
        await q.edit_message_text(
            f"📡 *Channel Setup Wizard*\n\n"
            f"*Step 4 of 5 — Account Number*\n\n"
            f"Enter your *{BANK_LABELS.get(bank, bank)}* account number that users will pay to:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_cancel(),
        )
        return

    # ── Join channel (from browse list) ──
    if data.startswith("join:"):
        cid = int(data.split(":")[1])
        ch = await get_channel(cid)
        if not ch or not ch["is_active"]:
            await q.answer("Channel is no longer available.", show_alert=True)
            return
        existing = await get_active_member(cid, uid)
        if existing:
            d = days_until(existing["expires_at"])
            await q.answer(
                f"You're already subscribed! Expires in {d} days.", show_alert=True
            )
            return
        await session_set(uid, S_JOIN_TXN, {"join_channel_id": cid})
        await q.edit_message_text(
            f"💳 *Join {ch['title']}*\n\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💰 *{ch['price']:,.0f} ETB* / {ch['billing_days']} days\n"
            f"🏦 {BANK_LABELS.get(ch['bank'], ch['bank'])}\n"
            f"🔢 `{ch['account_number']}`\n"
            f"👤 {ch['account_name']}\n"
            f"━━━━━━━━━━━━━━━━━━\n\n"
            f"Paste your *Transaction ID* after paying.\n\n"
            f"_For CBE you'll also enter the last 8 digits of your paying account._",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_join_payment(cid),
        )
        return

    # ── Switch to screenshot verification during join ──
    if data.startswith("joinimg:"):
        cid = int(data.split(":")[1])
        ch = await get_channel(cid)
        if not ch or not ch["is_active"]:
            await q.answer("Channel unavailable.", show_alert=True)
            return
        await session_set(uid, S_JOIN_IMAGE, {"join_channel_id": cid})
        await q.edit_message_text(
            f"📷 *Screenshot Verification — {ch['title']}*\n\n"
            f"Send a clear screenshot of your payment receipt.\n\n"
            f"*Make sure it shows:*\n"
            f"• Amount: *{ch['price']:,.0f} ETB*\n"
            f"• Transaction ID / reference\n"
            f"• Date and time",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_cancel(),
        )
        return

    # ── Export / broadcast / admin picks ──
    if data.startswith("exppick:"):
        cid = int(data.split(":")[1])
        if not await is_channel_admin(cid, uid):
            await q.answer("Not authorized.", show_alert=True)
            return
        await _export_channel(update, cid)
        return

    if data.startswith("bcpick:"):
        cid = int(data.split(":")[1])
        if not await is_channel_admin(cid, uid):
            await q.answer("Not authorized.", show_alert=True)
            return
        ch = await get_channel(cid)
        members = await get_channel_members(cid)
        await session_set(uid, S_BROADCAST_MSG, {"bc_channel_id": cid})
        await q.edit_message_text(
            f"📤 *Broadcast to {ch['title']}*\n\n"
            f"Will reach {len(members)} active member(s).\n\n"
            "Type your message:",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_cancel(),
        )
        return

    if data.startswith("editpick:"):
        cid = int(data.split(":")[1])
        ch = await get_channel(cid)
        if not ch or not await is_channel_admin(cid, uid):
            return
        await session_set(uid, S_EDIT_CHOOSE, {"edit_channel_id": cid})
        await q.edit_message_text(
            f"✏️ *Editing: {ch['title']}*\n\nWhat would you like to change?",
            reply_markup=kb_edit_fields(cid),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if data.startswith("adm:"):
        cid = int(data.split(":")[1])
        if not await is_channel_admin(cid, uid):
            await q.answer("Not authorized.", show_alert=True)
            return
        ch = await get_channel(cid)
        if not ch:
            await q.answer("Channel not found.", show_alert=True)
            return
        members = await get_channel_members(cid)
        link = build_deep_link(cid)
        status_icon = "✅ Active" if ch["is_active"] else "⏸️ Paused"
        await q.edit_message_text(
            f"📡 *{ch['title']}*\n\n"
            f"💰 {ch['price']:,.0f} ETB / {ch['billing_days']} days\n"
            f"🏦 {BANK_LABELS.get(ch['bank'], ch['bank'])}\n"
            f"🔢 `{ch['account_number']}`\n"
            f"👤 {ch['account_name']}\n"
            f"👥 {len(members)} active member(s)\n"
            f"📶 {status_icon}\n\n"
            f"🔗 Join link:\n`{link}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_channel_detail(cid, bool(ch["is_active"])),
            disable_web_page_preview=True,
        )
        return

    if data.startswith("tog:"):
        cid = int(data.split(":")[1])
        if not await is_channel_admin(cid, uid):
            return
        ch = await get_channel(cid)
        new_active = 0 if ch["is_active"] else 1
        await db._conn.execute(
            "UPDATE channels SET is_active=?, updated_at=datetime('now') WHERE id=?", (new_active, cid),
        )
        await db._conn.commit()
        ch = await get_channel(cid)
        members = await get_channel_members(cid)
        link = build_deep_link(cid)
        action = "✅ Activated" if new_active else "⏸️ Paused"
        await q.edit_message_text(
            f"📡 *{ch['title']}*\n\n"
            f"💰 {ch['price']:,.0f} ETB / {ch['billing_days']} days\n"
            f"👥 {len(members)} active member(s)\n"
            f"📶 {action}\n\n"
            f"🔗 `{link}`",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_channel_detail(cid, bool(ch["is_active"])),
            disable_web_page_preview=True,
        )
        return

    if data.startswith("mem:"):
        cid = int(data.split(":")[1])
        if not await is_channel_admin(cid, uid):
            return
        ch = await get_channel(cid)
        members = await get_channel_members(cid)
        if not members:
            await q.answer("No active members yet.", show_alert=True)
            return
        lines = [f"👥 *{ch['title']}* — {len(members)} member(s)\n"]
        rows_kb = []
        for m in members[:20]:
            uname = f"@{m['username']}" if m.get("username") else f"ID:{m['user_id']}"
            d = days_until(m["expires_at"])
            icon = "🔴" if d <= 1 else "🟡" if d <= 3 else "🟢"
            amt_str = f" | 💰 {m['paid_amount']:,.0f} ETB" if m.get("paid_amount") else ""
            lines.append(
                f"{icon} *{m['first_name']}* ({uname})\n"
                f"   ⏳ {fmt_expiry(m['expires_at'])}{amt_str}\n"
                f"   🔑 `{m['txn_id']}`"
            )
            rows_kb.append([InlineKeyboardButton(
                f"📋 {m['first_name']} — txn history",
                callback_data=f"mtxn:{m['user_id']}:{cid}",
            )])
        if len(members) > 20:
            lines.append(f"\n_…{len(members) - 20} more. Export CSV for full list._")
        rows_kb.append([
            InlineKeyboardButton("📥 Export CSV", callback_data=f"exp:{cid}"),
            InlineKeyboardButton("◀️ Back", callback_data=f"adm:{cid}"),
        ])
        await q.edit_message_text(
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(rows_kb),
        )
        return

    # ── Transaction list for a channel ──
    if data.startswith("ctxn:"):
        cid = int(data.split(":")[1])
        if not await is_channel_admin(cid, uid):
            await q.answer("Not authorized.", show_alert=True)
            return
        ch = await get_channel(cid)
        txns = await get_channel_transactions(cid, limit=30)
        if not txns:
            await q.answer("No transactions recorded yet.", show_alert=True)
            return
        lines = [f"📜 *{ch['title']}* — Last {len(txns)} Transactions\n"]
        rows_kb = []
        for t in txns:
            se = _TXN_STATUS_EMOJI.get(t["status"], "❓")
            amt = f"{t['amount_verified']:,.0f} ETB" if t.get("amount_verified") else "N/A"
            bl = BANK_LABELS.get(t["bank"], t["bank"])
            date = (t.get("created_at") or "")[:10]
            txn_short = (t.get("txn_id") or "?")[:18]
            lines.append(f"{se} `{txn_short}` — {amt} — {bl} — {date}")
            rows_kb.append([InlineKeyboardButton(
                f"{se} {txn_short} — {amt}",
                callback_data=f"txn:{t['id']}:{cid}",
            )])
        rows_kb.append([InlineKeyboardButton("◀️ Back", callback_data=f"adm:{cid}")])
        await q.edit_message_text(
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(rows_kb),
        )
        return

    # ── Transaction list for a specific member ──
    if data.startswith("mtxn:"):
        parts = data.split(":")
        target_uid, cid = int(parts[1]), int(parts[2])
        if not await is_channel_admin(cid, uid):
            await q.answer("Not authorized.", show_alert=True)
            return
        ch = await get_channel(cid)
        txns = await get_channel_transactions_for_user(target_uid, cid, limit=10)
        if not txns:
            await q.answer("No transactions found for this member.", show_alert=True)
            return
        lines = [f"📋 *Transactions — User {target_uid}*\n"]
        rows_kb = []
        for t in txns:
            se = _TXN_STATUS_EMOJI.get(t["status"], "❓")
            amt = f"{t['amount_verified']:,.0f} ETB" if t.get("amount_verified") else "N/A"
            bl = BANK_LABELS.get(t["bank"], t["bank"])
            date = (t.get("created_at") or "")[:10]
            txn_short = (t.get("txn_id") or "?")[:18]
            lines.append(f"{se} `{txn_short}` — {amt} — {bl} — {date}")
            rows_kb.append([InlineKeyboardButton(
                f"{se} {txn_short} — {amt}",
                callback_data=f"txn:{t['id']}:{cid}",
            )])
        rows_kb.append([InlineKeyboardButton("◀️ Back", callback_data=f"mem:{cid}")])
        await q.edit_message_text(
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(rows_kb),
        )
        return

    # ── Full transaction detail (all API fields from stored raw_response) ──
    if data.startswith("txn:"):
        parts = data.split(":")
        db_id, back_cid = int(parts[1]), int(parts[2])
        if not await is_channel_admin(back_cid, uid):
            await q.answer("Not authorized.", show_alert=True)
            return
        txn = await get_transaction_by_db_id(db_id)
        ch = await get_channel(back_cid)
        if not txn:
            await q.answer("Transaction not found.", show_alert=True)
            return
        text = fmt_transaction_detail(txn, channel=ch)
        await q.edit_message_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("◀️ Back to Transactions", callback_data=f"ctxn:{back_cid}"),
            ]]),
        )
        return

    if data.startswith("del:"):
        cid = int(data.split(":")[1])
        if not await is_channel_admin(cid, uid):
            return
        ch = await get_channel(cid)
        members = await get_channel_members(cid)
        await q.edit_message_text(
            f"⚠️ *Delete {ch['title']}?*\n\n"
            f"This will remove the channel registration and all {len(members)} subscriber records.\n"
            f"*This cannot be undone.*\n\n"
            "Are you sure?",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_confirm_delete(cid),
        )
        return

    if data.startswith("delconfirm:"):
        cid = int(data.split(":")[1])
        if not await is_channel_admin(cid, uid):
            return
        ch = await get_channel(cid)
        title = ch["title"] if ch else str(cid)
        await db._conn.execute("DELETE FROM channels WHERE id=?", (cid,))
        await db._conn.commit()
        await session_clear(uid)
        await q.edit_message_text(f"🗑️ Channel *{title}* deleted.", parse_mode=ParseMode.MARKDOWN)
        return

    if data.startswith("stat:"):
        cid = int(data.split(":")[1])
        if not await is_channel_admin(cid, uid):
            return
        ch = await get_channel(cid)
        cur = await db._conn.execute(
            """SELECT
                 COUNT(*) as total,
                 COALESCE(SUM(paid_amount), 0) as revenue,
                 COALESCE(AVG(paid_amount), 0) as avg_payment,
                 COALESCE(MIN(paid_amount), 0) as min_payment,
                 COALESCE(MAX(paid_amount), 0) as max_payment,
                 COUNT(CASE WHEN is_active=1 AND expires_at > datetime('now') THEN 1 END) as active,
                 COUNT(CASE WHEN expires_at <= datetime('now', '+7 days')
                       AND expires_at > datetime('now') AND is_active=1 THEN 1 END) as expiring_7d,
                 COUNT(CASE WHEN expires_at > datetime('now', '-30 days') AND is_active=0 THEN 1 END) as recent_expired,
                 COUNT(CASE WHEN paid_at > datetime('now', '-7 days') THEN 1 END) as new_7d,
                 COUNT(CASE WHEN paid_at > datetime('now', '-30 days') THEN 1 END) as new_30d
               FROM members WHERE channel_id=?""",
            (cid,),
        )
        row = await cur.fetchone()
        # Transaction status breakdown
        txn_cur = await db._conn.execute(
            """SELECT status, COUNT(*) as cnt, COALESCE(SUM(amount_verified), 0) as total
               FROM transactions WHERE channel_id=? GROUP BY status ORDER BY cnt DESC""",
            (cid,),
        )
        txn_rows = await txn_cur.fetchall()
        # Bank breakdown (verified only)
        bank_cur = await db._conn.execute(
            """SELECT bank, COUNT(*) as cnt, COALESCE(SUM(amount_verified), 0) as total
               FROM transactions WHERE channel_id=? AND status='verified'
               GROUP BY bank ORDER BY cnt DESC""",
            (cid,),
        )
        bank_rows = await bank_cur.fetchall()
        lines = [
            f"📊 *Stats: {ch['title']}*",
            "━━━━━━━━━━━━━━━━━",
            "👥 *Subscribers*",
            f"  🟢 Active: {row['active']}  |  📈 All-time: {row['total']}",
            f"  ⚠️ Expiring in 7d: {row['expiring_7d']}",
            f"  📉 Expired (last 30d): {row['recent_expired']}",
            f"  🆕 New this week: {row['new_7d']}  |  This month: {row['new_30d']}",
            "━━━━━━━━━━━━━━━━━",
            "💰 *Revenue*",
            f"  💵 Total: {row['revenue']:,.2f} ETB",
            f"  📊 Avg: {row['avg_payment']:,.2f} ETB",
            f"  ⬇️ Min: {row['min_payment']:,.2f} ETB  |  ⬆️ Max: {row['max_payment']:,.2f} ETB",
            f"  💵 Price/period: {ch['price']:,.0f} ETB / {ch['billing_days']}d",
        ]
        if txn_rows:
            lines.append("━━━━━━━━━━━━━━━━━")
            lines.append("📋 *Transaction Breakdown*")
            for tr in txn_rows:
                se = _TXN_STATUS_EMOJI.get(tr["status"], "❓")
                label = tr["status"].replace("_", " ").title()
                amt = f" ({tr['total']:,.0f} ETB)" if tr["total"] else ""
                lines.append(f"  {se} {label}: {tr['cnt']}{amt}")
        if bank_rows:
            lines.append("━━━━━━━━━━━━━━━━━")
            lines.append("🏦 *By Bank (verified)*")
            for br in bank_rows:
                bl = BANK_LABELS.get(br["bank"], br["bank"])
                lines.append(f"  {bl}: {br['cnt']} txns — {br['total']:,.0f} ETB")
        await q.edit_message_text(
            "\n".join(lines),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📜 View Transactions", callback_data=f"ctxn:{cid}"),
                 InlineKeyboardButton("📥 Export CSV", callback_data=f"exp:{cid}")],
                [InlineKeyboardButton("◀️ Back", callback_data=f"adm:{cid}")],
            ]),
        )
        return

    if data.startswith("exp:"):
        cid = int(data.split(":")[1])
        if not await is_channel_admin(cid, uid):
            return
        await _export_channel(update, cid)
        return

    if data.startswith("bc:"):
        cid = int(data.split(":")[1])
        if not await is_channel_admin(cid, uid):
            return
        ch = await get_channel(cid)
        members = await get_channel_members(cid)
        await session_set(uid, S_BROADCAST_MSG, {"bc_channel_id": cid})
        await q.edit_message_text(
            f"📤 *Broadcast to {ch['title']}*\n\n"
            f"Will reach {len(members)} active member(s).\n\n"
            "Type your message:",
            reply_markup=kb_cancel(),
        )
        return

    if data.startswith("edit:"):
        cid = int(data.split(":")[1])
        if not await is_channel_admin(cid, uid):
            return
        await session_set(uid, S_EDIT_CHOOSE, {"edit_channel_id": cid})
        ch = await get_channel(cid)
        await q.edit_message_text(
            f"✏️ *Editing: {ch['title']}*\n\nWhat would you like to change?",
            reply_markup=kb_edit_fields(cid),
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    if data.startswith("efld:"):
        parts = data.split(":")
        cid, field = int(parts[1]), parts[2]
        if not await is_channel_admin(cid, uid):
            return
        field_states = {
            "price": S_EDIT_PRICE, "days": S_EDIT_DAYS, "bank": S_EDIT_BANK,
            "account": S_EDIT_ACCOUNT, "accname": S_EDIT_ACCNAME, "addadmin": S_ADD_ADMIN,
        }
        if field not in field_states:
            return
        await session_set(uid, field_states[field], {"edit_channel_id": cid, "edit_field": field})
        if field == "bank":
            await q.edit_message_text("Select new bank:", reply_markup=kb_setup_banks())
        elif field == "addadmin":
            await q.edit_message_text(
                "Enter the Telegram *User ID* of the new co-admin:\n\n"
                "_They can use /my\\_channels to manage this channel._",
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=kb_cancel(),
            )
        else:
            prompts = {
                "price": "Enter new price in ETB (e.g. `150`):",
                "days": "Enter new billing period in days (e.g. `30`):",
                "account": "Enter new account number:",
                "accname": "Enter new account holder name:",
            }
            await q.edit_message_text(
                prompts.get(field, "Enter new value:"),
                parse_mode=ParseMode.MARKDOWN,
                reply_markup=kb_cancel(),
            )
        return

    if data.startswith("bcconfirm:"):
        if state != S_BROADCAST_CONFIRM:
            return
        cid = sdata.get("bc_channel_id")
        if not await is_channel_admin(cid, uid):
            return
        members = await get_channel_members(cid)
        ch = await get_channel(cid)
        text = sdata.get("bc_text", "")
        sent = failed = 0
        await q.edit_message_text(f"📤 Sending to {len(members)} member(s)…")
        for m in members:
            try:
                await ctx.bot.send_message(
                    m["user_id"],
                    f"📢 *Announcement from {ch['title']}*\n\n{text}",
                    parse_mode=ParseMode.MARKDOWN,
                )
                sent += 1
                await asyncio.sleep(0.05)
            except TelegramError:
                failed += 1
        await session_clear(uid)
        await q.edit_message_text(
            f"✅ Broadcast complete!\n\n"
            f"📨 Delivered: {sent}\n"
            f"❌ Failed: {failed} (users who blocked the bot)",
        )
        return

    if data == "bccancel":
        await session_clear(uid)
        await q.edit_message_text("Broadcast cancelled.")
        return


# ── Message handler ───────────────────────────────────────────────────────────

async def on_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg:
        return
    uid = update.effective_user.id
    state, sdata = await session_get(uid)

    # Handle images
    is_image = (
        msg.photo or
        (msg.document and msg.document.mime_type and msg.document.mime_type.startswith("image/"))
    )
    if is_image:
        if state in (S_IMAGE, S_JOIN_IMAGE):
            await _handle_image(update, ctx, state, sdata)
        else:
            await msg.reply_text(
                "💡 Send /verify to verify a payment, or /join to browse channels."
            )
        return

    if not msg.text:
        return

    text = msg.text.strip()
    ok, wait = await check_rate_limit(uid, "general_commands")
    if not ok:
        await msg.reply_text(f"⏳ Slow down — too many messages. Wait {wait}s.")
        return

    # Route by state
    handlers = {
        S_SETUP_ID: _setup_id,
        S_SETUP_PRICE: _setup_price,
        S_SETUP_DAYS: _setup_days,
        S_SETUP_ACCOUNT: _setup_account,
        S_SETUP_ACCNAME: _setup_finish,
        S_JOIN_TXN: lambda m, u, t, s: _join_txn(m, u, t, s, ctx),
        S_JOIN_SUFFIX: lambda m, u, t, s: _join_suffix(m, u, t, s, ctx),
        S_JOIN_PHONE: lambda m, u, t, s: _join_phone(m, u, t, s, ctx),
        S_JOIN_IMAGE: lambda m, u, t, s: _join_image_text(m, u, t, s, ctx),
        S_IMAGE: _verify_image_text,
        S_REF: _verify_ref,
        S_SUFFIX: _verify_suffix,
        S_PHONE: _verify_phone,
        S_EDIT_PRICE: lambda m, u, t, s: _edit_field(m, u, t, s, "price", float),
        S_EDIT_DAYS: lambda m, u, t, s: _edit_field(m, u, t, s, "billing_days", int),
        S_EDIT_ACCOUNT: lambda m, u, t, s: _edit_field(m, u, t, s, "account_number", str),
        S_EDIT_ACCNAME: lambda m, u, t, s: _edit_field(m, u, t, s, "account_name", str),
        S_ADD_ADMIN: _add_admin,
    }

    handler = handlers.get(state)
    if handler:
        if state in (S_SETUP_ACCNAME,):
            await handler(msg, uid, text, ctx)
        else:
            await handler(msg, uid, text, sdata)
    elif state == S_BROADCAST_MSG:
        await session_set(uid, S_BROADCAST_CONFIRM, {**sdata, "bc_text": text})
        ch = await get_channel(sdata.get("bc_channel_id"))
        members = await get_channel_members(sdata.get("bc_channel_id", 0))
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"✅ Send to {len(members)} member(s)", callback_data="bcconfirm:1")],
            [InlineKeyboardButton("❌ Cancel", callback_data="bccancel")],
        ])
        await msg.reply_text(
            f"📤 *Preview broadcast to {ch['title'] if ch else 'channel'}:*\n\n{text}\n\n"
            f"*Confirm sending to {len(members)} member(s)?*",
            reply_markup=kb,
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💼 Browse Channels", callback_data="act:browsechannels"),
             InlineKeyboardButton("🔍 Verify", callback_data="act:verify")],
        ])
        await msg.reply_text(
            "Use the buttons below or a command like /help, /join, or /verify.",
            reply_markup=kb,
        )


# ── Setup wizard steps ────────────────────────────────────────────────────────

async def _setup_id(msg, uid, text, sdata):
    raw = text.lstrip("@").strip()
    if not raw.lstrip("-").isdigit():
        try:
            chat = await msg.get_bot().get_chat(raw if raw.startswith("-") else f"@{raw}")
            raw = str(chat.id)
        except TelegramError:
            await msg.reply_text(
                "❌ Channel not found.\n\nMake sure:\n"
                "• You've added me as admin in the channel\n"
                "• You entered the correct ID or @username",
                reply_markup=kb_cancel(),
            )
            return
    else:
        raw = str(raw)

    cid = int(raw)
    ok, err = await bot_has_channel_perms(msg.get_bot(), cid)
    if not ok:
        await msg.reply_text(
            f"❌ *Permission Check Failed*\n\n{err}",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_cancel(),
        )
        return

    # Check channel not already registered
    existing = await get_channel(cid)
    if existing:
        if await is_channel_admin(cid, uid):
            await msg.reply_text(
                f"⚠️ *{existing['title']}* is already registered.\n\n"
                f"Use /my\\_channels to manage it.",
                parse_mode=ParseMode.MARKDOWN,
            )
        else:
            await msg.reply_text("❌ This channel is already registered by another admin.")
        await session_clear(uid)
        return

    chat = await msg.get_bot().get_chat(cid)
    await session_set(uid, S_SETUP_PRICE, {
        "setup_channel_id": cid,
        "setup_channel_title": (chat.title or str(cid))[:255],
    })
    await msg.reply_text(
        f"✅ *Found: {chat.title}*\n\n"
        f"📡 *Channel Setup Wizard*\n\n"
        f"*Step 2 of 5 — Subscription Price*\n\n"
        f"Enter the price in ETB (e.g. `150`):",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_cancel(),
    )


async def _setup_price(msg, uid, text, sdata):
    try:
        price = float(text.replace(",", "").strip())
        if not (0 < price <= 1_000_000):
            raise ValueError
    except (ValueError, AttributeError):
        await msg.reply_text("❌ Enter a valid price between 1 and 1,000,000 ETB.", reply_markup=kb_cancel())
        return
    await session_update_data(uid, setup_price=price)
    await session_set(uid, S_SETUP_DAYS)
    await msg.reply_text(
        "📡 *Channel Setup Wizard*\n\n"
        "*Step 3 of 5 — Billing Period*\n\n"
        "How many days per subscription?\n"
        "• 7 = weekly\n• 30 = monthly\n• 365 = yearly",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_cancel(),
    )


async def _setup_days(msg, uid, text, sdata):
    try:
        days = int(text.strip())
        if not (0 < days <= 3650):
            raise ValueError
    except (ValueError, AttributeError):
        await msg.reply_text("❌ Enter a valid number of days (1–3650).", reply_markup=kb_cancel())
        return
    await session_update_data(uid, setup_days=days)
    await session_set(uid, S_SETUP_BANK)
    await msg.reply_text(
        "📡 *Channel Setup Wizard*\n\n"
        "*Step 4 of 5 — Payment Bank*\n\n"
        "Select the bank subscribers will pay to:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_setup_banks(),
    )


async def _setup_account(msg, uid, text, sdata):
    cleaned = text.replace(" ", "").strip()
    if not re.match(r"^\d{8,20}$", cleaned):
        await msg.reply_text(
            "❌ Invalid account number. Must be 8–20 digits.",
            reply_markup=kb_cancel(),
        )
        return
    await session_update_data(uid, setup_account=cleaned)
    await session_set(uid, S_SETUP_ACCNAME)
    await msg.reply_text(
        "📡 *Channel Setup Wizard*\n\n"
        "*Step 5 of 5 — Account Holder Name*\n\n"
        "Enter the account holder name exactly as shown in the bank app:",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_cancel(),
    )


async def _setup_finish(msg, uid, text, ctx):
    _, sdata = await session_get(uid)
    cid = sdata.get("setup_channel_id")
    if not cid:
        await msg.reply_text("❌ Session expired. Please start /setup\\_channel again.", parse_mode=ParseMode.MARKDOWN)
        await session_clear(uid)
        return

    await sync_bot_username(msg.get_bot())
    join_link = build_deep_link(cid)
    if not join_link:
        await msg.reply_text(
            "❌ Bot has no @username set.\n\n"
            "Go to @BotFather → Your bot → Edit Bot → Edit Username → set a username, then retry.",
            parse_mode=ParseMode.MARKDOWN,
        )
        return

    account_name = text.strip()[:255]
    if len(account_name) < 2:
        await msg.reply_text("❌ Account name too short.", reply_markup=kb_cancel())
        return

    await db._conn.execute(
        """INSERT INTO channels
           (id, title, admin_id, price, billing_days, bank, account_number, account_name, join_link, is_active)
           VALUES (?,?,?,?,?,?,?,?,?,1)
           ON CONFLICT(id) DO UPDATE SET
           title=excluded.title, price=excluded.price, billing_days=excluded.billing_days,
           bank=excluded.bank, account_number=excluded.account_number,
           account_name=excluded.account_name, join_link=excluded.join_link,
           updated_at=datetime('now')""",
        (
            cid, sdata["setup_channel_title"], uid,
            sdata["setup_price"], sdata["setup_days"],
            sdata["setup_bank"], sdata["setup_account"],
            account_name, join_link,
        ),
    )
    await db._conn.execute(
        "INSERT OR IGNORE INTO channel_admins (channel_id, user_id, role) VALUES (?,?,?)",
        (cid, uid, "owner"),
    )
    await db._conn.commit()
    await session_clear(uid)

    bank_label = BANK_LABELS.get(sdata["setup_bank"], sdata["setup_bank"])
    await msg.reply_text(
        f"🎉 *Channel Registered Successfully!*\n\n"
        f"📡 *{sdata['setup_channel_title']}*\n"
        f"💰 {sdata['setup_price']:,.0f} ETB / {sdata['setup_days']} days\n"
        f"🏦 {bank_label}\n"
        f"🔢 {sdata['setup_account']}\n"
        f"👤 {account_name}\n\n"
        f"🔗 *Share this link with users:*\n`{join_link}`\n\n"
        f"_Users tap the link → pay → get instant access._\n\n"
        f"Use /my\\_channels to manage.",
        parse_mode=ParseMode.MARKDOWN,
        disable_web_page_preview=True,
    )


# ── Join payment flow ─────────────────────────────────────────────────────────

async def _join_txn(msg, uid, text, sdata, ctx):
    cid = sdata.get("join_channel_id")
    ch = await get_channel(cid) if cid else None
    if not ch:
        await msg.reply_text("❌ Session error. Tap the channel join link again.")
        await session_clear(uid)
        return
    if not ch["is_active"]:
        await msg.reply_text("⏸️ This channel's subscriptions are currently paused.")
        await session_clear(uid)
        return

    if ch["bank"] == BANK_IMAGE:
        await session_set(uid, S_JOIN_IMAGE, sdata)
        await msg.reply_text("📷 Send a *clear screenshot* of your payment receipt.", parse_mode=ParseMode.MARKDOWN, reply_markup=kb_cancel())
        return

    if not re.match(r"^[A-Za-z0-9/_\-]{4,60}$", text):
        await msg.reply_text(
            "❌ Invalid transaction ID format.\n\n"
            "_Transaction IDs are typically alphanumeric, 4–60 characters._",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_cancel(),
        )
        return

    bank = ch["bank"]

    if bank in (BANK_CBE, BANK_ABYSSINIA) and not sdata.get("join_suffix"):
        await session_set(uid, S_JOIN_SUFFIX, {**sdata, "join_pending_txn": text})
        digits = "8" if bank == BANK_CBE else "5"
        bank_name = "CBE" if bank == BANK_CBE else "Abyssinia"
        await msg.reply_text(
            f"Enter the last *{digits} digits* of your {bank_name} account "
            f"*(the account you paid from)*:\n\n"
            "_You can find this on your transfer receipt or SMS._",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_join_payment(ch["id"]),
        )
        return

    if bank == BANK_CBEBIRR and not sdata.get("join_phone"):
        await session_set(uid, S_JOIN_PHONE, {**sdata, "join_pending_txn": text})
        await msg.reply_text(
            "📱 Enter your phone number (format: `251912345678`):",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_cancel(),
        )
        return

    await _process_join_payment(
        msg, uid, ch,
        {
            "reference": text,
            "suffix": sdata.get("join_suffix", ""),
            "phone": sdata.get("join_phone", ""),
        },
        ctx,
    )


async def _join_suffix(msg, uid, text, sdata, ctx):
    cid = sdata.get("join_channel_id")
    ch = await get_channel(cid) if cid else None
    bank = ch["bank"] if ch else BANK_CBE
    need = 8 if bank == BANK_CBE else 5
    cleaned = re.sub(r"\D", "", text)
    if len(cleaned) < need:
        await msg.reply_text(
            f"❌ Enter exactly the last *{need} digits* (numbers only).",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_join_payment(cid),
        )
        return
    suffix = cleaned[-need:]
    await session_update_data(uid, join_suffix=suffix)
    pending = sdata.get("join_pending_txn", "")
    if pending:
        sdata["join_suffix"] = suffix
        await session_set(uid, S_JOIN_TXN, sdata)
        await _join_txn(msg, uid, pending, sdata, ctx)


async def _join_phone(msg, uid, text, sdata, ctx):
    if not re.match(r"^\d{10,13}$", text.replace("+", "").replace(" ", "")):
        await msg.reply_text("❌ Invalid phone. Use format: `251912345678`", parse_mode=ParseMode.MARKDOWN, reply_markup=kb_cancel())
        return
    await session_update_data(uid, join_phone=text)
    pending = sdata.get("join_pending_txn", "")
    if pending:
        sdata["join_phone"] = text
        await session_set(uid, S_JOIN_TXN, sdata)
        await _join_txn(msg, uid, pending, sdata, ctx)


async def _join_image_text(msg, uid, text, sdata, ctx):
    """Handle text input when expecting an image for channel join."""
    await msg.reply_text(
        "📷 Please send a screenshot of your payment receipt, not text.\n\n"
        "*Make sure the image shows:*\n"
        "• Transaction ID or receipt number\n"
        "• Amount paid\n"
        "• Date & time\n"
        "• Clear and readable\n\n"
        "Or use /cancel to go back.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_cancel(),
    )


async def _verify_image_text(msg, uid, text, sdata):
    """Handle text input when expecting an image for standalone verification."""
    await msg.reply_text(
        "📷 Please send a screenshot of your payment receipt, not text.\n\n"
        "*Make sure the image shows:*\n"
        "• Transaction ID or receipt number\n"
        "• Amount paid\n"
        "• Date & time\n"
        "• Clear and readable",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=kb_cancel(),
    )


async def _process_join_payment(msg, uid, ch: dict, params: dict, ctx):
    thinking = await msg.reply_text("🔄 *Verifying payment…*", parse_mode=ParseMode.MARKDOWN)
    result = await verify_for_channel(uid, ch["bank"], ch, params)

    try:
        await thinking.delete()
    except TelegramError:
        pass

    if not result.success:
        suggestions = ""
        if result.status == "amount_mismatch":
            suggestions = f"\n\n💡 Please pay exactly *{ch['price']:,.0f} ETB* and try again."
        elif result.status == "duplicate":
            suggestions = "\n\n💡 Use a different (newer) transaction for a fresh payment."
        elif result.status == "expired_txn":
            suggestions = "\n\n💡 Make a new payment and submit the fresh transaction ID."
        await msg.reply_text(
            f"❌ *Verification Failed*\n\n{result.error}{suggestions}\n\n"
            "_Having trouble? Use /support for help._",
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=kb_join_payment(ch["id"]),
        )
        return

    try:
        was_member = await get_active_member(ch["id"], uid)
        invite, exp = await grant_access(ctx.application, ch, msg.from_user, result.txn_id, result.amount)
        action = "Renewed" if was_member else "Access Granted"
        amount_line = f"💰 Amount: *{result.amount:,.2f} ETB*\n" if result.amount else ""

        # Build full transaction detail block from the raw API response
        txn_detail = ""
        if result.raw and result.raw.get("success"):
            detail_lines = fmt_bank_result(ch["bank"], result.raw, channel=ch).split("\n")
            # Skip the header line (first line) since we already have context
            detail_lines = detail_lines[2:]  # skip "✅ *...*" and separator
            if detail_lines:
                txn_detail = "\n\n📄 *Transaction Details:*\n" + "\n".join(detail_lines)

        await msg.reply_text(
            f"✅ *{action}!*\n\n"
            f"📡 Channel: *{ch['title']}*\n"
            f"{amount_line}"
            f"⏰ Expires: {exp}"
            f"{txn_detail}\n\n"
            f"👇 *Tap the link below to join:*\n{invite}\n\n"
            f"_This link is single-use and expires in {INVITE_LINK_EXPIRY_HOURS}h._",
            parse_mode=ParseMode.MARKDOWN,
            disable_web_page_preview=True,
        )
        try:
            uname = f"@{msg.from_user.username}" if msg.from_user.username else f"ID:{uid}"

            # Prepare full transaction fields for admin notification
            raw_data = result.raw.get("data", result.raw) if result.raw else {}
            txn_lines = ["📋 *Full Transaction Data:*"]
            for k, v in sorted(raw_data.items()):
                if v not in (None, "") and k.lower() not in ("success",):
                    txn_lines.append(f"  • *{k}:* {v}")
            txn_block = "\n".join(txn_lines) if len(txn_lines) > 1 else ""

            await ctx.bot.send_message(
                ch["admin_id"],
                f"✅ *New {'Renewal' if was_member else 'Subscriber'}*\n"
                f"👤 {msg.from_user.first_name} ({uname})\n"
                f"📡 {ch['title']}\n"
                f"🔑 Txn: `{result.txn_id}`\n"
                f"⏰ Expires: {exp}\n"
                f"🔢 Receiver Acct: `{ch.get('account_number', 'N/A')}`\n"
                f"👤 Receiver Name: {ch.get('account_name', 'N/A')}\n\n"
                f"{txn_block}",
                parse_mode=ParseMode.MARKDOWN,
            )
        except TelegramError:
            pass
    except TelegramError as e:
        logger.error("Grant access / invite link failed: %s", e)
        await msg.reply_text(
            f"✅ *Payment verified!*\n\n"
            f"However, I couldn't generate your invite link right now.\n"
            f"Your Txn ID: `{result.txn_id}`\n\n"
            f"Please contact the channel admin and share this ID.",
            parse_mode=ParseMode.MARKDOWN,
        )

    await session_clear(uid)


# ── Image handler ─────────────────────────────────────────────────────────────

async def _handle_image(update, ctx, state, sdata):
    msg = update.message
    uid = update.effective_user.id
    photo = msg.photo[-1] if msg.photo else None
    doc = msg.document if msg.document else None
    if not photo and not doc:
        await msg.reply_text("❌ Could not read image. Try again.", reply_markup=kb_cancel())
        return

    thinking = await msg.reply_text("🔄 *Analyzing receipt…*", parse_mode=ParseMode.MARKDOWN)
    try:
        if photo:
            f = await photo.get_file()
            filename = f"receipt_{photo.file_id}.jpg"
            image_bytes = bytes(await f.download_as_bytearray())
        else:
            f = await doc.get_file()
            filename = doc.file_name or "receipt.jpg"
            image_bytes = bytes(await f.download_as_bytearray())

        if state == S_JOIN_IMAGE:
            cid = sdata.get("join_channel_id")
            ch = await get_channel(cid)
            if not ch:
                await thinking.delete()
                await msg.reply_text("❌ Channel not found. Try the join link again.")
                await session_clear(uid)
                return
            result = await verify_for_channel(uid, BANK_IMAGE, ch, {
                "image_bytes": image_bytes, "filename": filename,
            })
            try:
                await thinking.delete()
            except TelegramError:
                pass
            if not result.success:
                await msg.reply_text(
                    f"❌ *Image Verification Failed*\n\n{result.error}\n\n"
                    "💡 Try a clearer screenshot, or enter your transaction ID manually.",
                    parse_mode=ParseMode.MARKDOWN,
                    reply_markup=kb_join_payment(ch["id"]),
                )
                return
            await _process_join_payment(msg, uid, ch, {"reference": result.txn_id}, ctx)
        else:
            # Standalone image verify (S_IMAGE state)
            raw = await verifier.verify(BANK_IMAGE, {"image_bytes": image_bytes, "filename": filename})
            try:
                await thinking.delete()
            except TelegramError:
                pass
            text = fmt_bank_result(BANK_IMAGE, raw) if raw.get("success") else fmt_error(BANK_IMAGE, raw)
            await msg.reply_text(text, parse_mode=ParseMode.MARKDOWN)
            await session_clear(uid)
            await msg.reply_text("Would you like to verify another?", reply_markup=kb_again())
    except Exception as e:
        logger.error("Image processing error: %s", e)
        try:
            await thinking.delete()
        except TelegramError:
            pass
        await msg.reply_text(
            "❌ Failed to process image. Please try again or use a text transaction ID.",
            reply_markup=kb_cancel(),
        )


# ── Standalone verification steps ─────────────────────────────────────────────

async def _verify_ref(msg, uid, text, sdata):
    bank = sdata.get("bank")
    if not bank:
        await msg.reply_text("Session error. Use /verify to start again.")
        await session_clear(uid)
        return
    await session_update_data(uid, reference=text)
    if bank in (BANK_CBE, BANK_ABYSSINIA):
        await session_set(uid, S_SUFFIX)
        hint = "last 8 digits of your CBE account" if bank == BANK_CBE else "last 5 digits of your Abyssinia account"
        await msg.reply_text(f"🔢 Enter the *{hint}*:", parse_mode=ParseMode.MARKDOWN, reply_markup=kb_cancel())
    elif bank == BANK_CBEBIRR:
        await session_set(uid, S_PHONE)
        await msg.reply_text("📱 Enter your phone number (`251912345678`):", parse_mode=ParseMode.MARKDOWN, reply_markup=kb_cancel())
    else:
        await _do_standalone_verify(msg, uid, sdata)


async def _verify_suffix(msg, uid, text, sdata):
    await session_update_data(uid, suffix=text)
    _, updated_data = await session_get(uid)
    await _do_standalone_verify(msg, uid, updated_data)


async def _verify_phone(msg, uid, text, sdata):
    await session_update_data(uid, phone=text)
    _, updated_data = await session_get(uid)
    await _do_standalone_verify(msg, uid, updated_data)


async def _do_standalone_verify(msg, uid, sdata):
    bank = sdata.get("bank")
    thinking = await msg.reply_text("🔄 *Verifying payment…*", parse_mode=ParseMode.MARKDOWN)
    params = {
        "reference": sdata.get("reference", ""),
        "suffix": sdata.get("suffix", ""),
        "phone": sdata.get("phone", ""),
    }
    raw = await verifier.verify(bank, params)
    try:
        await thinking.delete()
    except TelegramError:
        pass

    # Show ALL raw fields the API returned
    if raw.get("success"):
        text = fmt_bank_result(bank, raw)
    else:
        text = fmt_error(bank, raw)

    await msg.reply_text(text, parse_mode=ParseMode.MARKDOWN)
    await session_clear(uid)
    await msg.reply_text("Would you like to verify another payment?", reply_markup=kb_again())


# ── Edit channel field ────────────────────────────────────────────────────────

async def _edit_field(msg, uid, text, sdata, column, cast):
    cid = sdata.get("edit_channel_id")
    if not cid or not await is_channel_admin(cid, uid):
        await msg.reply_text("❌ Not authorized.")
        return
    try:
        if cast == str:
            val = text.strip()[:255]
            if not val:
                raise ValueError("Empty")
            if column == "account_number" and not re.match(r"^\d{8,20}$", val.replace(" ", "")):
                await msg.reply_text("❌ Account number must be 8–20 digits.", reply_markup=kb_cancel())
                return
        else:
            val = cast(text.replace(",", "").strip())
        if column == "price" and not (0 < val <= 1_000_000):
            raise ValueError
        if column == "billing_days" and not (0 < val <= 3650):
            raise ValueError
    except Exception:
        limits = {
            "price": "1–1,000,000 ETB",
            "billing_days": "1–3,650 days",
        }
        hint = limits.get(column, "a valid value")
        await msg.reply_text(f"❌ Invalid. Enter {hint}.", reply_markup=kb_cancel())
        return
    await db._conn.execute(
        f"UPDATE channels SET {column}=?, updated_at=datetime('now') WHERE id=?", (val, cid),
    )
    await db._conn.commit()
    await session_clear(uid)
    field_name = column.replace("_", " ").title()
    ch = await get_channel(cid)
    await msg.reply_text(
        f"✅ *{field_name}* updated successfully.\n\nUse /my\\_channels to see changes.",
        parse_mode=ParseMode.MARKDOWN,
    )


async def _add_admin(msg, uid, text, sdata):
    cid = sdata.get("edit_channel_id")
    if not cid or not await is_channel_admin(cid, uid):
        await msg.reply_text("❌ Not authorized.")
        return
    try:
        new_admin_id = int(text.strip())
    except ValueError:
        await msg.reply_text("❌ Enter a numeric Telegram User ID (e.g. `123456789`).", parse_mode=ParseMode.MARKDOWN, reply_markup=kb_cancel())
        return
    if new_admin_id == uid:
        await msg.reply_text("❌ You're already an admin of this channel.")
        return
    await db._conn.execute(
        "INSERT OR IGNORE INTO channel_admins (channel_id, user_id, role) VALUES (?,?,?)",
        (cid, new_admin_id, "moderator"),
    )
    await db._conn.commit()
    await session_clear(uid)
    ch = await get_channel(cid)
    await msg.reply_text(
        f"✅ User `{new_admin_id}` added as co-admin to *{ch['title']}*.\n\n"
        f"They can now use /my\\_channels to manage this channel.",
        parse_mode=ParseMode.MARKDOWN,
    )


# ── Error handler ─────────────────────────────────────────────────────────────

async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    logger.error("Unhandled exception: %s", ctx.error, exc_info=ctx.error)
    if update and hasattr(update, "effective_message") and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "❌ Something went wrong. Please try again.\n"
                "If the problem persists, use /support."
            )
        except Exception:
            pass
    if ADMIN_ALERT_CHAT_ID:
        try:
            await ctx.bot.send_message(
                int(ADMIN_ALERT_CHAT_ID),
                f"🚨 *Bot Error*\n\n`{str(ctx.error)[:400]}`",
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            pass


# ── Application lifecycle ─────────────────────────────────────────────────────

async def post_init(app: Application):
    await db.connect()
    await db.migrate_legacy_json()
    await sync_bot_username(app.bot)
    await ensure_join_links()

    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        job_expiry_warnings, "interval",
        hours=WARNING_INTERVAL_HOURS, args=[app],
        id="warnings", max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        job_kick_expired, "interval",
        hours=KICK_INTERVAL_HOURS, args=[app],
        id="kick", max_instances=1, coalesce=True,
    )
    scheduler.add_job(
        job_daily_cleanup, "cron",
        hour=2, minute=0,
        id="cleanup",
    )
    scheduler.add_job(
        job_health_check, "interval",
        hours=24, args=[app],
        id="health", max_instances=1,
    )
    scheduler.start()
    app.bot_data["scheduler"] = scheduler

    logger.info(
        "✅ Bot ready | @%s | DB: %s | Jobs: warnings/%dh, kick/%dh",
        BOT_USERNAME or "unknown", DB_PATH, WARNING_INTERVAL_HOURS, KICK_INTERVAL_HOURS,
    )


async def post_shutdown(app: Application):
    sched = app.bot_data.get("scheduler")
    if sched and sched.running:
        sched.shutdown(wait=False)
    await verifier.close()
    await db.close()
    logger.info("Bot shut down cleanly.")


def build_app() -> Application:
    if not BOT_TOKEN:
        logger.critical("BOT_TOKEN is not set in .env!")
        sys.exit(1)
    if not API_KEY:
        logger.critical("VERIFIER_API_KEY is not set in .env!")
        sys.exit(1)
    if not BOT_USERNAME:
        logger.warning("BOT_USERNAME not set — join links may not work until first /start.")

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("support", cmd_support))
    app.add_handler(CommandHandler("verify", cmd_verify))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("join", cmd_join))
    app.add_handler(CommandHandler("my_subscriptions", cmd_my_subscriptions))
    app.add_handler(CommandHandler("setup_channel", cmd_setup_channel))
    app.add_handler(CommandHandler("my_channels", cmd_my_channels))
    app.add_handler(CommandHandler("members", cmd_members))
    app.add_handler(CommandHandler("edit_channel", cmd_edit_channel))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("transactions", cmd_transactions))
    app.add_handler(CommandHandler("export", cmd_export))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))
    app.add_handler(CommandHandler("regenerate_links", cmd_regenerate_links))
    app.add_handler(ChatJoinRequestHandler(on_join_request))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(
        (filters.TEXT & ~filters.COMMAND) | filters.PHOTO | filters.Document.IMAGE,
        on_message,
    ))
    app.add_error_handler(on_error)
    return app


async def async_main():
    app = build_app()
    logger.info("🚀 Starting Ethiopian Payment Bot…")
    async with app:
        await post_init(app)
        await app.start()
        await app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES,
        )
        logger.info("✅ Polling started. Press Ctrl+C to stop.")
        try:
            await asyncio.Event().wait()
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            logger.info("Shutting down…")
            # Correct PTB shutdown order:
            # 1. Stop polling first
            # 2. Stop the app (required — without this, the `async with app:`
            #    context manager's __aexit__ raises
            #    "RuntimeError: This Application is still running!")
            # 3. Clean up our resources (scheduler, db, http client)
            await app.updater.stop()
            await app.stop()
            await post_shutdown(app)


if __name__ == "__main__":
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass
