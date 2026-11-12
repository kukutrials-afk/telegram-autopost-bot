import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from aiohttp import web
from telegram import Update
from telegram.error import Forbidden, RetryAfter, TelegramError
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# Optional PostgreSQL support (for Neon / Supabase / Render)
try:
    import psycopg2
    import psycopg2.extras
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False

# ==========================================
# CONFIGURATION (via Environment Variables)
# ==========================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "").strip()
ADMIN_USER_ID = int(os.environ.get("ADMIN_USER_ID", "0"))
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
PORT = int(os.environ.get("PORT", "10000"))

DEFAULT_INTERVAL_MINUTES = int(os.environ.get("INTERVAL_MINUTES", "30"))
DEFAULT_BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))
DEFAULT_MODE = os.environ.get("FORWARD_MODE", "forward").lower()  # "forward" or "copy"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("AutoPostBot")


# ==========================================
# DATABASE LAYER (PostgreSQL + SQLite fallback)
# ==========================================
class Database:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.use_pg = bool(db_url and HAS_POSTGRES)
        self.sqlite_path = "bot_data.db"
        self._init_db()

    def _get_conn(self):
        if self.use_pg:
            return psycopg2.connect(self.db_url)
        conn = sqlite3.connect(self.sqlite_path)
        return conn

    def _ph(self) -> str:
        return "%s" if self.use_pg else "?"

    def _init_db(self):
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS target_channels (
                    channel_id BIGINT PRIMARY KEY,
                    title TEXT,
                    added_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS posts (
                    message_id BIGINT PRIMARY KEY,
                    created_at TEXT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sent_logs (
                    target_channel_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    sent_at TEXT NOT NULL,
                    PRIMARY KEY (target_channel_id, message_id)
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

        # Initialize default settings if not present
        if self.get_setting("interval_minutes") is None:
            self.set_setting("interval_minutes", str(DEFAULT_INTERVAL_MINUTES))
        if self.get_setting("batch_size") is None:
            self.set_setting("batch_size", str(DEFAULT_BATCH_SIZE))
        if self.get_setting("mode") is None:
            self.set_setting("mode", DEFAULT_MODE)

        env_main = os.environ.get("MAIN_CHANNEL_ID", "").strip()
        if env_main and self.get_setting("main_channel_id") is None:
            self.set_setting("main_channel_id", env_main)

    def get_setting(self, key: str) -> Optional[str]:
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT value FROM settings WHERE key = {self._ph()}", (key,))
            row = cur.fetchone()
            return row[0] if row else None
        finally:
            conn.close()

    def set_setting(self, key: str, value: str):
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            p = self._ph()
            if self.use_pg:
                cur.execute(
                    f"""
                    INSERT INTO settings (key, value) VALUES ({p}, {p})
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                    """,
                    (key, value),
                )
            else:
                cur.execute(
                    f"INSERT OR REPLACE INTO settings (key, value) VALUES ({p}, {p})",
                    (key, value),
                )
            conn.commit()
        finally:
            conn.close()

    def add_target_channel(self, channel_id: int, title: str = ""):
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            p = self._ph()
            now = datetime.now(timezone.utc).isoformat()
            if self.use_pg:
                cur.execute(
                    f"""
                    INSERT INTO target_channels (channel_id, title, added_at)
                    VALUES ({p}, {p}, {p})
                    ON CONFLICT (channel_id) DO UPDATE SET title = EXCLUDED.title
                    """,
                    (channel_id, title, now),
                )
            else:
                cur.execute(
                    f"INSERT OR REPLACE INTO target_channels (channel_id, title, added_at) VALUES ({p}, {p}, {p})",
                    (channel_id, title, now),
                )
            conn.commit()
        finally:
            conn.close()

    def remove_target_channel(self, channel_id: int) -> bool:
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                f"DELETE FROM target_channels WHERE channel_id = {self._ph()}",
                (channel_id,),
            )
            deleted = cur.rowcount > 0
            conn.commit()
            return deleted
        finally:
            conn.close()

    def get_target_channels(self) -> list[tuple[int, str]]:
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT channel_id, COALESCE(title, '') FROM target_channels ORDER BY added_at ASC")
            return [(int(r[0]), r[1]) for r in cur.fetchall()]
        finally:
            conn.close()

    def save_post(self, message_id: int):
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            p = self._ph()
            now = datetime.now(timezone.utc).isoformat()
            if self.use_pg:
                cur.execute(
                    f"""
                    INSERT INTO posts (message_id, created_at) VALUES ({p}, {p})
                    ON CONFLICT (message_id) DO NOTHING
                    """,
                    (message_id, now),
                )
            else:
                cur.execute(
                    f"INSERT OR IGNORE INTO posts (message_id, created_at) VALUES ({p}, {p})",
                    (message_id, now),
                )
            conn.commit()
        finally:
            conn.close()

    def get_total_posts(self) -> int:
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM posts")
            return int(cur.fetchone()[0])
        finally:
            conn.close()

    def get_unsent_posts(self, target_channel_id: int, limit: int) -> list[int]:
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            p = self._ph()
            # 1. Pehle bilkul fresh posts uthao jo abhi tak KISI bhi target channel me nahi gaye
            cur.execute(
                f"""
                SELECT p.message_id
                FROM posts p
                WHERE NOT EXISTS (
                    SELECT 1 FROM sent_logs s WHERE s.message_id = p.message_id
                )
                ORDER BY p.message_id ASC
                LIMIT {p}
                """,
                (limit,),
            )
            fresh_ids = [int(r[0]) for r in cur.fetchall()]

            # 2. Agar fresh posts kam padein, to bache hue slots me doosre channels ke posts
            #    uthao jo ISS target channel me abhi tak nahi gaye (same channel me kabhi repeat nahi hoga)
            remaining = limit - len(fresh_ids)
            if remaining > 0:
                if fresh_ids:
                    placeholders = ", ".join([p] * len(fresh_ids))
                    cur.execute(
                        f"""
                        SELECT p.message_id
                        FROM posts p
                        LEFT JOIN sent_logs s
                          ON p.message_id = s.message_id AND s.target_channel_id = {p}
                        WHERE s.message_id IS NULL
                          AND p.message_id NOT IN ({placeholders})
                        ORDER BY p.message_id ASC
                        LIMIT {p}
                        """,
                        (target_channel_id, *fresh_ids, remaining),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT p.message_id
                        FROM posts p
                        LEFT JOIN sent_logs s
                          ON p.message_id = s.message_id AND s.target_channel_id = {p}
                        WHERE s.message_id IS NULL
                        ORDER BY p.message_id ASC
                        LIMIT {p}
                        """,
                        (target_channel_id, remaining),
                    )
                fresh_ids.extend(int(r[0]) for r in cur.fetchall())

            return fresh_ids
        finally:
            conn.close()

    def count_unsent_posts(self, target_channel_id: int) -> int:
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            p = self._ph()
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM posts p
                LEFT JOIN sent_logs s
                  ON p.message_id = s.message_id AND s.target_channel_id = {p}
                WHERE s.message_id IS NULL
                """,
                (target_channel_id,),
            )
            return int(cur.fetchone()[0])
        finally:
            conn.close()

    def mark_sent(self, target_channel_id: int, message_id: int):
        conn = self._get_conn()
        try:
            cur = conn.cursor()
            p = self._ph()
            now = datetime.now(timezone.utc).isoformat()
            if self.use_pg:
                cur.execute(
                    f"""
                    INSERT INTO sent_logs (target_channel_id, message_id, sent_at)
                    VALUES ({p}, {p}, {p})
                    ON CONFLICT (target_channel_id, message_id) DO NOTHING
                    """,
                    (target_channel_id, message_id, now),
                )
            else:
                cur.execute(
                    f"INSERT OR IGNORE INTO sent_logs (target_channel_id, message_id, sent_at) VALUES ({p}, {p}, {p})",
                    (target_channel_id, message_id, now),
                )
            conn.commit()
        finally:
            conn.close()


db: Optional[Database] = None


# ==========================================
# HELPER FUNCTIONS
# ==========================================
def is_admin(update: Update) -> bool:
    if ADMIN_USER_ID == 0:
        return True  # If not set, allow any DM (set ADMIN_USER_ID for security!)
    user = update.effective_user
    return user is not None and user.id == ADMIN_USER_ID


async def parse_channel_arg(context: ContextTypes.DEFAULT_TYPE, arg: str) -> tuple[Optional[int], str]:
    arg = arg.strip()
    try:
        if arg.startswith("@") or not arg.lstrip("-").isdigit():
            chat = await context.bot.get_chat(arg)
            return chat.id, chat.title or arg
        else:
            cid = int(arg)
            try:
                chat = await context.bot.get_chat(cid)
                return cid, chat.title or str(cid)
            except TelegramError:
                return cid, str(cid)
    except Exception as e:
        logger.warning(f"Could not resolve channel {arg}: {e}")
        return None, ""


# ==========================================
# BOT COMMANDS
# ==========================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    text = (
        "🤖 *Telegram Auto-Posting Bot Ready!*\n\n"
        "*Commands:*\n"
        "• `/setmain -100xxxx` — Main channel set karo\n"
        "• `/add -100xxxx` — Target channel add karo\n"
        "• `/remove -100xxxx` — Target channel hatao\n"
        "• `/list` — Saare channels & pending posts dekho\n"
        "• `/sendnow` — Abhi ke abhi 1 batch send karo\n"
        "• `/mode forward` ya `/mode copy` — Forward tag rakhna hai ya clean copy\n"
        "• `/setbatch 10` — Har baar kitne posts bhejne hain\n"
        "• `/setinterval 30` — Kitne minutes baad bhejna hai\n\n"
        "💡 *Tip:* Kisi bhi channel ka ID pata karne ke liye uss channel se koi bhi post mujhe yahan forward kar do!"
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def cmd_setmain(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: `/setmain -1001234567890` (ya `@channelusername`)", parse_mode="Markdown")
        return

    cid, title = await parse_channel_arg(context, context.args[0])
    if cid is None:
        await update.message.reply_text("❌ Channel nahi mila. Make sure bot uss channel me Admin hai aur ID sahi hai.")
        return

    db.set_setting("main_channel_id", str(cid))
    await update.message.reply_text(f"✅ Main channel set ho gaya:\n*{title}* (`{cid}`)", parse_mode="Markdown")


async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: `/add -1001234567890`", parse_mode="Markdown")
        return

    cid, title = await parse_channel_arg(context, context.args[0])
    if cid is None:
        await update.message.reply_text("❌ Channel nahi mila. Make sure bot uss channel me Admin hai aur ID sahi hai.")
        return

    main_id = db.get_setting("main_channel_id")
    if main_id and str(cid) == main_id:
        await update.message.reply_text("⚠️ Ye already Main channel hai! Target channel alag hona chahiye.")
        return

    db.add_target_channel(cid, title)
    await update.message.reply_text(f"✅ Target channel added:\n*{title}* (`{cid}`)", parse_mode="Markdown")


async def cmd_remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: `/remove -1001234567890`", parse_mode="Markdown")
        return
    try:
        cid = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Valid numeric Channel ID daalo (jaise `-1001234567890`).", parse_mode="Markdown")
        return

    if db.remove_target_channel(cid):
        await update.message.reply_text(f"🗑️ Target channel `{cid}` remove ho gaya.", parse_mode="Markdown")
    else:
        await update.message.reply_text("⚠️ Ye channel list me nahi tha.")


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    main_id = db.get_setting("main_channel_id") or "Not Set"
    interval = db.get_setting("interval_minutes")
    batch = db.get_setting("batch_size")
    mode = db.get_setting("mode")
    total_posts = db.get_total_posts()
    targets = db.get_target_channels()

    lines = [
        f"📌 *Main Channel:* `{main_id}`",
        f"📦 *Total Saved Posts:* {total_posts}",
        f"⏱️ *Schedule:* {batch} posts every {interval} mins (`{mode}` mode)",
        "",
        "*Target Channels:*",
    ]
    if not targets:
        lines.append("• Koi target channel add nahi hai. `/add -100xxxx` use karo.")
    else:
        for cid, title in targets:
            pending = db.count_unsent_posts(cid)
            label = title if title else str(cid)
            lines.append(f"• *{label}* (`{cid}`) — Pending: {pending}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args or context.args[0].lower() not in ("forward", "copy"):
        await update.message.reply_text("Usage: `/mode forward` (tag ke saath) ya `/mode copy` (bina tag ke)", parse_mode="Markdown")
        return
    mode = context.args[0].lower()
    db.set_setting("mode", mode)
    await update.message.reply_text(f"✅ Mode set to: `{mode}`", parse_mode="Markdown")


async def cmd_setbatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args or not context.args[0].isdigit() or int(context.args[0]) < 1:
        await update.message.reply_text("Usage: `/setbatch 10`", parse_mode="Markdown")
        return
    val = int(context.args[0])
    db.set_setting("batch_size", str(val))
    await update.message.reply_text(f"✅ Ab har cycle me *{val}* posts send honge.", parse_mode="Markdown")


async def cmd_setinterval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    if not context.args or not context.args[0].isdigit() or int(context.args[0]) < 1:
        await update.message.reply_text("Usage: `/setinterval 30` (minutes)", parse_mode="Markdown")
        return
    val = int(context.args[0])
    db.set_setting("interval_minutes", str(val))
    await update.message.reply_text(f"✅ Interval set to *{val} minutes*.", parse_mode="Markdown")


async def cmd_sendnow(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return
    await update.message.reply_text("🚀 Batch sending start kar raha hu...")
    summary = await run_auto_post_cycle(context.bot)
    await update.message.reply_text(summary)


# ==========================================
# CHANNEL POST LISTENER & FORWARDED ID HELPER
# ==========================================
async def handle_channel_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    post = update.channel_post
    if not post:
        return
    main_id = db.get_setting("main_channel_id")
    if main_id and str(post.chat.id) == main_id:
        db.save_post(post.message_id)
        logger.info(f"Saved new post {post.message_id} from main channel {main_id}")


async def handle_forwarded_in_dm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update) or not update.message:
        return
    origin = update.message.forward_origin
    if origin and hasattr(origin, "chat") and origin.chat:
        chat = origin.chat
        await update.message.reply_text(
            f"📢 *Channel Info:*\nName: {chat.title}\nID: `{chat.id}`\n\n"
            f"Main banane ke liye: `/setmain {chat.id}`\n"
            f"Target add karne ke liye: `/add {chat.id}`",
            parse_mode="Markdown",
        )


# ==========================================
# AUTO-POSTING ENGINE (NO DUPLICATES)
# ==========================================
async def run_auto_post_cycle(bot) -> str:
    main_id_str = db.get_setting("main_channel_id")
    if not main_id_str:
        return "⚠️ Main channel set nahi hai. Pehle `/setmain -100xxxx` chalao."

    main_id = int(main_id_str)
    targets = db.get_target_channels()
    if not targets:
        return "⚠️ Koi target channel add nahi hai. `/add -100xxxx` use karo."

    batch_s
