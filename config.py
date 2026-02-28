import os
import re
import asyncio
import datetime

TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("🚫 致命阻断：环境变量中未找到 BOT_TOKEN，请检查 .env 文件及 docker-compose 映射！")

BOT_ID = int(os.getenv("BOT_ID", "0"))
if not BOT_ID:
    raise ValueError("🚫 致命阻断：环境变量中未找到 BOT_ID，请检查 .env 文件！")

SUPER_ADMIN_ID = int(os.getenv("SUPER_ADMIN_ID", "0"))
if not SUPER_ADMIN_ID:
    raise ValueError("🚫 致命阻断：环境变量中未找到 SUPER_ADMIN_ID，请检查 .env 文件！")

ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
if not ADMIN_IDS:
    raise ValueError("🚫 致命阻断：环境变量中未找到 ADMIN_IDS，请检查 .env 文件！")

# 话题频道限制（0 表示不限制）
ALLOWED_CHAT_ID = int(os.getenv("ALLOWED_CHAT_ID", "0"))
ALLOWED_THREAD_ID = int(os.getenv("ALLOWED_THREAD_ID", "0"))

# 每次停机修复后更新此处，停机补偿公告会自动带上本次修复说明
LAST_FIX_DESC = (
    "• 修复停机补偿置顶不会自动消失的问题（重启后清理协程丢失）"
)

TZ_BJ = datetime.timezone(datetime.timedelta(hours=8))

PATTERN = re.compile(r"^(大|小)\s*([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)(?:\s+([+-]?\d+))?\s*(多)?\s*([+-]?\d+)?$")

# 并发互斥锁（dict 是可变对象，所有模块 from config import game_locks 后共享同一引用）
game_locks: dict = {}

def get_lock(game_id: str):
    if game_id not in game_locks:
        game_locks[game_id] = asyncio.Lock()
    return game_locks[game_id]
