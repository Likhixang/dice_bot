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

# 每次停机修复后更新此处，停机补偿公告会自动带上本次修复说明
LAST_FIX_DESC = (
    "• 发车面板新增参与玩家列表\n"
    "• /event 改为展示24小时内事件，支持分页翻页（仅发起人可按），无操作1分钟后自动删除\n"
    "• 新增「停机维护」超管口令：自动退款对局和红包，置顶维护公告，并锁定发车/发红包功能直到停机补偿发出\n"
    "• 事件日志上限从30条扩展至200条"
)

TZ_BJ = datetime.timezone(datetime.timedelta(hours=8))

PATTERN = re.compile(r"^(大|小)\s*([+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)(?:\s+([+-]?\d+))?\s*(多)?\s*([+-]?\d+)?$")

# 并发互斥锁（dict 是可变对象，所有模块 from config import game_locks 后共享同一引用）
game_locks: dict = {}

def get_lock(game_id: str):
    if game_id not in game_locks:
        game_locks[game_id] = asyncio.Lock()
    return game_locks[game_id]
