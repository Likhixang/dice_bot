import asyncio
import datetime
import json
import logging
import math
import random
import re
import sqlite3
import time
import uuid

from aiogram import Router, F, types, BaseMiddleware
from aiogram.filters import Command
from typing import Callable, Dict, Any, Awaitable

from config import BOT_ID, SUPER_ADMIN_ID, ADMIN_IDS, TZ_BJ, PATTERN, LAST_FIX_DESC, get_lock, ALLOWED_CHAT_ID, ALLOWED_THREAD_ID
from core import bot, redis, CleanTextFilter
from utils import (get_mention, safe_html, delete_msgs, delete_msg_by_id,
                   reply_and_auto_delete, safe_zrevrange, safe_zrange, delete_msgs_by_ids)
from balance import get_or_init_balance, update_balance, get_period_keys
from tasks import perform_backup
from game import start_game_creation, start_rolling_phase, rank_panel_watcher, refund_game
from game_settle import process_dice_value
from redpack import (build_redpack_panel, refresh_dice_panel, attempt_claim_pw_redpack,
                     redpack_expiry_watcher, generate_redpack_amounts)

router = Router()


# ==============================
# 话题频道限制中间件
# ==============================

class TopicRestrictionMiddleware(BaseMiddleware):
    def __init__(self, silent: bool = False):
        self.silent = silent

    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any]
    ) -> Any:
        if not ALLOWED_CHAT_ID:
            return await handler(event, data)
        if isinstance(event, types.Message):
            chat = event.chat
            if chat.type not in ("group", "supergroup"):
                return await handler(event, data)
            if chat.id != ALLOWED_CHAT_ID or event.message_thread_id != ALLOWED_THREAD_ID:
                if not self.silent:
                    await reply_and_auto_delete(event, "❌ 本 bot 仅在 🎲 赌博话题提供服务。")
                return
        elif isinstance(event, types.CallbackQuery):
            msg = event.message
            if msg and msg.chat.type in ("group", "supergroup"):
                if msg.chat.id != ALLOWED_CHAT_ID or msg.message_thread_id != ALLOWED_THREAD_ID:
                    if not self.silent:
                        try:
                            await event.answer("❌ 本 bot 仅在 🎲 赌博话题提供服务。", show_alert=True)
                        except Exception:
                            pass
                    return
        return await handler(event, data)


# ==============================
# 维护期全量拦截中间件
# ==============================

class MaintenanceMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Any, Dict[str, Any]], Awaitable[Any]],
        event: Any,
        data: Dict[str, Any]
    ) -> Any:
        if isinstance(event, types.Message):
            chat_id = event.chat.id
            if await redis.exists(f"maintenance:{chat_id}"):
                await reply_and_auto_delete(event, "🔧 <b>系统维护中</b>，暂停所有功能，请等待维护完成后再操作。")
                return
        elif isinstance(event, types.CallbackQuery):
            chat_id = event.message.chat.id if event.message else None
            if chat_id and await redis.exists(f"maintenance:{chat_id}"):
                try:
                    await event.answer("🔧 系统维护中，请稍后再试", show_alert=True)
                except Exception:
                    pass
                return
        return await handler(event, data)


router.message.middleware(TopicRestrictionMiddleware())
router.callback_query.middleware(TopicRestrictionMiddleware())
router.message.middleware(MaintenanceMiddleware())
router.callback_query.middleware(MaintenanceMiddleware())


# ==============================
# 排行榜辅助函数
# ==============================

async def get_leaderboard_text(period: str, board: str, title: str) -> str:
    daily_k, weekly_k, monthly_k = get_period_keys()
    period_map = {"daily": daily_k, "weekly": weekly_k, "monthly": monthly_k}
    period_key = period_map.get(period, daily_k)

    lines = [f"🏆 <b>{title}</b>\n"]

    if board == "net":
        raw_winners = await safe_zrevrange(f"rank_points:{period}:{period_key}", 0, 9, withscores=True)
        raw_losers = await safe_zrange(f"rank_points:{period}:{period_key}", 0, 9, withscores=True)
        top_winners = [(uid, score) for uid, score in raw_winners if score > 0][:5]
        top_losers = [(uid, score) for uid, score in raw_losers if score < 0][:5]
        lines.append("📈 <b>净赢家 TOP 5</b>")
        if top_winners:
            for i, (uid, score) in enumerate(top_winners):
                name = await redis.hget("user_names", uid) or "未知玩家"
                lines.append(f"{i+1}. {get_mention(uid, name)} | +{score:g}")
        else:
            lines.append("暂无盈利数据。")
        lines.append("\n📉 <b>净亏损 TOP 5</b>")
        if top_losers:
            for i, (uid, score) in enumerate(top_losers):
                name = await redis.hget("user_names", uid) or "未知玩家"
                lines.append(f"{i+1}. {get_mention(uid, name)} | {score:g}")
        else:
            lines.append("暂无亏损数据。")
    else:
        top_winners = await safe_zrevrange(f"rank_gross_wins:{period}:{period_key}", 0, 4, withscores=True)
        top_losers = await safe_zrevrange(f"rank_gross_losses:{period}:{period_key}", 0, 4, withscores=True)
        lines.append("📈 <b>赢家榜 TOP 5</b>")
        if top_winners:
            for i, (uid, score) in enumerate(top_winners):
                name = await redis.hget("user_names", uid) or "未知玩家"
                lines.append(f"{i+1}. {get_mention(uid, name)} | +{score:g}")
        else:
            lines.append("暂无盈利数据。")
        lines.append("\n📉 <b>散财榜 TOP 5</b>")
        if top_losers:
            for i, (uid, score) in enumerate(top_losers):
                name = await redis.hget("user_names", uid) or "未知玩家"
                lines.append(f"{i+1}. {get_mention(uid, name)} | -{score:g}")
        else:
            lines.append("暂无亏损数据。")

    return "\n".join(lines)


def get_rank_markup(period: str, board: str, uid: str) -> types.InlineKeyboardMarkup:
    def btn(label, p, b):
        is_active = (p == period and b == board)
        text = f"✅ {label}" if is_active else label
        return types.InlineKeyboardButton(text=text, callback_data=f"rank_sw:{p}:{b}:{uid}")
    row1 = [btn("今日", "daily", board), btn("本周", "weekly", board), btn("本月", "monthly", board)]
    row2 = [btn("胜负榜", period, "gross"), btn("净胜负榜", period, "net")]
    return types.InlineKeyboardMarkup(inline_keyboard=[row1, row2])


# ==============================
# 指令 handlers
# ==============================

@router.message(CleanTextFilter(), Command("dice_help"))
async def cmd_help(message: types.Message):
    help_text = """🎲 <b>骰子竞技场 · 指令与玩法指南</b> 🎲

🏷 <b>一、怎么发起对局？（4种实战姿势）</b>

<b>格式口诀：玩法金额 + 空格 + 骰子数</b>
（注：大小与金额可连打，数字之间必须用空格隔开。支持 1-5 颗，填 0 积分即为友谊赛）。

• <b>普通双人局</b>：发送 <code>大100 3</code>
（只等1人，有人点按钮立刻发车）

• <b>指定单挑局</b>：回复对手的消息发送 <code>大100 3</code>
（只准他接单，1分钟不理你自动退回积分；对方已在对局中则无法发起）

• <b>多人拼车局</b>：发送 <code>大100 3 多</code>
（2到5人都能玩。有人进就触发15秒倒计时，满5人瞬间发车）

• <b>定员死等局</b>：发送 <code>大100 3 多 4</code>
（结尾的 4 代表必须死等凑齐4人，少一个都不发车）

🏷 <b>二、连胜 / 连败奖惩</b>

• <b>乐善好施</b>：连赢 3 局（有积分加）→ 自动扣 200 积分，重置后循环计算
• <b>同舟共济</b>：连败 3 局（有积分扣）→ 自动补贴 +200 积分，重置后循环计算
（平局 ±0 重置计数；与名次无关，以实际盈亏符号判定）

🏷 <b>三、/dice_attack 单挑对决</b>

回复某人的消息发 <code>/dice_attack</code> 向其发起攻击！

• 发起时先扣 <b>1000 积分</b>，双方可在1分钟内反复追加（每次 +1000）
• 💥 <b>加大力度</b>：仅发起方可按   🛡 <b>回手反击</b>：仅迎战方可按
• 投入越多赢面越大（加权随机），每人最高投入 <b>20000</b> 积分
• 1分钟后自动结算：赢家取回本金 + 缴获对方 <b>90%</b> 投入（10% 销毁防刷）
• 对方未回应：全额退款，原面板自动销毁

🏷 <b>四、指令大全</b>

• <code>/dice_checkin</code>：每日签到领积分。<b>连续签到5天白送两万！</b>
• <code>/dice_bal</code>：查看自己的可用积分余额。
• <code>/dice_gift 100</code>：回复某人的消息发送，直接赠送他100积分。
• <code>/dice_redpack 1000 5</code>：发拼手气红包（总额1000，分5个包）。
• <code>/dice_redpack_pw 100 2 芝麻开门</code>：发口令红包，打出"芝麻开门"才能抢。
• <code>/dice_attack</code>：回复某人消息发起 Attack 对决。
• <code>/dice_rank</code>：查看今日胜负榜（支持按钮切换净赚榜）。
• <code>/dice_rank_week</code>：查看本周胜负榜。
• <code>/dice_rank_month</code>：查看本月胜负榜。
• <code>/dice_event</code>：查看过去24小时系统事件（彩蛋/补偿记录）。"""
    bot_msg = await message.reply(help_text)
    asyncio.create_task(delete_msgs([message, bot_msg], 60))


async def get_event_page(page: int, uid: str):
    """返回 (text, markup)，markup=None 表示单页无按钮"""
    all_raw = await redis.lrange("event_log", 0, -1)
    cutoff = time.time() - 86400
    valid = []
    for raw in all_raw:
        try:
            r = json.loads(raw)
            if r.get("ts", 0) >= cutoff:
                valid.append(r)
        except Exception:
            continue
    total = len(valid)
    per_page = 5
    total_pages = max(1, math.ceil(total / per_page))
    page = max(0, min(page, total_pages - 1))
    chunk = valid[page * per_page: page * per_page + per_page]
    if not chunk:
        return "📭 <b>过去24小时暂无系统事件</b>", None
    header = f"📋 <b>【过去24小时系统事件】</b>"
    if total_pages > 1:
        header += f" ({page + 1}/{total_pages})"
    lines = [header, ""]
    for r in chunk:
        dt = datetime.datetime.fromtimestamp(r["ts"], tz=TZ_BJ).strftime("%m/%d %H:%M")
        icon = "🔧" if r["type"] == "compensation" else "🎊"
        lines.append(f"{icon} <b>{r['desc']}</b>")
        lines.append(f"    ⏰ {dt} | 全员 <b>+{r['bonus']}</b> | 惠及 <b>{r['count']}</b> 人")
        lines.append("")
    text = "\n".join(lines).strip()
    if total_pages <= 1:
        return text, None
    btns = []
    if page > 0:
        btns.append(types.InlineKeyboardButton(text="◀️ 上一页", callback_data=f"ev_p:{uid}:{page - 1}"))
    if page < total_pages - 1:
        btns.append(types.InlineKeyboardButton(text="下一页 ▶️", callback_data=f"ev_p:{uid}:{page + 1}"))
    return text, types.InlineKeyboardMarkup(inline_keyboard=[btns]) if btns else None


async def event_panel_watcher(chat_id: int, msg_id: int, cmd_msg_id: int):
    while True:
        await asyncio.sleep(5)
        ttl = await redis.ttl(f"event_msg:{chat_id}:{msg_id}")
        if ttl <= 0:
            asyncio.create_task(delete_msgs_by_ids(chat_id, [msg_id, cmd_msg_id]))
            break


@router.message(CleanTextFilter(), Command("dice_event"))
async def cmd_event(message: types.Message):
    uid = str(message.from_user.id)
    text, markup = await get_event_page(0, uid)
    bot_msg = await message.reply(text, reply_markup=markup)
    if markup:
        await redis.setex(f"event_msg:{message.chat.id}:{bot_msg.message_id}", 60, "1")
        asyncio.create_task(event_panel_watcher(message.chat.id, bot_msg.message_id, message.message_id))
    else:
        asyncio.create_task(delete_msgs([message, bot_msg], 60))


@router.message(CleanTextFilter(), Command("dice_backup_db"))
async def cmd_backup_db(message: types.Message):
    if message.from_user.id != SUPER_ADMIN_ID:
        bot_msg = await message.reply("❌ 越权拦截")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))
    count = await perform_backup()
    bot_msg = await message.reply(f"✅ <b>手动备份完成！</b>\n当前 Redis 核心资产已全部写入 SQLite 物理数据库（共 {count} 条）。")
    asyncio.create_task(delete_msgs([message, bot_msg], 10))


@router.message(CleanTextFilter(), Command("dice_restore_db"))
async def cmd_restore_db(message: types.Message):
    if message.from_user.id != SUPER_ADMIN_ID:
        bot_msg = await message.reply("❌ 越权拦截")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))

    markup = types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text="⚠️ 确认覆盖恢复", callback_data="confirm_restore"),
        types.InlineKeyboardButton(text="❌ 取消", callback_data="cancel_restore")
    ]])
    await message.reply("⚠️ <b>高危操作警告</b> ⚠️\n\n此操作将清空并覆写当前 Redis 中的所有用户资产！\n确定要从 `backup.db` 恢复数据吗？", reply_markup=markup)


@router.message(CleanTextFilter(), Command("dice_checkin"))
async def cmd_checkin(message: types.Message):
    uid = str(message.from_user.id)
    today = datetime.datetime.now(TZ_BJ).strftime("%Y%m%d")
    yesterday = (datetime.datetime.now(TZ_BJ) - datetime.timedelta(days=1)).strftime("%Y%m%d")
    lock_key = f"checkin_lock:{uid}:{today}"
    if not await redis.set(lock_key, "1", nx=True, ex=86400):
        return await reply_and_auto_delete(message, "❌ 今日已签到过啦，明天再来吧！")
    last_date = await redis.hget(f"user_data:{uid}", "last_checkin")
    streak = int(await redis.hget(f"user_data:{uid}", "streak") or 0)
    streak = streak + 1 if last_date == yesterday else 1
    reward = random.randint(100, 1000)
    extra_msg = ""
    if streak % 5 == 0:
        reward += 20000
        extra_msg = "\n🎉 <b>达成5天连签，额外奖励 20000 积分！</b>"
        streak = 0
    new_bal = await update_balance(uid, reward)
    await redis.hset(f"user_data:{uid}", mapping={"last_checkin": today, "streak": str(streak)})
    await reply_and_auto_delete(message, f"📅 <b>签到成功！</b>\n获得积分：<b>{reward}</b>{extra_msg}\n当前余额：<b>{new_bal}</b>\n当前连签：{streak}天")


@router.message(CleanTextFilter(), Command("dice_redpack"))
async def cmd_redpack(message: types.Message):
    args = message.text.split()
    if len(args) < 3:
        return await reply_and_auto_delete(message, "❌ 用法：`/dice_redpack 总金额 个数`")
    uid = str(message.from_user.id)
    if not re.match(r"^\+?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$", args[1]):
        return await reply_and_auto_delete(message, "❌ 格式错误！请输入有效数字。")
    try:
        raw_amount = float(args[1])
        amount = round(raw_amount, 2)
        if amount != raw_amount:
            return await reply_and_auto_delete(message, "❌ 精度拦截！最多保留两位小数。")
    except ValueError:
        return await reply_and_auto_delete(message, "❌ 格式错误！请输入有效数字。")
    try:
        count = int(args[2])
    except ValueError:
        return await reply_and_auto_delete(message, "❌ 格式错误！请输入有效数字。")

    if amount <= 0 or amount > 200000:
        return await reply_and_auto_delete(message, "❌ 总金额必须在 0.01 到 200,000 之间。")
    if count <= 0 or count > 50:
        return await reply_and_auto_delete(message, "❌ 个数必须在 1 到 50 之间。")
    if amount / count < 0.01:
        return await reply_and_auto_delete(message, "❌ 均值过低！单个至少 0.01。")

    bal = await get_or_init_balance(uid)
    if bal < amount:
        return await reply_and_auto_delete(message, f"❌ <b>余额不足</b>\n需要 {amount}，你仅有 {bal}。")

    await update_balance(uid, -amount)
    rp_id = str(uuid.uuid4())[:8]
    amounts = generate_redpack_amounts(amount, count)

    epoch = str(time.time())
    await redis.hset(f"redpack_meta:{rp_id}", mapping={
        "amount": str(amount), "count": str(count), "chat_id": str(message.chat.id),
        "sender_uid": uid, "sender_name": message.from_user.first_name, "created_at": epoch
    })
    await redis.rpush(f"redpack_list:{rp_id}", *amounts)
    await redis.expire(f"redpack_meta:{rp_id}", 300)
    await redis.expire(f"redpack_list:{rp_id}", 300)

    text, markup = await build_redpack_panel(rp_id, is_pw=False)
    bot_msg = await bot.send_message(message.chat.id, text, reply_markup=markup, message_thread_id=ALLOWED_THREAD_ID or None)

    await redis.hset(f"redpack_meta:{rp_id}", "msg_id", str(bot_msg.message_id))

    # 0秒光速抹除老板发包指令
    asyncio.create_task(delete_msgs([message], 0))
    asyncio.create_task(redpack_expiry_watcher(message.chat.id, bot_msg.message_id, rp_id, False, epoch))


@router.message(CleanTextFilter(), Command("dice_redpack_pw"))
async def cmd_redpack_pw(message: types.Message):
    args = message.text.split(maxsplit=3)
    if len(args) < 4:
        return await reply_and_auto_delete(message, "❌ 用法：`/dice_redpack_pw 总额 个数 口令`")
    uid = str(message.from_user.id)
    if not re.match(r"^\+?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$", args[1]):
        return await reply_and_auto_delete(message, "❌ 格式错误！请输入有效数字。")
    try:
        raw_amount = float(args[1])
        amount = round(raw_amount, 2)
        if amount != raw_amount:
            return await reply_and_auto_delete(message, "❌ 精度拦截！最多保留两位小数。")
    except ValueError:
        return await reply_and_auto_delete(message, "❌ 格式错误！请输入有效数字。")
    try:
        count = int(args[2])
    except ValueError:
        return await reply_and_auto_delete(message, "❌ 格式错误！请输入有效数字。")

    pw = args[3].strip()

    if pw == "🎲":
        active_games = await redis.smembers(f"chat_games:{message.chat.id}")
        if active_games:
            return await reply_and_auto_delete(message, "❌ <b>口令冲突</b>\n当前群内有正在进行的对局，为防止干扰，禁止使用「🎲」作为红包口令！请换个口令或等对局结束。")

    if amount <= 0 or amount > 200000:
        return await reply_and_auto_delete(message, "❌ 总金额必须在 0.01 到 200,000 之间。")
    if count <= 0 or count > 50:
        return await reply_and_auto_delete(message, "❌ 个数必须在 1 到 50 之间。")
    if amount / count < 0.01:
        return await reply_and_auto_delete(message, "❌ 均值过低！单个至少 0.01。")

    bal = await get_or_init_balance(uid)
    if bal < amount:
        return await reply_and_auto_delete(message, f"❌ <b>余额不足</b>\n需要 {amount}，你仅有 {bal}。")

    await update_balance(uid, -amount)
    rp_id = str(uuid.uuid4())[:8]
    amounts = generate_redpack_amounts(amount, count)

    epoch = str(time.time())
    await redis.hset(f"redpack_meta:{rp_id}", mapping={
        "amount": str(amount), "count": str(count), "pw": pw, "chat_id": str(message.chat.id),
        "sender_uid": uid, "sender_name": message.from_user.first_name, "created_at": epoch
    })
    await redis.sadd("active_pw_rps", rp_id)
    await redis.rpush(f"redpack_list:{rp_id}", *amounts)
    await redis.expire(f"redpack_meta:{rp_id}", 320)
    await redis.expire(f"redpack_list:{rp_id}", 320)

    # 0秒光速抹除老板发包指令
    asyncio.create_task(delete_msgs([message], 0))

    if pw == "🎲":
        text, _ = await build_redpack_panel(rp_id, is_pw=True)
        bot_msg = await bot.send_message(message.chat.id, text, message_thread_id=ALLOWED_THREAD_ID or None)
        await redis.hset(f"redpack_meta:{rp_id}", "msg_id", str(bot_msg.message_id))
        try:
            await refresh_dice_panel(message.chat.id)
        except Exception as e:
            logging.warning(f"[redpack_pw] refresh_dice_panel 异常: {e}")
        asyncio.create_task(redpack_expiry_watcher(message.chat.id, bot_msg.message_id, rp_id, True, epoch))
    else:
        text, markup = await build_redpack_panel(rp_id, is_pw=True)
        bot_msg = await bot.send_message(message.chat.id, text, reply_markup=markup, message_thread_id=ALLOWED_THREAD_ID or None)
        await redis.hset(f"redpack_meta:{rp_id}", "msg_id", str(bot_msg.message_id))
        asyncio.create_task(redpack_expiry_watcher(message.chat.id, bot_msg.message_id, rp_id, True, epoch))


@router.message(CleanTextFilter(), Command("dice_rank"))
async def cmd_rank_daily(message: types.Message):
    uid = str(message.from_user.id)
    text = await get_leaderboard_text("daily", "gross", "今日胜负榜")
    bot_msg = await message.reply(text, reply_markup=get_rank_markup("daily", "gross", uid))
    await redis.setex(f"rank_msg:{message.chat.id}:{bot_msg.message_id}", 60, "1")
    asyncio.create_task(rank_panel_watcher(message.chat.id, bot_msg.message_id, message.message_id))


@router.message(CleanTextFilter(), Command("dice_rank_week"))
async def cmd_rank_weekly(message: types.Message):
    uid = str(message.from_user.id)
    text = await get_leaderboard_text("weekly", "gross", "本周胜负榜")
    bot_msg = await message.reply(text, reply_markup=get_rank_markup("weekly", "gross", uid))
    await redis.setex(f"rank_msg:{message.chat.id}:{bot_msg.message_id}", 60, "1")
    asyncio.create_task(rank_panel_watcher(message.chat.id, bot_msg.message_id, message.message_id))


@router.message(CleanTextFilter(), Command("dice_rank_month"))
async def cmd_rank_monthly(message: types.Message):
    uid = str(message.from_user.id)
    text = await get_leaderboard_text("monthly", "gross", "本月胜负榜")
    bot_msg = await message.reply(text, reply_markup=get_rank_markup("monthly", "gross", uid))
    await redis.setex(f"rank_msg:{message.chat.id}:{bot_msg.message_id}", 60, "1")
    asyncio.create_task(rank_panel_watcher(message.chat.id, bot_msg.message_id, message.message_id))


@router.message(CleanTextFilter(), Command("dice_bal"))
async def check_balance(message: types.Message):
    uid = str(message.from_user.id)
    bal = await get_or_init_balance(uid)
    _, _, monthly_k = get_period_keys()
    wins = float(await redis.zscore(f"rank_wins:monthly:{monthly_k}", uid) or 0)
    losses = float(await redis.zscore(f"rank_losses:monthly:{monthly_k}", uid) or 0)
    total_games = int(wins + losses)
    if total_games > 0:
        win_rate = wins / total_games * 100
        rate_line = f"\n📊 本月胜率：<b>{win_rate:.1f}%</b>（{int(wins)}胜 {int(losses)}负 / 共{total_games}局）"
    else:
        rate_line = "\n📊 本月胜率：暂无对局记录"
    bot_msg = await message.reply(f"💰 当前可用积分为：<b>{bal}</b>{rate_line}")
    asyncio.create_task(delete_msgs([message, bot_msg], 10))


@router.message(CleanTextFilter(), Command("dice_gift"))
async def cmd_gift(message: types.Message):
    args = message.text.split()
    if len(args) < 2 or not message.reply_to_message:
        return await reply_and_auto_delete(message, "❌ 用法：回复玩家并输入 `/dice_gift 数量`")
    if not re.match(r"^\+?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$", args[1]):
        return await reply_and_auto_delete(message, "❌ 格式错误！请输入有效数字。")
    try:
        raw_amount = float(args[1])
        amount = round(raw_amount, 2)
        if amount != raw_amount:
            return await reply_and_auto_delete(message, "❌ 精度拦截！最多保留两位小数。")
    except ValueError:
        return await reply_and_auto_delete(message, "❌ 格式错误！请输入有效数字。")
    if amount <= 0 or amount > 200000:
        return await reply_and_auto_delete(message, f"❌ 赠送金额必须在 0.01 到 200,000 之间。")

    sender_uid = str(message.from_user.id)
    target_uid = str(message.reply_to_message.from_user.id)
    if sender_uid == target_uid:
        return await reply_and_auto_delete(message, "❌ 禁止自娱自乐‼️")
    if target_uid == str(BOT_ID) or message.reply_to_message.from_user.is_bot:
        sender_bal = await get_or_init_balance(sender_uid)
        deduct = min(amount, round(sender_bal, 2))
        if deduct > 0:
            await update_balance(sender_uid, -deduct)
        bot_msg = await message.reply(f"❌ 禁止贿赂荷官！礼品已没收，扣除 <b>{deduct}</b> 积分🤫")
        asyncio.create_task(delete_msgs([message, bot_msg], 10))
        return

    sender_bal = await get_or_init_balance(sender_uid)
    if sender_bal < amount:
        return await reply_and_auto_delete(message, f"❌ <b>余额不足</b>\n需要 {amount}，你仅有 {sender_bal}。")

    await update_balance(sender_uid, -amount)
    await update_balance(target_uid, amount)
    bot_msg = await message.reply(f"🎁 成功赠送给 {safe_html(message.reply_to_message.from_user.first_name)} <b>{amount}</b> 积分。")
    asyncio.create_task(delete_msgs([message, bot_msg], 10))


@router.message(CleanTextFilter(), Command("dice_forced_stop"))
async def force_stop_game(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        bot_msg = await message.reply("❌ 越权拦截")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))

    active_games = await redis.smembers(f"chat_games:{message.chat.id}")
    if not active_games:
        bot_msg = await message.reply("⚠️ 当前群组没有正在进行的对局。")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))

    for gid in active_games:
        g_data = await redis.hgetall(f"game:{gid}")
        if g_data:
            mid = g_data.get("init_msg_id")
            cid = g_data.get("cmd_msg_id")
            if mid:
                try:
                    await bot.edit_message_reply_markup(message.chat.id, int(mid), reply_markup=None)
                except:
                    pass
                asyncio.create_task(delete_msg_by_id(message.chat.id, int(mid)))
            if cid:
                asyncio.create_task(delete_msg_by_id(message.chat.id, int(cid)))
        await refund_game(message.chat.id, gid)

    bot_msg = await message.reply("🛑 <b>管理员已强杀当前群组异常对局，押金退还！</b>")
    asyncio.create_task(delete_msgs([message, bot_msg], 10))


# ==============================
# 后台调账指令 (回复匹配)
# ==============================

@router.message(Command("dice_let"), F.reply_to_message)
async def admin_set_balance(message: types.Message):
    if message.from_user.id != SUPER_ADMIN_ID:
        bot_msg = await message.reply("❌ 越权拦截")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot_msg = await message.reply("❌ 用法：回复目标消息 /dice_let 数字")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))
    try:
        amount = round(float(parts[1]), 2)
    except ValueError:
        bot_msg = await message.reply("❌ 格式错误！请输入有效数字。")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))

    target_uid = str(message.reply_to_message.from_user.id)
    if target_uid == str(BOT_ID) or message.reply_to_message.from_user.is_bot:
        bot_msg = await message.reply("❌ 禁止贿赂荷官🤫")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))

    await redis.set(f"user_balance:{target_uid}", amount)
    bot_msg = await message.reply(f"👑 <b>系统调账 (覆写)</b>\n已将该玩家的积分强制设为：<b>{amount}</b>")
    asyncio.create_task(delete_msgs([message, bot_msg], 10))


@router.message(Command("dice_give"), F.reply_to_message)
async def admin_give_balance(message: types.Message):
    if message.from_user.id != SUPER_ADMIN_ID:
        bot_msg = await message.reply("❌ 越权拦截")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot_msg = await message.reply("❌ 用法：回复目标消息 /dice_give 数字")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))
    try:
        amount = round(float(parts[1]), 2)
    except ValueError:
        bot_msg = await message.reply("❌ 格式错误！请输入有效数字。")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))
    if amount <= 0:
        bot_msg = await message.reply("❌ 数字必须大于0")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))

    target_uid = str(message.reply_to_message.from_user.id)
    if target_uid == str(BOT_ID) or message.reply_to_message.from_user.is_bot:
        bot_msg = await message.reply("❌ 禁止贿赂荷官🤫")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))

    await update_balance(target_uid, amount)
    bot_msg = await message.reply(f"👑 <b>系统调账</b> +{amount:g} 已完成。")
    asyncio.create_task(delete_msgs([message, bot_msg], 10))


@router.message(Command("dice_take"), F.reply_to_message)
async def admin_take_balance(message: types.Message):
    if message.from_user.id != SUPER_ADMIN_ID:
        bot_msg = await message.reply("❌ 越权拦截")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot_msg = await message.reply("❌ 用法：回复目标消息 /dice_take 数字")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))
    try:
        amount = round(float(parts[1]), 2)
    except ValueError:
        bot_msg = await message.reply("❌ 格式错误！请输入有效数字。")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))
    if amount <= 0:
        bot_msg = await message.reply("❌ 数字必须大于0")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))

    target_uid = str(message.reply_to_message.from_user.id)
    if target_uid == str(BOT_ID) or message.reply_to_message.from_user.is_bot:
        bot_msg = await message.reply("❌ 禁止贿赂荷官🤫")
        return asyncio.create_task(delete_msgs([message, bot_msg], 10))

    await update_balance(target_uid, -amount)
    bot_msg = await message.reply(f"👑 <b>系统调账</b> -{amount:g} 已完成。")
    asyncio.create_task(delete_msgs([message, bot_msg], 10))


# ==============================
# 发车对局核心指令
# ==============================

@router.message(CleanTextFilter(), F.text.regexp(PATTERN))
async def handle_bet_command(message: types.Message):
    match = PATTERN.match(message.text)
    if not match:
        return
    uid = str(message.from_user.id)

    if message.chat.type in ['group', 'supergroup']:
        await redis.sadd("active_groups", str(message.chat.id))
        today_str = datetime.datetime.now(TZ_BJ).strftime("%Y%m%d")
        await redis.zincrby(f"rank_init:daily:{today_str}", 1, uid)
        await redis.expire(f"rank_init:daily:{today_str}", 86400 * 7)

    if await redis.exists(f"user_game:{uid}"):
        return await reply_and_auto_delete(message, "❌ <b>分身乏术</b>\n请结算后再开启新局。")

    try:
        raw_amount = float(match.group(2))
        amount = round(raw_amount, 2)
        if amount != raw_amount:
            return await reply_and_auto_delete(message, "❌ 精度拦截！最多保留两位小数。")
        dice_count = int(match.group(3)) if match.group(3) else 1
    except ValueError:
        return await reply_and_auto_delete(message, "❌ 格式错误！请输入有效数字。")

    if amount < 0 or amount > 40000:
        return await reply_and_auto_delete(message, "❌ 额度拦截！单局下注金额必须在 0 到 40,000 之间。负数被禁止。")
    if not (1 <= dice_count <= 5):
        return await reply_and_auto_delete(message, "❌ 规则不符！骰子数量必须在 1-5 颗之间。")

    direction = match.group(1)
    is_multi = bool(match.group(4))
    target_players_str = match.group(5)

    target_players = 2
    is_exact = False
    if is_multi:
        if target_players_str:
            target_players = int(target_players_str)
            if not (3 <= target_players <= 5):
                return await reply_and_auto_delete(message, "❌ 指定发车人数必须在 3-5 之间。")
            is_exact = True
        else:
            target_players = 5

    target_uid = ""
    target_name = ""
    if message.reply_to_message and not is_multi and not (
            message.message_thread_id and
            message.reply_to_message.message_id == message.message_thread_id):
        target_uid = str(message.reply_to_message.from_user.id)
        target_name = message.reply_to_message.from_user.first_name
        if target_uid == uid:
            return await reply_and_auto_delete(message, "❌ 禁止自娱自乐‼️")
        if target_uid == str(BOT_ID) or message.reply_to_message.from_user.is_bot:
            return await reply_and_auto_delete(message, "❌ 禁止与荷官谈笑风生👀")
        if await redis.exists(f"user_game:{target_uid}"):
            return await reply_and_auto_delete(message, "❌ <b>对方正在对局中</b>\n等对方结算后再发起挑战。")

    waiting_duels = []
    active_games = await redis.smembers(f"chat_games:{message.chat.id}")
    for gid in active_games:
        g_data = await redis.hgetall(f"game:{gid}")
        if g_data and g_data.get("status") == "waiting_join" and g_data.get("game_mode") == "targeted" and g_data.get("target_uid") == uid:
            waiting_duels.append((gid, g_data))

    if waiting_duels:
        duel_gid, duel_data = waiting_duels[0]
        initiator = json.loads(duel_data["players"])[0]
        names = json.loads(duel_data.get("names", "{}"))
        initiator_name = names.get(initiator, "对方")

        pending_key = f"pending_bet:{uid}"
        pending_data = {
            "direction": direction, "amount": str(amount), "dice_count": str(dice_count),
            "is_multi": is_multi, "is_exact": is_exact, "target_players": str(target_players),
            "target_uid": target_uid, "target_name": target_name
        }
        await redis.setex(pending_key, 60, json.dumps(pending_data))

        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="🆕 开新局", callback_data=f"d_new:{uid}"),
            types.InlineKeyboardButton(text="⚔️ 接决斗", callback_data=f"jg:{duel_gid}")
        ]])
        bot_msg = await message.reply(f"⚠️ <b>{safe_html(initiator_name)}</b> 正在向你发起决斗！\n你要无视对方开新局，还是接下决斗？", reply_markup=kb)
        asyncio.create_task(delete_msgs([message, bot_msg], 60))
        return

    pending_data = {
        "direction": direction, "amount": str(amount), "dice_count": str(dice_count),
        "is_multi": is_multi, "is_exact": is_exact, "target_players": str(target_players),
        "target_uid": target_uid, "target_name": target_name
    }

    asyncio.create_task(delete_msgs([message], 0))
    await start_game_creation(message.chat.id, uid, message.from_user.first_name, pending_data)


# ==============================
# Callback handlers
# ==============================

@router.callback_query(F.data == "confirm_restore")
async def handle_confirm_restore_cb(callback: types.CallbackQuery):
    try:
        await callback.answer()
    except:
        pass
    if callback.from_user.id != SUPER_ADMIN_ID:
        try:
            await callback.answer("❌ 越权拦截", show_alert=True)
        except:
            pass
        return

    try:
        await callback.message.edit_text("⏳ 正在从 SQLite 恢复数据...")
    except:
        pass

    def db_read():
        try:
            conn = sqlite3.connect("backup.db")
            c = conn.cursor()
            c.execute("SELECT uid, balance, name, last_checkin, streak FROM users")
            rows = c.fetchall()
            conn.close()
            return rows
        except Exception as e:
            return str(e)

    rows = await asyncio.to_thread(db_read)
    if isinstance(rows, str):
        try:
            await callback.message.edit_text(f"❌ 读取异常：{rows}")
        except:
            pass
        return

    if not rows:
        try:
            await callback.message.edit_text("⚠️ 备份数据库为空，无法恢复！")
        except:
            pass
        return

    for uid, bal, name, last_checkin, streak in rows:
        await redis.set(f"user_balance:{uid}", bal)
        await redis.hset("user_names", uid, name)
        if last_checkin or streak:
            await redis.hset(f"user_data:{uid}", mapping={"last_checkin": last_checkin, "streak": str(streak)})

    try:
        await callback.message.edit_text(f"✅ <b>系统恢复成功！</b>\n已恢复 <b>{len(rows)}</b> 个用户的核心资产。")
    except:
        pass
    asyncio.create_task(delete_msgs([callback.message], 10))


@router.callback_query(F.data == "cancel_restore")
async def handle_cancel_restore_cb(callback: types.CallbackQuery):
    try:
        await callback.answer()
    except:
        pass
    if callback.from_user.id != SUPER_ADMIN_ID:
        try:
            await callback.answer("❌ 越权拦截", show_alert=True)
        except:
            pass
        return
    try:
        await callback.message.edit_text("✅ 恢复操作已取消。")
    except:
        pass
    asyncio.create_task(delete_msgs([callback.message], 10))


@router.callback_query(F.data.startswith("rank_sw:"))
async def handle_rank_switch_cb(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    period = parts[1]
    board = parts[2]
    target_uid = parts[3] if len(parts) > 3 else ""

    if target_uid and str(callback.from_user.id) != target_uid:
        return await callback.answer("⚠️ 只有唤起该榜单的人可以切换！", show_alert=True)

    await redis.expire(f"rank_msg:{callback.message.chat.id}:{callback.message.message_id}", 60)

    title_map = {"daily": "今日", "weekly": "本周", "monthly": "本月"}
    title = title_map[period] + ("净胜负榜" if board == "net" else "胜负榜")
    text = await get_leaderboard_text(period, board, title)
    markup = get_rank_markup(period, board, target_uid)
    try:
        await callback.message.edit_text(text, reply_markup=markup)
    except:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("fs:"))
async def handle_force_start(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    game_id = parts[1]
    initiator_uid = parts[2]

    if str(callback.from_user.id) != initiator_uid:
        return await callback.answer("⚠️ 只有发起人可以强行发车！", show_alert=True)

    game_key = f"game:{game_id}"
    async with get_lock(game_id):
        game_data = await redis.hgetall(game_key)
        if not game_data or game_data.get("status") != "waiting_join":
            return await callback.answer("⚠️ 对局已开启、结束或不存在。", show_alert=True)

        players = json.loads(game_data["players"])
        if len(players) < 2:
            return await callback.answer("⚠️ 至少需要 2 人才能发车！", show_alert=True)
        await redis.hset(game_key, "status", "starting")

    try:
        await callback.message.delete()
    except:
        pass
    game_data = await redis.hgetall(game_key)
    chat_id = int(game_data.get("chat_id") or callback.message.chat.id)
    await start_rolling_phase(chat_id, game_id, game_data)


@router.callback_query(F.data.startswith("d_new:"))
async def handle_duel_new(callback: types.CallbackQuery):
    uid = str(callback.from_user.id)
    if uid != callback.data.split(":")[1]:
        return await callback.answer("⚠️ 还没轮到你操作！", show_alert=True)
    pending_key = f"pending_bet:{uid}"
    pending_data_str = await redis.get(pending_key)
    if not pending_data_str:
        return await callback.answer("⚠️ 操作已过期", show_alert=True)
    await redis.delete(pending_key)
    try:
        await callback.message.delete()
    except:
        pass
    data = json.loads(pending_data_str)
    await start_game_creation(callback.message.chat.id, uid, callback.from_user.first_name, data)


@router.callback_query(F.data.startswith("jg:"))
async def handle_join(callback: types.CallbackQuery):
    game_id = callback.data.split(":")[1]
    game_key = f"game:{game_id}"
    uid = str(callback.from_user.id)

    if await redis.exists(f"user_game:{uid}"):
        return await callback.answer("已有进行中对局！", show_alert=True)

    async with get_lock(game_id):
        game_data = await redis.hgetall(game_key)
        if not game_data or game_data.get("status") != "waiting_join":
            return await callback.answer("⚠️ 对局已开启、结束或不存在。", show_alert=True)

        players = json.loads(game_data["players"])
        names = json.loads(game_data["names"])
        game_mode = game_data.get("game_mode")
        amount = float(game_data["amount"])
        target_players = int(game_data.get("target_players", 5))

        if uid in players:
            return await callback.answer("你已在局内！", show_alert=True)
        if game_mode == "targeted" and uid != game_data.get("target_uid"):
            return await callback.answer("这是专属决斗！", show_alert=True)

        bal = await get_or_init_balance(uid)
        if bal < amount:
            return await callback.answer(f"❌ 余额不足\n需要 {amount}，你仅有 {bal}。", show_alert=True)

        if amount > 0:
            await update_balance(uid, -amount)

        await redis.set(f"user_game:{uid}", game_id)
        players.append(uid)
        names[uid] = callback.from_user.first_name
        await redis.hset(game_key, "players", json.dumps(players))
        await redis.hset(game_key, "names", json.dumps(names))

        is_full = False
        if game_mode in ["single", "targeted"]:
            is_full = True
        elif game_mode == "multi_exact" and len(players) == target_players:
            is_full = True
        elif game_mode == "multi_dynamic" and len(players) == 5:
            is_full = True

        if is_full:
            await redis.hset(game_key, "status", "starting")

    if is_full:
        try:
            await callback.message.delete()
        except:
            pass
        fresh_game_data = await redis.hgetall(game_key)
        chat_id = int(fresh_game_data.get("chat_id") or callback.message.chat.id)
        await start_rolling_phase(chat_id, game_id, fresh_game_data)
    else:
        player_list_str = "、".join([get_mention(p, names[p]) for p in players])
        keys = [[types.InlineKeyboardButton(text="接单", callback_data=f"jg:{game_id}")]]
        _dir = game_data.get("direction", "?")
        _amt = float(game_data["amount"])
        _dc = game_data.get("dice_count", "1")

        if game_mode == "multi_exact":
            keys.append([types.InlineKeyboardButton(text="🚀 发起人强行发车", callback_data=f"fs:{game_id}:{players[0]}")])
            txt = (f"🎲 <b>定员组局 ({len(players)}/{target_players})</b>\n"
                   f"押注：<b>{_amt:g}</b> | 骰子：<b>{_dc}</b>颗 | 比<b>{_dir}</b>\n"
                   f"当前：{player_list_str}\n死等满员👇")
        else:
            await redis.hset(game_key, "join_deadline", str(time.time() + 15))
            txt = (f"🎲 <b>多人发车 ({len(players)}/5)</b>\n"
                   f"押注：<b>{_amt:g}</b> | 骰子：<b>{_dc}</b>颗 | 比<b>{_dir}</b>\n"
                   f"当前：{player_list_str}\n15秒无人进则开局👇")

        try:
            await callback.message.edit_text(txt, reply_markup=types.InlineKeyboardMarkup(inline_keyboard=keys))
        except:
            pass


@router.callback_query(F.data.startswith("grab_rp:"))
async def handle_grab_rp(callback: types.CallbackQuery):
    rp_id = callback.data.split(":")[1]
    uid = str(callback.from_user.id)
    rp_key = f"redpack_meta:{rp_id}"
    list_key = f"redpack_list:{rp_id}"

    if not await redis.exists(rp_key):
        return await callback.answer("已过期", show_alert=True)
    if await redis.hget(f"redpack_users:{rp_id}", uid):
        return await callback.answer("抢过了！", show_alert=True)

    amt_str = await redis.lpop(list_key)
    if not amt_str:
        return await callback.answer("抢光了！", show_alert=True)

    amt = float(amt_str)
    await redis.hset(f"redpack_users:{rp_id}", uid, f"{callback.from_user.first_name}|{amt}")
    await update_balance(uid, amt)
    await callback.answer(f"抢到 {amt} 积分！", show_alert=True)

    text, markup = await build_redpack_panel(rp_id, is_pw=False)
    try:
        await callback.message.edit_text(text, reply_markup=markup)
    except:
        pass

    meta = await redis.hgetall(rp_key)
    users_data = await redis.hgetall(f"redpack_users:{rp_id}")
    sender_uid = meta.get("sender_uid")
    sender_name = meta.get("sender_name", "某人")
    sender_mention = get_mention(sender_uid, sender_name) if sender_uid else safe_html(sender_name)

    announce_msg = await bot.send_message(callback.message.chat.id, f"🎉 {get_mention(uid, callback.from_user.first_name)} 领取了 {sender_mention} 的拼手气红包，获得 <b>{amt}</b> 积分！", message_thread_id=ALLOWED_THREAD_ID or None)
    asyncio.create_task(delete_msgs([announce_msg], 10))

    if len(users_data) >= int(meta.get('count', 0)):
        if meta.get('msg_id'):
            asyncio.create_task(delete_msg_by_id(callback.message.chat.id, int(meta['msg_id']), delay=60))


@router.callback_query(F.data.startswith("ev_p:"))
async def handle_event_page_cb(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) < 3:
        return await callback.answer()
    initiator_uid, page = parts[1], int(parts[2])
    if str(callback.from_user.id) != initiator_uid:
        return await callback.answer("⚠️ 只有查询者可以翻页！", show_alert=True)
    chat_id, msg_id = callback.message.chat.id, callback.message.message_id
    ttl = await redis.ttl(f"event_msg:{chat_id}:{msg_id}")
    if ttl <= 0:
        try:
            await callback.message.delete()
        except Exception:
            pass
        return await callback.answer("⏰ 面板已过期", show_alert=True)
    await redis.expire(f"event_msg:{chat_id}:{msg_id}", 60)
    text, markup = await get_event_page(page, initiator_uid)
    try:
        await callback.message.edit_text(text, reply_markup=markup)
    except Exception:
        pass
    await callback.answer()


@router.callback_query(F.data.startswith("r1:") | F.data.startswith("ra:"))
async def handle_roll_button(callback: types.CallbackQuery):
    parts = callback.data.split(":")
    action = parts[0]
    game_id = parts[1]
    target_uid = parts[2] if len(parts) > 2 else ""

    uid = str(callback.from_user.id)
    if target_uid and uid != target_uid:
        return await callback.answer("⚠️ 这不是你的专属投掷按钮！", show_alert=True)

    game_key = f"game:{game_id}"
    game_data = await redis.hgetall(game_key)
    if not game_data:
        return await callback.answer("⚠️ 对局已开启、结束或不存在。", show_alert=True)

    chat_id = int(game_data.get("chat_id") or callback.message.chat.id)

    status = game_data.get("status")
    target_lengths = json.loads(game_data.get("target_lengths", "{}"))
    rolls = json.loads(game_data.get("rolls", "{}"))

    target = target_lengths.get(uid, 0)
    current_count = len(rolls.get(uid, []))

    if current_count >= target:
        return await callback.answer("✅ 你已经投完了！", show_alert=True)

    current_roller = None
    if status == "rolling":
        queue = json.loads(game_data.get("queue", "[]"))
        if queue:
            current_roller = queue[0]
    elif status == "tie_break":
        tie_queue = json.loads(game_data.get("tie_queue", "[]"))
        g_idx = int(game_data.get("current_tie_group", "0"))
        t_idx = int(game_data.get("current_turn", "0"))
        if g_idx < len(tie_queue) and t_idx < len(tie_queue[g_idx]):
            current_roller = tie_queue[g_idx][t_idx]
    else:
        return await callback.answer("⚠️ 对局已开启、结束或不存在。", show_alert=True)

    if uid != current_roller:
        return await callback.answer("⚠️ 还没轮到你投掷！", show_alert=True)

    pending = await redis.hincrby(game_key, f"pending_{uid}", 1)
    roll_count = 1
    rem = target - current_count

    if action == "ra":
        if pending > 1:
            await redis.hincrby(game_key, f"pending_{uid}", -1)
            return await callback.answer("⚠️ 点击过快，防止超投！", show_alert=True)
        await redis.hincrby(game_key, f"pending_{uid}", rem - 1)
        roll_count = rem
        pending = rem

    if current_count + pending > target:
        await redis.hincrby(game_key, f"pending_{uid}", -1)
        return await callback.answer("⚠️ 点击过快，防止超投！", show_alert=True)

    if current_count + pending >= target:
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except:
            pass

    await callback.answer(f"准备投 {roll_count} 颗...")

    for i in range(roll_count):
        fresh_data = await redis.hgetall(game_key)
        if not fresh_data:
            break

        fresh_status = fresh_data.get("status")
        fresh_rolls = json.loads(fresh_data.get("rolls", "{}")).get(uid, [])
        fresh_target = json.loads(fresh_data.get("target_lengths", "{}")).get(uid, 0)

        is_my_turn = False
        if fresh_status == "rolling":
            queue = json.loads(fresh_data.get("queue", "[]"))
            if queue and queue[0] == uid:
                is_my_turn = True
        elif fresh_status == "tie_break":
            tie_queue = json.loads(fresh_data.get("tie_queue", "[]"))
            g_idx = int(fresh_data.get("current_tie_group", "0"))
            t_idx = int(fresh_data.get("current_turn", "0"))
            if g_idx < len(tie_queue) and t_idx < len(tie_queue[g_idx]) and tie_queue[g_idx][t_idx] == uid:
                is_my_turn = True

        if len(fresh_rolls) >= fresh_target or not is_my_turn:
            cancel_amount = roll_count - i
            if await redis.exists(game_key):
                await redis.hincrby(game_key, f"pending_{uid}", -cancel_amount)
            break

        try:
            dice_msg = await bot.send_dice(chat_id=chat_id, emoji="🎲", message_thread_id=ALLOWED_THREAD_ID or None)
            await asyncio.sleep(2.5)
        except Exception:
            cancel_amount = roll_count - i
            if await redis.exists(game_key):
                await redis.hincrby(game_key, f"pending_{uid}", -cancel_amount)
            break

        if await redis.exists(game_key):
            await redis.hincrby(game_key, f"pending_{uid}", -1)
            await process_dice_value(chat_id, game_id, uid, dice_msg.dice.value, dice_msg.message_id)



# ==============================
# /dice_attack 对决系统
# ==============================

ATTACK_BET = 1000
ATTACK_MAX = 20000


def _attack_active_text(c_uid, c_name, d_uid, d_name, c_total, d_total):
    c_m = get_mention(c_uid, c_name)
    d_m = get_mention(d_uid, d_name)
    return (
        f"⚔️ {c_m} 向 {d_m} 发起了 <b>Attack！</b>\n\n"
        f"💥 {c_m}：已投入 <b>{int(c_total)}</b> 积分\n"
        f"🛡 {d_m}：已投入 <b>{int(d_total)}</b> 积分\n\n"
        f"⏱ 1分钟内可持续追加，时间到自动结算"
    )


def _attack_markup(attack_id):
    return types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text="💥 加大力度 (+1000)", callback_data=f"atk_c:{attack_id}"),
        types.InlineKeyboardButton(text="🛡 回手反击 (+1000)", callback_data=f"atk_d:{attack_id}")
    ]])


async def _attack_watcher(chat_id: int, attack_id: str, msg_id: int):
    await asyncio.sleep(61)
    key = f"attack:{attack_id}"
    try:
        # 原子抢占结算权，防止极端情况下重入
        won_lock = await redis.hsetnx(key, "settled", "1")
        if not won_lock:
            return

        data = await redis.hgetall(key)
        if not data:
            return

        await redis.hset(key, "status", "ended")

        c_uid = data["challenger_uid"]
        c_name = data["challenger_name"]
        d_uid = data["defender_uid"]
        d_name = data["defender_name"]
        c_total = float(data.get("challenger_total", ATTACK_BET))
        d_total = float(data.get("defender_total", 0))

        # 清除进行中标记
        await redis.delete(f"active_attack_by:{c_uid}", f"active_attack_target:{d_uid}")

        # 删除原面板
        try:
            await bot.delete_message(chat_id, msg_id)
        except Exception:
            pass

        c_m = get_mention(c_uid, c_name)
        d_m = get_mention(d_uid, d_name)

        if d_total == 0:
            # 防守方始终未回应 → 全额退款，@挑战方通知
            await update_balance(c_uid, c_total)
            notif = await bot.send_message(
                chat_id,
                f"⚔️ {c_m}，你向 {d_m} 发起的攻击无人应战，已全额退回 <b>{int(c_total)}</b> 积分。",
                message_thread_id=ALLOWED_THREAD_ID or None
            )
            asyncio.create_task(delete_msgs([notif], 30))
            await redis.delete(key)
            return

        # 加权随机决定胜负
        total = c_total + d_total
        challenger_wins = random.uniform(0, total) < c_total
        w_uid = c_uid if challenger_wins else d_uid
        w_name = c_name if challenger_wins else d_name
        winner_invested = c_total if challenger_wins else d_total

        loser_invested = total - winner_invested
        captured = int(loser_invested * 0.9)  # 缴获对方90%，10%销毁防刷
        payout = int(winner_invested) + captured
        await update_balance(w_uid, payout)

        w_m = get_mention(w_uid, w_name)
        result = (
            f"⚔️ <b>Attack 结算！</b>\n"
            f"发起方：{c_m}  vs  迎战方：{d_m}\n\n"
            f"💥 {c_m}：共投入 <b>{int(c_total)}</b> 积分\n"
            f"🛡 {d_m}：共投入 <b>{int(d_total)}</b> 积分\n\n"
            f"🏆 {w_m} <b>获胜！</b>\n"
            f"本金 <b>{int(winner_invested)}</b> + 缴获 <b>{captured}</b> = 共得 <b>{payout}</b> 积分"
        )
        await bot.send_message(chat_id, result, message_thread_id=ALLOWED_THREAD_ID or None)
        await redis.expire(key, 3600)

    except Exception as e:
        logging.warning(f"[attack_watcher] 结算异常 attack_id={attack_id}: {e}")


@router.message(CleanTextFilter(), Command("dice_attack"))
async def cmd_attack(message: types.Message):
    if not message.reply_to_message:
        return await reply_and_auto_delete(message, "❌ 用法：回复某人的消息并发送 /dice_attack")

    c_uid = str(message.from_user.id)
    c_name = message.from_user.first_name
    defender = message.reply_to_message.from_user
    d_uid = str(defender.id)
    d_name = defender.first_name

    if c_uid == d_uid:
        return await reply_and_auto_delete(message, "❌ 禁止自娱自乐‼️")
    if d_uid == str(BOT_ID) or defender.is_bot:
        penalty = random.randint(200, 2000)
        bal = await get_or_init_balance(c_uid)
        actual_penalty = min(penalty, int(bal))
        if actual_penalty > 0:
            await update_balance(c_uid, -actual_penalty)
        bot_msg = await bot.send_message(
            message.chat.id,
            f"❌ <b>{safe_html(c_name)}</b> 恶意攻击荷官，扣除 <b>{actual_penalty}</b> 积分 🔨",
            message_thread_id=ALLOWED_THREAD_ID or None
        )
        asyncio.create_task(delete_msgs([message, bot_msg], 15))
        return
    if await redis.exists(f"active_attack_by:{c_uid}"):
        return await reply_and_auto_delete(message, "❌ 你已有一场进行中的 Attack，请等结束后再发起！")
    if await redis.exists(f"active_attack_target:{d_uid}"):
        return await reply_and_auto_delete(message, f"❌ {safe_html(d_name)} 已在一场 Attack 中，请稍后再挑战！")

    bal = await get_or_init_balance(c_uid)
    if bal < ATTACK_BET:
        return await reply_and_auto_delete(message, f"❌ 余额不足！发起攻击需要 {ATTACK_BET} 积分，你仅有 {bal}。")

    await update_balance(c_uid, -ATTACK_BET)

    attack_id = str(uuid.uuid4())[:8]
    chat_id = message.chat.id
    await redis.hset(f"attack:{attack_id}", mapping={
        "challenger_uid": c_uid,
        "challenger_name": c_name,
        "defender_uid": d_uid,
        "defender_name": d_name,
        "chat_id": str(chat_id),
        "challenger_total": str(float(ATTACK_BET)),
        "defender_total": "0",
        "status": "active",
        "created_at": str(time.time()),
    })
    await redis.expire(f"attack:{attack_id}", 300)
    await redis.setex(f"active_attack_by:{c_uid}", 300, attack_id)
    await redis.setex(f"active_attack_target:{d_uid}", 300, attack_id)

    asyncio.create_task(delete_msgs([message], 0))
    text = _attack_active_text(c_uid, c_name, d_uid, d_name, ATTACK_BET, 0)
    panel = await bot.send_message(message.chat.id, text, reply_markup=_attack_markup(attack_id), message_thread_id=ALLOWED_THREAD_ID or None)
    await redis.hset(f"attack:{attack_id}", "msg_id", str(panel.message_id))
    asyncio.create_task(_attack_watcher(chat_id, attack_id, panel.message_id))


@router.callback_query(F.data.startswith("atk_c:"))
async def handle_attack_challenger(callback: types.CallbackQuery):
    attack_id = callback.data.split(":")[1]
    uid = str(callback.from_user.id)
    key = f"attack:{attack_id}"

    data = await redis.hgetall(key)
    if not data or data.get("status") != "active" or data.get("settled"):
        return await callback.answer("⚠️ 这场 Attack 已结束！", show_alert=True)
    if uid != data["challenger_uid"]:
        return await callback.answer("⚠️ 只有发起方可以加大力度！", show_alert=True)

    c_total = float(data.get("challenger_total", ATTACK_BET))
    if c_total >= ATTACK_MAX:
        return await callback.answer(f"⚠️ 已达到最高投入上限 {ATTACK_MAX} 积分！", show_alert=True)

    bal = await get_or_init_balance(uid)
    if bal < ATTACK_BET:
        return await callback.answer(f"❌ 余额不足，需要 {ATTACK_BET} 积分，你仅有 {bal}。", show_alert=True)

    await update_balance(uid, -ATTACK_BET)
    new_c = float(await redis.hincrbyfloat(key, "challenger_total", ATTACK_BET))
    d_total = float(await redis.hget(key, "defender_total") or 0)

    text = _attack_active_text(data["challenger_uid"], data["challenger_name"],
                               data["defender_uid"], data["defender_name"],
                               new_c, d_total)
    try:
        await callback.message.edit_text(text, reply_markup=_attack_markup(attack_id))
    except Exception:
        pass
    await callback.answer(f"💥 已追加 {ATTACK_BET}！你的总投入：{int(new_c)}")


@router.callback_query(F.data.startswith("atk_d:"))
async def handle_attack_defender(callback: types.CallbackQuery):
    attack_id = callback.data.split(":")[1]
    uid = str(callback.from_user.id)
    key = f"attack:{attack_id}"

    data = await redis.hgetall(key)
    if not data or data.get("status") != "active" or data.get("settled"):
        return await callback.answer("⚠️ 这场 Attack 已结束！", show_alert=True)
    if uid != data["defender_uid"]:
        return await callback.answer("⚠️ 只有迎战方可以回手反击！", show_alert=True)

    d_total = float(data.get("defender_total", 0))
    if d_total >= ATTACK_MAX:
        return await callback.answer(f"⚠️ 已达到最高投入上限 {ATTACK_MAX} 积分！", show_alert=True)

    bal = await get_or_init_balance(uid)
    if bal < ATTACK_BET:
        return await callback.answer(f"❌ 余额不足，需要 {ATTACK_BET} 积分，你仅有 {bal}。", show_alert=True)

    await update_balance(uid, -ATTACK_BET)
    new_d = float(await redis.hincrbyfloat(key, "defender_total", ATTACK_BET))
    c_total = float(await redis.hget(key, "challenger_total") or ATTACK_BET)

    text = _attack_active_text(data["challenger_uid"], data["challenger_name"],
                               data["defender_uid"], data["defender_name"],
                               c_total, new_d)
    try:
        await callback.message.edit_text(text, reply_markup=_attack_markup(attack_id))
    except Exception:
        pass
    await callback.answer(f"🛡 已反击投入 {ATTACK_BET}！你的总投入：{int(new_d)}")
