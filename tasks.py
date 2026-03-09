import asyncio
import datetime
import glob
import json
import logging
import os
import re
import sqlite3
import time

from config import TZ_BJ, SUPER_ADMIN_ID, ALLOWED_THREAD_ID
from core import bot, redis
from utils import get_mention, safe_zrevrange, unpin_and_delete_after, pin_in_topic
from balance import update_balance

HELP_TEXT = """🎲 <b>骰子竞技场 · 指令与玩法指南</b> 🎲

🏷 <b>一、怎么发起对局？（4种实战姿势）</b>

<b>格式口诀：玩法金额 + 空格 + 骰子数</b>
（注：大小与金额可连打，数字之间必须用空格隔开。支持 1-5 颗，填 0 积分即为友谊赛）。

• <b>普通双人局</b>：发送 <code>大100 3</code>
（只等1人，有人点按钮立刻发车）

• <b>指定单挑局</b>：回复对手的消息发送 <code>大100 3</code>
（只准他接单，1分钟不理你自动退回积分）

• <b>多人拼车局</b>：发送 <code>大100 3 多</code>
（2到5人都能玩。有人进就触发15秒倒计时，满5人瞬间发车）

• <b>定员死等局</b>：发送 <code>大100 3 多 4</code>
（结尾的 4 代表必须死等凑齐4人，少一个都不发车）

🏷 <b>二、骰子计算规则</b>

• 每位玩家先投 <b>N</b> 颗（1-5 颗），系统按该组骰子算分。
• <b>底分</b> = 所有骰子点数之和。
• <b>同点加成</b>：每多一颗重复点数 +1（如 <code>[2,2,5]</code> +1、<code>[4,4,4]</code> +2）。
• <b>顺子翻倍</b>：当点数互不重复且连续（至少3颗）时，整组 ×2（如 <code>[1,2,3]</code>、<code>[2,3,4,5]</code>）。
• <b>最终点数</b> = （底分 + 同点加成，顺子再翻倍）后取个位（<code>%10</code>）。
• 比<b>大</b>：点数越大越强；比<b>小</b>：点数越小越强；同分自动加赛，每轮并列玩家各补投1颗。
• 超时未投会记为逃跑判负；加赛最多到 20 颗后强制清算。

🏷 <b>三、连胜 / 连败奖惩</b>

• <b>乐善好施</b>：连赢 3 局（有积分加）→ 按最近3局平均下注的20%四舍五入取整自动扣分，重置后循环计算
• <b>同舟共济</b>：连败 3 局（有积分扣）→ 按最近3局平均下注的20%四舍五入取整自动补贴，重置后循环计算
（平局 ±0 重置计数；与名次无关，以实际盈亏符号判定）

🏷 <b>四、/dice_attack 单挑对决</b>

回复某人的消息发 <code>/dice_attack</code> 向其发起攻击！

• 发起时先扣 <b>1000 积分</b>，双方可在1分钟内反复追加（每次 +1000）
• 💥 <b>加大力度</b>：仅发起方可按   🛡 <b>回手反击</b>：仅迎战方可按
• 投入越多赢面越大（加权随机），每人最高投入 <b>20000</b> 积分
• 1分钟后自动结算：赢家取回本金 + 随机奖励 <b>2000–20000</b> 积分
• 对方未回应：全额退款，原面板自动销毁

🏷 <b>五、指令大全</b>

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

try:
    from lunardate import LunarDate
    _HAS_LUNAR = True
except ImportError:
    _HAS_LUNAR = False
    logging.warning("lunardate 未安装，农历节日彩蛋不可用（pip install lunardate 后 rebuild）")

# 冬至日期逐年查表（约 12/21 或 12/22）
_DONGZHI_DAY = {
    2024: 21, 2025: 22, 2026: 22, 2027: 22,
    2028: 21, 2029: 22, 2030: 22, 2031: 22,
    2032: 21, 2033: 22, 2034: 22, 2035: 22,
}

BACKUP_GLOB = "backup_*.db"
BACKUP_KEEP = 3


def list_backup_files() -> list[str]:
    files = sorted(glob.glob(BACKUP_GLOB), reverse=True)
    if not files and os.path.exists("backup.db"):
        return ["backup.db"]
    return files


def get_latest_backup_path() -> str | None:
    files = list_backup_files()
    return files[0] if files else None


def _new_backup_path() -> str:
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
    return f"backup_{ts}.db"


def _prune_old_backups() -> None:
    files = sorted(glob.glob(BACKUP_GLOB), reverse=True)
    for stale in files[BACKUP_KEEP:]:
        try:
            os.remove(stale)
        except OSError as e:
            logging.warning(f"清理旧备份失败: {stale} err={e}")
    if files and os.path.exists("backup.db"):
        try:
            os.remove("backup.db")
        except OSError as e:
            logging.warning(f"清理旧格式备份失败: backup.db err={e}")


async def perform_backup() -> int:
    keys = []
    async for key in redis.scan_iter("user_balance:*"):
        keys.append(key)

    users_data = []
    for key in keys:
        uid = key.split(":")[1]
        bal = float(await redis.get(key) or 20000.0)
        name = await redis.hget("user_names", uid) or "未知玩家"
        u_data = await redis.hgetall(f"user_data:{uid}")
        last_checkin = u_data.get("last_checkin", "")
        streak = int(u_data.get("streak", 0))
        users_data.append((uid, bal, name, last_checkin, streak))

    backup_file = _new_backup_path()

    def db_write():
        conn = sqlite3.connect(backup_file)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS users
                     (uid TEXT PRIMARY KEY, balance REAL, name TEXT, last_checkin TEXT, streak INTEGER)''')
        c.execute("BEGIN TRANSACTION")
        c.executemany('''INSERT OR REPLACE INTO users (uid, balance, name, last_checkin, streak)
                         VALUES (?, ?, ?, ?, ?)''', users_data)
        conn.commit()
        conn.close()

    if users_data:
        await asyncio.to_thread(db_write)
        _prune_old_backups()
        logging.info(f"✅ SQLite 物理备份完成，共写入 {len(users_data)} 条记录。文件: {backup_file}")
        return len(users_data)
    return 0


async def daily_backup_task():
    while True:
        now = datetime.datetime.now(TZ_BJ)
        # 整点触发：等到下一个整点
        next_run = now.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
        await asyncio.sleep((next_run - now).total_seconds())

        count = await perform_backup()
        latest = get_latest_backup_path() or "无"
        try:
            await bot.send_message(
                chat_id=SUPER_ADMIN_ID,
                text=f"🛡 <b>系统自动通报：每小时灾备完成</b>\n\n⏰ 时间：{datetime.datetime.now(TZ_BJ).strftime('%Y-%m-%d %H:%M:%S')}\n📦 备份条数：<b>{count}</b> 条核心资产\n🗂 最新备份：<code>{latest}</code>\n♻️ 仅保留最近 <b>{BACKUP_KEEP}</b> 份。"
            )
        except Exception as e:
            logging.error(f"每小时备份通报超管失败: {e}")


async def daily_report_task():
    while True:
        now = datetime.datetime.now(TZ_BJ)
        next_run = now.replace(hour=0, minute=1, second=0, microsecond=0)
        if next_run <= now:
            next_run += datetime.timedelta(days=1)

        await asyncio.sleep((next_run - now).total_seconds())

        yesterday_dt = datetime.datetime.now(TZ_BJ) - datetime.timedelta(days=1)
        yesterday_str = yesterday_dt.strftime("%Y%m%d")
        display_date = yesterday_dt.strftime("%Y-%m-%d")

        points_key = f"rank_points:daily:{yesterday_str}"
        wins_key = f"rank_wins:daily:{yesterday_str}"
        losses_key = f"rank_losses:daily:{yesterday_str}"
        init_key = f"rank_init:daily:{yesterday_str}"

        if not await redis.exists(points_key):
            continue

        async def get_top_user(key, reverse=True):
            if reverse:
                res = await safe_zrevrange(key, 0, 0, withscores=True)
            else:
                res = await redis.zrange(key, 0, 0, withscores=True)
            if not res:
                return None, None, 0
            uid, score = res[0]
            name = await redis.hget("user_names", uid) or "未知玩家"
            return uid, get_mention(uid, name), int(score)

        init_uid, init_user, init_score = await get_top_user(init_key)
        win_uid, win_user, win_score = await get_top_user(wins_key)
        loss_uid, loss_user, loss_score = await get_top_user(losses_key)

        top_winners = await safe_zrevrange(points_key, 0, 4, withscores=True)
        winners = [(u, p) for u, p in top_winners if p > 0]

        top_losers = await redis.zrange(points_key, 0, 4, withscores=True)
        losers = [(u, p) for u, p in top_losers if p < 0]

        lines = [f"🌅 <b>昨日战况播报 ({display_date})</b>\n"]
        lines.append("🎖 <b>【昨日之最】</b>")
        if init_user:
            lines.append(f"🚕 <b>发车狂魔</b>: {init_user} (带头冲锋 <b>{init_score}</b> 局)")
        if win_user:
            lines.append(f"⚔️ <b>常胜将军</b>: {win_user} (大杀四方 <b>{win_score}</b> 局)")
        if loss_user:
            lines.append(f"💸 <b>慈善大使</b>: {loss_user} (散财送暖 <b>{loss_score}</b> 局)")

        lines.append("\n📈 <b>【昨日狂赚榜 TOP 5】</b>")
        if winners:
            for idx, (uid, points) in enumerate(winners):
                name = await redis.hget("user_names", uid) or "未知玩家"
                lines.append(f"{idx+1}. {get_mention(uid, name)} | 净赚: <b>+{points:g}</b>分")
        else:
            lines.append("暂无盈利数据。")

        lines.append("\n📉 <b>【昨日随份子榜 TOP 5】</b>")
        if losers:
            for idx, (uid, points) in enumerate(losers):
                name = await redis.hget("user_names", uid) or "未知玩家"
                lines.append(f"{idx+1}. {get_mention(uid, name)} | 净亏: <b>{abs(points):g}</b>分")
        else:
            lines.append("暂无亏损数据。")

        # ── 上榜奖励（每次上榜 +500，重复上榜累加）──
        LEADERBOARD_BONUS = 500
        reward_counts: dict = {}
        for uid in [init_uid, win_uid, loss_uid]:
            if uid:
                reward_counts[uid] = reward_counts.get(uid, 0) + 1
        for uid, _ in winners:
            reward_counts[uid] = reward_counts.get(uid, 0) + 1
        for uid, _ in losers:
            reward_counts[uid] = reward_counts.get(uid, 0) + 1

        if reward_counts:
            lines.append("\n🏅 <b>【上榜奖励 +500/次】</b>")
            for uid, count in reward_counts.items():
                bonus = LEADERBOARD_BONUS * count
                await update_balance(uid, bonus)
                name = await redis.hget("user_names", uid) or "未知玩家"
                tag = f"（上榜 {count} 次）" if count > 1 else ""
                lines.append(f"🎁 {get_mention(uid, name)} 获得 <b>+{bonus}</b> 分{tag}")

        report_text = "\n".join(lines)

        active_groups = await redis.smembers("active_groups")
        for gid in active_groups:
            try:
                await bot.send_message(chat_id=int(gid), text=report_text, message_thread_id=ALLOWED_THREAD_ID or None)
            except Exception as e:
                await redis.srem("active_groups", gid)
                logging.warning(f"无法向群组 {gid} 发送战报，已移除记录: {e}")


async def noon_event_task():
    while True:
        now = datetime.datetime.now(TZ_BJ)
        next_noon = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if next_noon <= now:
            next_noon += datetime.timedelta(days=1)
        await asyncio.sleep((next_noon - now).total_seconds())

        now = datetime.datetime.now(TZ_BJ)
        month, day = now.month, now.day
        weekday = now.weekday()  # 0=周一, 3=周四
        is_last_day = (now + datetime.timedelta(days=1)).day == 1

        events = []

        # ── 每周四 ──
        if weekday == 3:
            events.append(("🍗 <b>疯狂星期四，V你50！</b>\n周四到了，全体玩家今天有鸡腿！疯起来！", 50))

        # ── 周末 ──
        if weekday == 5:
            events.append(("🎉 <b>周六快乐！</b>\n周末终于来了，先把积分收好，好好放松！", 200))
        if weekday == 6:
            events.append(("🛌 <b>周日快乐！</b>\n周末最后一天，摸鱼摸到底，明天见！", 200))

        # ── 固定节日 ──
        if month == 1 and day == 1:
            events.append(("🎆 <b>元旦快乐！新年大吉！</b>\n新年第一天，财运来了，接住！", 100))
        if month == 2 and day == 14:
            events.append(("💕 <b>情人节快乐！</b>\n愿天下有情人终成眷属，520 的爱意带走！", 520))
        if month == 2 and day == 29:
            events.append(("🦁 <b>四年一遇！2月29日！</b>\n闰年限定，错过再等四年，快拿走！", 229))
        if month == 3 and day == 8:
            events.append(("🌸 <b>妇女节快乐！</b>\n巾帼不让须眉，今天女生们最棒！", 38))
        if month == 4 and day == 1:
            events.append(("🃏 <b>愚人节！骗你的——积分是真的！</b>\n哈哈，诚意给到位了。", 41))
        if month == 5 and day == 1:
            events.append(("🔨 <b>劳动节快乐！打工人辛苦了！</b>\n五一好好歇着，积分先收好。", 51))
        if month == 6 and day == 1:
            events.append(("🎈 <b>儿童节快乐！</b>\n大家都是老小孩，今天放肆玩！", 61))
        if month == 6 and day == 18:
            events.append(("🛒 <b>618 大促！</b>\n钱没了没关系，积分先到位！", 18))
        if month == 8 and day == 8:
            events.append(("🀄 <b>双八吉日！发发发！</b>\n88 谐音「发发」，今天手气一定好！", 88))
        if month == 9 and day == 10:
            events.append(("📚 <b>教师节快乐！</b>\n老师们辛苦了，知识无价，积分有价。", 36))
        if month == 10 and day == 1:
            events.append(("🎉 <b>国庆节快乐！</b>\n祖国生日快乐，山河无恙，人间皆安！", 100))
        if month == 11 and day == 11:
            events.append(("💔 <b>光棍节，感同身受！</b>\n一起单着，积分总不会飞走。", 111))
        if month == 12 and day == 12:
            events.append(("🛍 <b>双十二！</b>\n钱包空了，积分补上，继续冲！", 12))
        if month == 12 and day == 25:
            events.append(("🎄 <b>圣诞快乐！HO HO HO！</b>\n圣诞礼物到了，接住接住！", 88))

        # 冬至：日期查表，避免 12/21 与 12/22 都触发
        dongzhi_day = _DONGZHI_DAY.get(now.year, 22)
        if month == 12 and day == dongzhi_day:
            events.append(("❄️ <b>冬至快乐！</b>\n冬至大如年，饺子汤圆随便选，吃好喝好！", 21))

        # ── 月末慰问 ──
        if is_last_day:
            events.append(("📅 <b>月末了！</b>\n这个月大家辛苦了，积分先拿着，下月继续！", 30))

        # ── 农历节日（依赖 lunardate，rebuild 后生效）──
        if _HAS_LUNAR:
            try:
                lunar = LunarDate.fromSolarDate(now.year, now.month, now.day)
                lm, ld, leap = lunar.month, lunar.day, lunar.isLeapMonth

                # 除夕：明天是农历正月初一
                tomorrow = now + datetime.timedelta(days=1)
                tmr_lunar = LunarDate.fromSolarDate(tomorrow.year, tomorrow.month, tomorrow.day)
                if tmr_lunar.month == 1 and tmr_lunar.day == 1 and not tmr_lunar.isLeapMonth:
                    events.append(("🧧 <b>除夕快乐！</b>\n年夜饭摆起来，今年最后一天，好好过！", 888))

                if not leap:
                    if lm == 1 and ld == 1:
                        events.append(("🎊 <b>新年快乐！大年初一！</b>\n恭喜发财，万事如意，开门大吉！", 1000))
                    if lm == 1 and ld == 15:
                        events.append(("🏮 <b>元宵节快乐！</b>\n花灯亮起来，汤圆吃起来，热热闹闹！", 150))
                    if lm == 5 and ld == 5:
                        events.append(("🐉 <b>端午节快乐！</b>\n粽子香，龙舟响，祝大家端午安康！", 55))
                    if lm == 7 and ld == 7:
                        events.append(("⭐ <b>七夕快乐！</b>\n鹊桥今夜搭好了，有情人好好珍惜！", 77))
                    if lm == 8 and ld == 15:
                        events.append(("🌕 <b>中秋节快乐！</b>\n月亮最圆的一夜，月饼和积分都有！", 100))
                    if lm == 9 and ld == 9:
                        events.append(("🏔 <b>重阳节快乐！</b>\n登高望远，步步高升，孝敬家人别忘了！", 99))
                    if lm == 12 and ld == 23:
                        events.append(("🍬 <b>小年快乐！</b>\n年味来了，好日子就要开始了！", 23))
            except Exception as e:
                logging.warning(f"农历节日判断失败: {e}")

        if not events:
            continue

        uids = await redis.hkeys("user_names")
        total_bonus = sum(amt for _, amt in events)
        for uid in uids:
            await update_balance(uid, total_bonus)

        # 写事件日志（每个触发事件单独一条）
        ts_now = int(time.time())
        for msg, amt in events:
            short_desc = msg.split("\n")[0]  # 取第一行作为标题
            short_desc = re.sub(r"<[^>]+>", "", short_desc).strip()  # 去 HTML 标签
            record = json.dumps({"ts": ts_now, "type": "easter_egg", "desc": short_desc, "bonus": amt, "count": len(uids)}, ensure_ascii=False)
            await redis.lpush("event_log", record)
        await redis.ltrim("event_log", 0, 199)

        text_parts = "\n\n".join(f"{msg}\n🎁 全员 <b>+{amt}</b> 积分！" for msg, amt in events)
        announce_text = f"🎊 <b>【系统彩蛋触发！】</b>\n\n{text_parts}\n\n✅ 已自动发放给 <b>{len(uids)}</b> 名玩家！"

        # 计算挂到17:00的剩余秒数
        unpin_at = now.replace(hour=17, minute=0, second=0, microsecond=0)
        pin_secs = max(60.0, (unpin_at - now).total_seconds())

        active_groups = await redis.smembers("active_groups")
        for gid in list(active_groups):
            try:
                msg = await bot.send_message(chat_id=int(gid), text=announce_text, message_thread_id=ALLOWED_THREAD_ID or None)
                try:
                    await pin_in_topic(int(gid), msg.message_id, disable_notification=False)
                except Exception:
                    pass
                asyncio.create_task(unpin_and_delete_after(int(gid), msg.message_id, pin_secs))
            except Exception as e:
                await redis.srem("active_groups", gid)
                logging.warning(f"无法向群组 {gid} 发送彩蛋公告，已移除记录: {e}")


async def weekly_help_task():
    while True:
        now = datetime.datetime.now(TZ_BJ)
        # 每周一 10:00 整发送，次周再来
        days_until_monday = (7 - now.weekday()) % 7
        next_run = now.replace(hour=10, minute=0, second=0, microsecond=0) + datetime.timedelta(days=days_until_monday)
        if next_run <= now:
            next_run += datetime.timedelta(days=7)
        await asyncio.sleep((next_run - now).total_seconds())

        active_groups = await redis.smembers("active_groups")
        for gid in list(active_groups):
            gid_int = int(gid)
            try:
                # 解钉并删除上一条帮助置顶
                old_pin_id = await redis.get(f"help_pin:{gid}")
                if old_pin_id:
                    try:
                        await bot.unpin_chat_message(chat_id=gid_int, message_id=int(old_pin_id))
                    except Exception:
                        pass
                    try:
                        await bot.delete_message(chat_id=gid_int, message_id=int(old_pin_id))
                    except Exception:
                        pass
                    await redis.delete(f"help_pin:{gid}")

                msg = await bot.send_message(chat_id=gid_int, text=HELP_TEXT, message_thread_id=ALLOWED_THREAD_ID or None)
                try:
                    await pin_in_topic(gid_int, msg.message_id, disable_notification=True)
                except Exception:
                    pass
                await redis.set(f"help_pin:{gid}", str(msg.message_id))
            except Exception as e:
                await redis.srem("active_groups", gid)
                logging.warning(f"无法向群组 {gid} 发送每周帮助，已移除记录: {e}")
