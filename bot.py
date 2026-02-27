import asyncio
import json
import logging
import time

from aiogram import F, Router

from config import LAST_FIX_DESC, SUPER_ADMIN_ID
from core import bot, dp, redis, CleanTextFilter
from utils import delete_msgs, delete_msg_by_id
from balance import update_balance
from tasks import daily_backup_task, daily_report_task, noon_event_task, weekly_help_task
from redpack import redpack_expiry_watcher, attempt_claim_pw_redpack, refresh_dice_panel
from game_settle import process_dice_value
from game import refund_game
from handlers import router as handlers_router

# ==============================
# ⏬ 绝对兜底的全局黑洞 ⏬
# 务必放在代码最最底部，绝不拦截上方的核心指令
# ==============================
blackhole_router = Router()


async def _compensation_cleanup(chat_id: int, msg_id: int, delay: float, redis_key: str):
    """延迟后清理停机补偿置顶：仅当 key 仍指向本消息时才解钉+删除+清 key"""
    await asyncio.sleep(delay)
    current = await redis.get(redis_key)
    if current and int(current) == msg_id:
        try:
            await bot.unpin_chat_message(chat_id=chat_id, message_id=msg_id)
        except:
            pass
        try:
            await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except:
            pass
        await redis.delete(redis_key)


@blackhole_router.message(CleanTextFilter(), F.text)
async def handle_pw_redpack_text(message):
    text = message.text.strip()
    if not text:
        return

    # ── 停机维护（超管专属，精确匹配）──
    if text == "停机维护" and message.from_user.id == SUPER_ADMIN_ID:
        asyncio.create_task(delete_msgs([message], 0))
        # 1. 全群退款对局
        active_groups = await redis.smembers("active_groups")
        destroyed = 0
        for cid_str in active_groups:
            for gid in list(await redis.smembers(f"chat_games:{cid_str}")):
                try:
                    await refund_game(int(cid_str), gid)
                    destroyed += 1
                except Exception as e:
                    logging.warning(f"[maintenance] refund {gid}: {e}")
        # 2. 退回所有活跃 pw 红包
        active_rps = await redis.smembers("active_pw_rps")
        rp_refunded = 0
        affected_rp_chats = set()
        for rp_id in list(active_rps):
            meta = await redis.hgetall(f"redpack_meta:{rp_id}")
            if not meta:
                await redis.srem("active_pw_rps", rp_id)
                continue
            amounts = await redis.lrange(f"redpack_list:{rp_id}", 0, -1)
            total = sum(float(a) for a in amounts)
            if total > 0 and (sid := meta.get("sender_uid")):
                await update_balance(sid, total)
            cid_rp = meta.get("chat_id", "")
            mid_rp = meta.get("msg_id", "0")
            if cid_rp:
                affected_rp_chats.add(cid_rp)
            if cid_rp and mid_rp and int(mid_rp) > 0:
                asyncio.create_task(delete_msg_by_id(int(cid_rp), int(mid_rp)))
            await redis.delete(f"redpack_meta:{rp_id}", f"redpack_list:{rp_id}")
            await redis.srem("active_pw_rps", rp_id)
            rp_refunded += 1
        # 3. 清理骰子聚合面板
        for cid_dc in affected_rp_chats:
            panel = await redis.get(f"dice_panel_msg:{cid_dc}")
            if panel:
                try:
                    await bot.delete_message(int(cid_dc), int(panel))
                except Exception:
                    pass
                await redis.delete(f"dice_panel_msg:{cid_dc}")
        # 4. 先解钉旧公告（补偿或上一次维护）
        for old_key in [f"compensation_pin:{message.chat.id}", f"maintenance_pin:{message.chat.id}"]:
            old_id = await redis.get(old_key)
            if old_id:
                try:
                    await bot.unpin_chat_message(chat_id=message.chat.id, message_id=int(old_id))
                except Exception:
                    pass
                try:
                    await bot.delete_message(chat_id=message.chat.id, message_id=int(old_id))
                except Exception:
                    pass
                await redis.delete(old_key)
        # 5. 发维护公告并置顶
        body = (f"🔧 <b>【停机维护公告】</b>\n\n系统即将进行维护，暂时停止服务。\n"
                f"• 已销毁 <b>{destroyed}</b> 个进行中对局并全额退款\n"
                f"• 已退回 <b>{rp_refunded}</b> 个未过期红包\n\n"
                f"维护完成后将置顶「停机补偿」公告并发放补偿积分，感谢耐心等待！")
        announce = await bot.send_message(message.chat.id, body)
        try:
            await bot.pin_chat_message(chat_id=message.chat.id, message_id=announce.message_id, disable_notification=False)
        except Exception as e:
            logging.warning(f"[maintenance] 置顶失败: {e}")
        await redis.set(f"maintenance_pin:{message.chat.id}", str(announce.message_id))
        await redis.set(f"maintenance:{message.chat.id}", "1")
        return

    # ── 停机补偿（超管专属）──
    if text.startswith("停机补偿") and message.from_user.id == SUPER_ADMIN_ID:
        extra_desc = text[4:].strip()  # 取"停机补偿"后面的自定义说明
        uids = await redis.hkeys("user_names")
        for uid in uids:
            await update_balance(uid, 500)
        record = json.dumps({"ts": int(time.time()), "type": "compensation", "desc": extra_desc or "停机补偿", "bonus": 500, "count": len(uids)}, ensure_ascii=False)
        await redis.lpush("event_log", record)
        await redis.ltrim("event_log", 0, 199)
        asyncio.create_task(delete_msgs([message], 0))
        # 旧维护公告（如有）先解钉+删除
        old_maint_id = await redis.get(f"maintenance_pin:{message.chat.id}")
        if old_maint_id:
            try:
                await bot.unpin_chat_message(chat_id=message.chat.id, message_id=int(old_maint_id))
            except Exception:
                pass
            try:
                await bot.delete_message(chat_id=message.chat.id, message_id=int(old_maint_id))
            except Exception:
                pass
            await redis.delete(f"maintenance_pin:{message.chat.id}")
        await redis.delete(f"maintenance:{message.chat.id}")
        old_comp_msg_id = await redis.get(f"compensation_pin:{message.chat.id}")
        if old_comp_msg_id:
            try:
                await bot.unpin_chat_message(chat_id=message.chat.id, message_id=int(old_comp_msg_id))
            except:
                pass
            try:
                await bot.delete_message(chat_id=message.chat.id, message_id=int(old_comp_msg_id))
            except:
                pass
        body = (
            f"🔧 <b>【停机补偿】</b>\n\n"
            f"非常抱歉给大家带来不便！\n"
            f"系统已向全体 <b>{len(uids)}</b> 名玩家发放 <b>+500</b> 积分补偿！\n"
        )
        desc = extra_desc or LAST_FIX_DESC
        if desc:
            body += f"\n📋 <b>本次更新内容：</b>\n{desc}\n"
        body += "\n感谢耐心等待，继续欢乐！"
        announce = await message.answer(body)
        try:
            await bot.pin_chat_message(chat_id=message.chat.id, message_id=announce.message_id, disable_notification=False)
        except Exception:
            pass
        await redis.set(f"compensation_pin:{message.chat.id}", str(announce.message_id))
        asyncio.create_task(_compensation_cleanup(message.chat.id, announce.message_id, 1800, f"compensation_pin:{message.chat.id}"))
        return

    active_rps = await redis.smembers("active_pw_rps")
    if not active_rps:
        return
    await attempt_claim_pw_redpack(message, text, str(message.from_user.id), list(active_rps))


@blackhole_router.message(CleanTextFilter(), F.dice)
async def handle_manual_dice(message):
    if getattr(message, 'forward_origin', None) or getattr(message, 'forward_date', None):
        return
    uid = str(message.from_user.id)
    chat_id = message.chat.id

    active_games = await redis.smembers(f"chat_games:{chat_id}")
    active_rps = await redis.smembers("active_pw_rps")
    claimed = False

    if active_rps:
        claimed = await attempt_claim_pw_redpack(message, message.dice.emoji, uid, list(active_rps))
        if claimed:
            return

    if not active_games:
        return

    game_id = await redis.get(f"user_game:{uid}")

    if not game_id or game_id not in active_games:
        if not claimed:
            asyncio.create_task(delete_msgs([message], 0))
        return

    if message.dice.emoji != "🎲":
        if not claimed:
            asyncio.create_task(delete_msgs([message], 0))
        return

    msg_id_to_pass = None if claimed else message.message_id
    await process_dice_value(chat_id, game_id, uid, message.dice.value, msg_id_to_pass)


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    # 精确 handler 先注册，黑洞兜底最后
    dp.include_router(handlers_router)
    dp.include_router(blackhole_router)
    asyncio.create_task(daily_backup_task())
    asyncio.create_task(daily_report_task())
    asyncio.create_task(noon_event_task())
    asyncio.create_task(weekly_help_task())

    # ── 重启恢复：清理残留骰子面板 + 重启活跃红包 watcher ──
    try:
        # 1. 扫描所有群，清理重启前留下的骰子面板消息
        group_ids = await redis.smembers("active_groups")
        active_rps = await redis.smembers("active_pw_rps")
        active_dice_chats = set()
        dice_rp_per_chat = {}
        for rp_id in active_rps:
            meta = await redis.hgetall(f"redpack_meta:{rp_id}")
            if meta and meta.get("pw") == "🎲" and meta.get("suspended") != "1":
                cid_str = meta.get("chat_id", "")
                if cid_str:
                    active_dice_chats.add(cid_str)
                    dice_rp_per_chat[cid_str] = dice_rp_per_chat.get(cid_str, 0) + 1
        for cid in group_ids:
            panel_msg_id = await redis.get(f"dice_panel_msg:{cid}")
            if panel_msg_id and dice_rp_per_chat.get(cid, 0) < 2:
                try:
                    await bot.delete_message(int(cid), int(panel_msg_id))
                except Exception:
                    pass
                await redis.delete(f"dice_panel_msg:{cid}")
                logging.info(f"[startup] 清理残留骰子面板 chat={cid} msg={panel_msg_id}")

        # 2. 重启活跃红包的 expiry watcher（普通口令/拼手气）
        for rp_id in active_rps:
            meta = await redis.hgetall(f"redpack_meta:{rp_id}")
            if not meta:
                await redis.srem("active_pw_rps", rp_id)
                continue
            epoch = meta.get("created_at", "")
            chat_id_str = meta.get("chat_id", "")
            msg_id_str = meta.get("msg_id", "0")
            is_pw = "pw" in meta
            if not chat_id_str or not epoch:
                continue
            asyncio.create_task(redpack_expiry_watcher(
                int(chat_id_str), int(msg_id_str), rp_id, is_pw, epoch
            ))
            logging.info(f"[startup] 重启红包 watcher rp_id={rp_id}")
    except Exception as e:
        logging.warning(f"[startup] 重启恢复异常: {e}")

    from aiogram import types as tg_types
    base_commands = [
        tg_types.BotCommand(command="help", description="查看全量规则与指令指南"),
        tg_types.BotCommand(command="event", description="查看最近系统彩蛋与补偿记录"),
        tg_types.BotCommand(command="bal", description="查询余额"),
        tg_types.BotCommand(command="checkin", description="每日签到"),
        tg_types.BotCommand(command="gift", description="回复赠送积分"),
        tg_types.BotCommand(command="redpack", description="发拼手气红包"),
        tg_types.BotCommand(command="redpack_pw", description="发口令红包"),
        tg_types.BotCommand(command="rank", description="今日胜负榜"),
        tg_types.BotCommand(command="rank_week", description="本周胜负榜"),
        tg_types.BotCommand(command="rank_month", description="本月胜负榜")
    ]

    admin_commands = base_commands + [
        tg_types.BotCommand(command="forced_stop", description="[仅限管理] 强杀异常对局"),
        tg_types.BotCommand(command="backup_db", description="[仅限超管] 备份数据"),
        tg_types.BotCommand(command="restore_db", description="[仅限超管] 恢复数据")
    ]

    try:
        await bot.set_my_commands(base_commands, scope=tg_types.BotCommandScopeDefault())
        await bot.set_my_commands(base_commands, scope=tg_types.BotCommandScopeAllGroupChats())
        await bot.set_my_commands(base_commands, scope=tg_types.BotCommandScopeAllPrivateChats())
        await bot.set_my_commands(admin_commands, scope=tg_types.BotCommandScopeAllChatAdministrators())
    except Exception as e:
        logging.warning(f"推送菜单失败: {e}")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
