import asyncio
import logging
import random
import time

from aiogram import types

from core import bot, redis
from config import ALLOWED_THREAD_ID
from utils import get_mention, safe_html, delete_msg_by_id, delete_msgs
from balance import update_balance


def generate_redpack_amounts(total_amount, count):
    if count == 1:
        return [round(total_amount, 2)]
    amounts = []
    rem_amount = total_amount
    rem_count = count
    for _ in range(count - 1):
        max_val = (rem_amount / rem_count) * 2
        amt = round(random.uniform(0.01, max_val), 2)
        if amt < 0.01:
            amt = 0.01
        amounts.append(amt)
        rem_amount -= amt
        rem_count -= 1
    amounts.append(round(rem_amount, 2))
    random.shuffle(amounts)
    return amounts


async def build_redpack_panel(rp_id: str, is_pw: bool, remaining_mins: int = None, refund_info: str = None):
    meta = await redis.hgetall(f"redpack_meta:{rp_id}")
    if not meta:
        return "", None
    users_data = await redis.hgetall(f"redpack_users:{rp_id}")
    count = int(meta['count'])
    amount = meta['amount']
    sender_uid = meta.get('sender_uid', '')
    sender_name = meta.get('sender_name', '某人')
    is_resumed = meta.get("resumed") == "1"

    if remaining_mins is None:
        created_at = float(meta.get('created_at', time.time()))
        elapsed_mins = int((time.time() - created_at) / 60)
        remaining_mins = max(1, 5 - elapsed_mins)

    if is_pw:
        header = f"🧧 <b>{get_mention(sender_uid, sender_name)}</b> 发出了口令红包！\n"
        if is_resumed:
            header = f"🧧 <b>{get_mention(sender_uid, sender_name)}</b> 的红包已恢复！\n<i>(因对局打断挂起重发)</i>\n"

        lines = [
            header,
            f"🔑 口令：<b><code>{safe_html(meta['pw'])}</code></b>\n",
            f"总额：<b>{amount}</b> | 个数：<b>{count}</b>",
            f"领取情况 ({len(users_data)}/{count})："
        ]
        markup = None
    else:
        lines = [
            f"🧧 <b>{get_mention(sender_uid, sender_name)}</b> 发出了拼手气红包！\n",
            f"总额：<b>{amount}</b> | 个数：<b>{count}</b>",
            f"领取情况 ({len(users_data)}/{count})："
        ]
        markup = types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="🧧 抢红包", callback_data=f"grab_rp:{rp_id}")]])

    for u, val in users_data.items():
        name, a = val.rsplit("|", 1)
        lines.append(f"• {get_mention(u, name)} 抢到 <b>{a}</b>")

    if len(users_data) >= count:
        lines.append("\n✅ <b>红包已被抢空！</b>")
        markup = None
    elif remaining_mins <= 0:
        expiry_line = "\n❌ <b>红包已过期！</b>"
        if refund_info:
            expiry_line += f"\n{refund_info}"
        lines.append(expiry_line)
        markup = None
    else:
        lines.append(f"\n⏳ <i>{remaining_mins}分钟后过期自动清理</i>")

    return "\n".join(lines), markup


# 构建单骰子专属【聚合看板】 - 彻底杜绝刷屏并对齐排版
async def refresh_dice_panel(chat_id: int, is_resume: bool = False):
    active_rps = await redis.smembers("active_pw_rps")
    dice_rps = []
    for rp_id in active_rps:
        meta = await redis.hgetall(f"redpack_meta:{rp_id}")
        if meta and meta.get("chat_id") == str(chat_id) and meta.get("pw") == "🎲" and meta.get("suspended") != "1":
            dice_rps.append((rp_id, meta))

    dice_rps.sort(key=lambda x: float(x[1].get('created_at', 0)))

    # 没有活跃骰子红包：解钉+删除聚合面板（如有）
    if len(dice_rps) == 0:
        old_msg_id = await redis.get(f"dice_panel_msg:{chat_id}")
        if old_msg_id:
            try:
                await bot.unpin_chat_message(chat_id=chat_id, message_id=int(old_msg_id))
            except:
                pass
            try:
                await bot.delete_message(chat_id=chat_id, message_id=int(old_msg_id))
            except:
                pass
            await redis.delete(f"dice_panel_msg:{chat_id}")
        return

    header = "🧧 <b>「🎲」口令红包聚合看板</b>\n👇扔出 🎲 即可一键通吃👇\n"
    if is_resume:
        header = "🧧 <b>「🎲」口令红包已恢复！</b>\n<i>(因对局打断挂起重发)</i>\n👇扔出 🎲 即可一键通吃👇\n"

    lines = [header]
    min_remaining_mins = 5

    for i, (rp_id, meta) in enumerate(dice_rps):
        users_data = await redis.hgetall(f"redpack_users:{rp_id}")
        count = int(meta['count'])
        amount = meta['amount']

        # 老板排面高亮
        sender_uid = meta.get('sender_uid', '')
        sender_name = meta.get('sender_name', '某人')
        sender_mention = get_mention(sender_uid, sender_name) if sender_uid else safe_html(sender_name)

        created_at = float(meta.get('created_at', time.time()))
        elapsed_mins = int((time.time() - created_at) / 60)
        rem_mins = max(0, 5 - elapsed_mins)
        if rem_mins < min_remaining_mins:
            min_remaining_mins = rem_mins

        # 抢包人排面高亮
        claimed_strs = []
        for u, val in users_data.items():
            name, a = val.rsplit("|", 1)
            claimed_strs.append(f"{get_mention(u, name)}({a})")

        claimed_text = ", ".join(claimed_strs) if claimed_strs else "暂无"
        rem_count = count - len(users_data)

        lines.append(f"📦 {sender_mention} 的包 ({amount}分/{count}个) | 剩 <b>{rem_count}</b> 个")
        lines.append(f"└ 已领: {claimed_text}\n")

    if min_remaining_mins <= 0:
        lines.append("❌ <b>部分红包已过期！</b>\n<i>(系统正在清理退款...)</i>")
    else:
        lines.append(f"⏳ <i>最早的一个 {min_remaining_mins} 分钟后过期自动清理</i>")

    panel_text = "\n".join(lines)

    old_msg_id = await redis.get(f"dice_panel_msg:{chat_id}")
    if old_msg_id:
        try:
            await bot.edit_message_text(panel_text, chat_id=chat_id, message_id=int(old_msg_id))
        except Exception as e:
            # 忽略 not modified 避免刷屏
            if "not modified" not in str(e).lower():
                try:
                    msg = await bot.send_message(chat_id, panel_text, message_thread_id=ALLOWED_THREAD_ID or None)
                    await redis.set(f"dice_panel_msg:{chat_id}", str(msg.message_id))
                    try:
                        await bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, disable_notification=True)
                    except:
                        pass
                except Exception as e2:
                    logging.warning(f"[dice_panel] 发送面板失败(fallback): {e2}")
    else:
        try:
            msg = await bot.send_message(chat_id, panel_text, message_thread_id=ALLOWED_THREAD_ID or None)
            await redis.set(f"dice_panel_msg:{chat_id}", str(msg.message_id))
            try:
                await bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id, disable_notification=True)
            except:
                pass
        except Exception as e:
            logging.warning(f"[dice_panel] 发送面板失败: {e}")


async def redpack_expiry_watcher(chat_id: int, msg_id: int, rp_id: str, is_pw: bool, expected_epoch: str):
    meta = await redis.hgetall(f"redpack_meta:{rp_id}")
    is_dice = is_pw and meta and meta.get("pw") == "🎲"

    for i in range(5):
        await asyncio.sleep(60)
        meta = await redis.hgetall(f"redpack_meta:{rp_id}")
        if not meta or meta.get("created_at") != expected_epoch or meta.get("suspended") == "1":
            return

        users_data = await redis.hgetall(f"redpack_users:{rp_id}")
        if len(users_data) >= int(meta.get('count', 0)):
            return

        remaining = 4 - i
        if remaining > 0:
            text, markup = await build_redpack_panel(rp_id, is_pw, remaining)
            if msg_id:
                try:
                    await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, reply_markup=markup)
                except Exception:
                    pass
            if is_dice:
                await refresh_dice_panel(chat_id)

    # 过期退款 & 更新面板（不删消息）
    meta = await redis.hgetall(f"redpack_meta:{rp_id}")
    if not meta or meta.get("created_at") != expected_epoch or meta.get("suspended") == "1":
        return

    users_data = await redis.hgetall(f"redpack_users:{rp_id}")
    if len(users_data) >= int(meta.get('count', 0)):
        return

    total_amount = float(meta['amount'])
    claimed_amount = sum(float(v.rsplit("|", 1)[1]) for v in users_data.values())
    refund = round(total_amount - claimed_amount, 2)
    sender_uid = meta.get("sender_uid")
    sender_name = meta.get("sender_name", "老板")

    if refund > 0 and sender_uid:
        await update_balance(sender_uid, refund)

    refund_info = f"已退回 <b>{refund}</b> 积分给 {get_mention(sender_uid, sender_name)}" if (refund > 0 and sender_uid) else None

    # 先构建面板文本（Redis 数据还在），再清理数据
    text, _ = await build_redpack_panel(rp_id, is_pw, 0, refund_info=refund_info)
    await redis.delete(f"redpack_meta:{rp_id}")
    await redis.delete(f"redpack_list:{rp_id}")
    await redis.delete(f"redpack_users:{rp_id}")
    await redis.srem("active_pw_rps", rp_id)

    # 更新面板，不删除
    if msg_id:
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, reply_markup=None)
        except:
            pass

    if is_dice:
        await refresh_dice_panel(chat_id)


async def attempt_claim_pw_redpack(message: types.Message, text: str, uid: str, active_rps: list) -> bool:
    total_claimed = 0
    claimed_info = []
    panels_to_update = {}
    is_dice_claim = (text == "🎲")

    for rp_id in active_rps:
        meta = await redis.hgetall(f"redpack_meta:{rp_id}")
        if not meta:
            await redis.srem("active_pw_rps", rp_id)
            continue

        if meta.get("pw") == text and meta.get("suspended") != "1" and meta.get("chat_id") == str(message.chat.id):
            list_key = f"redpack_list:{rp_id}"
            if await redis.hget(f"redpack_users:{rp_id}", uid):
                continue

            amt_str = await redis.lpop(list_key)
            if not amt_str:
                await redis.srem("active_pw_rps", rp_id)
                continue

            amt = float(amt_str)
            await redis.hset(f"redpack_users:{rp_id}", uid, f"{message.from_user.first_name}|{amt}")
            await redis.expire(f"redpack_users:{rp_id}", 300)

            total_claimed += amt
            claimed_info.append((rp_id, meta, amt))
            panels_to_update[rp_id] = meta

    if total_claimed > 0:
        await update_balance(uid, total_claimed)

        if is_dice_claim:
            await refresh_dice_panel(message.chat.id)  # 提前更新聚合面板，不等个人面板/公告 API call

        for rp_id, meta, amt in claimed_info:
            # 所有红包（含骰子）统一更新独立面板，不删除
            panel_text, markup = await build_redpack_panel(rp_id, is_pw=True)
            if meta.get('chat_id') and meta.get('msg_id'):
                try:
                    await bot.edit_message_text(panel_text, chat_id=int(meta['chat_id']), message_id=int(meta['msg_id']), reply_markup=markup)
                except:
                    pass

            sender_uid = meta.get("sender_uid")
            sender_name = meta.get("sender_name", "某人")
            sender_mention = get_mention(sender_uid, sender_name) if sender_uid else sender_name

            announce_msg = await message.answer(f"🎉 {get_mention(uid, message.from_user.first_name)} 领取了 {sender_mention} 的口令红包，获得 <b>{amt}</b> 积分！")
            asyncio.create_task(delete_msgs([announce_msg], 10))

            users_data = await redis.hgetall(f"redpack_users:{rp_id}")
            if len(users_data) >= int(meta.get('count', 0)):
                await redis.srem("active_pw_rps", rp_id)
                # 面板保留"已抢空"状态，不删除

        if is_dice_claim:
            await refresh_dice_panel(message.chat.id)
        return True
    return False


async def suspend_dice_redpacks(chat_id: int):
    active_rps = await redis.smembers("active_pw_rps")
    suspended_count = 0
    for rp_id in active_rps:
        meta = await redis.hgetall(f"redpack_meta:{rp_id}")
        if meta and meta.get("chat_id") == str(chat_id) and meta.get("pw") == "🎲" and meta.get("suspended") != "1":
            await redis.hset(f"redpack_meta:{rp_id}", "suspended", "1")
            suspended_count += 1

    if suspended_count > 0:
        old_msg_id = await redis.get(f"dice_panel_msg:{chat_id}")
        if old_msg_id:
            asyncio.create_task(delete_msg_by_id(chat_id, int(old_msg_id)))
            await redis.delete(f"dice_panel_msg:{chat_id}")

        msg = await bot.send_message(chat_id, f"⏸ <b>红包保护系统</b>\n因对局已开启，当前群内 <b>{suspended_count}</b> 个「🎲」红包已被自动挂起保护。\n将在赌桌清空后自动合并重发！", message_thread_id=ALLOWED_THREAD_ID or None)
        asyncio.create_task(delete_msgs([msg], 15))


async def resume_dice_redpacks(chat_id: int):
    active_games = await redis.smembers(f"chat_games:{chat_id}")
    if active_games:
        return

    active_rps = await redis.smembers("active_pw_rps")
    resumed_count = 0
    for rp_id in active_rps:
        meta = await redis.hgetall(f"redpack_meta:{rp_id}")
        if meta and meta.get("chat_id") == str(chat_id) and meta.get("pw") == "🎲" and meta.get("suspended") == "1":
            users_data = await redis.hgetall(f"redpack_users:{rp_id}")
            rem_count = int(meta["count"]) - len(users_data)
            if rem_count <= 0:
                continue

            new_epoch = str(time.time())
            await redis.hdel(f"redpack_meta:{rp_id}", "suspended")
            await redis.hset(f"redpack_meta:{rp_id}", "created_at", new_epoch)
            await redis.hset(f"redpack_meta:{rp_id}", "resumed", "1")
            await redis.expire(f"redpack_meta:{rp_id}", 320)
            await redis.expire(f"redpack_list:{rp_id}", 320)
            resumed_count += 1

            asyncio.create_task(redpack_expiry_watcher(chat_id, 0, rp_id, True, new_epoch))

    if resumed_count > 0:
        await refresh_dice_panel(chat_id, is_resume=True)
