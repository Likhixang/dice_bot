import asyncio
import json
import time
import uuid

from aiogram import types

from config import game_locks, ALLOWED_THREAD_ID
from core import bot, redis
from utils import get_mention, delete_msg_by_id, delete_msgs, delete_msgs_by_ids
from balance import update_balance, release_user_locks, get_or_init_balance
from redpack import suspend_dice_redpacks, resume_dice_redpacks
from game_settle import get_roll_keyboard, process_dice_value


async def refund_game(chat_id: int, game_id: str):
    game_key = f"game:{game_id}"
    game_data = await redis.hgetall(game_key)
    if not game_data:
        return

    players = json.loads(game_data.get("players", "[]"))
    amount = float(game_data.get("amount", 0))
    if amount > 0:
        for p in players:
            await update_balance(p, amount)

    await release_user_locks(players)
    await redis.srem(f"chat_games:{chat_id}", game_id)

    msg_ids = await redis.lrange(f"game_msgs:{game_id}", 0, -1)
    if msg_ids:
        asyncio.create_task(delete_msgs_by_ids(chat_id, msg_ids))
    tie_panel_id = game_data.get("tie_panel_msg_id")
    if tie_panel_id:
        asyncio.create_task(delete_msg_by_id(chat_id, int(tie_panel_id)))
    await redis.delete(f"game_msgs:{game_id}")
    await redis.delete(game_key)
    game_locks.pop(game_id, None)

    await resume_dice_redpacks(chat_id)


async def check_and_destroy_timeout(chat_id: int, game_id: str):
    game_key = f"game:{game_id}"
    game_data = await redis.hgetall(game_key)
    if not game_data:
        return

    players = json.loads(game_data.get("players", "[]"))
    names = json.loads(game_data.get("names", "{}"))
    game_mode = game_data.get("game_mode")

    cmd_msg_id = game_data.get("cmd_msg_id")
    if cmd_msg_id:
        asyncio.create_task(delete_msg_by_id(chat_id, int(cmd_msg_id)))

    await refund_game(chat_id, game_id)

    if game_mode == "targeted":
        initiator = players[0]
        msg = await bot.send_message(chat_id, f"⏰ 对方 未在1分钟内应答，{get_mention(initiator, names[initiator])} 的指定对战已自动销毁，押金退回。", message_thread_id=ALLOWED_THREAD_ID or None)
    elif game_mode == "multi_exact":
        initiator = players[0]
        msg = await bot.send_message(chat_id, f"⏰ {get_mention(initiator, names[initiator])} 的发车未在规定时间内达到指定人数，对局作废，押金退回。", message_thread_id=ALLOWED_THREAD_ID or None)
    else:
        mentions = " ".join([get_mention(uid, names.get(uid, "未知")) for uid in players])
        msg = await bot.send_message(chat_id, f"💥 <b>发车超时/人员流失强制解散</b>\n{mentions}\n押金已全额退回！", message_thread_id=ALLOWED_THREAD_ID or None)
    asyncio.create_task(delete_msgs([msg], 10))


async def join_timer_watcher(chat_id: int, game_id: str):
    game_key = f"game:{game_id}"
    while True:
        await asyncio.sleep(2)
        game_data = await redis.hgetall(game_key)
        if not game_data or game_data.get("status") != "waiting_join":
            break

        deadline = float(game_data.get("join_deadline", 0))
        if time.time() >= deadline:
            players = json.loads(game_data.get("players", "[]"))
            game_mode = game_data.get("game_mode")
            target_players = int(game_data.get("target_players", 5))

            if game_mode == "multi_exact" and len(players) < target_players:
                await check_and_destroy_timeout(chat_id, game_id)
            elif len(players) < 2:
                await check_and_destroy_timeout(chat_id, game_id)
            else:
                await start_rolling_phase(chat_id, game_id, game_data)
            break


async def start_rolling_phase(chat_id: int, game_id: str, game_data: dict):
    game_key = f"game:{game_id}"
    players = json.loads(game_data["players"])
    names = json.loads(game_data["names"])
    dice_count = int(game_data['dice_count'])
    amount = float(game_data['amount'])
    init_msg_id = game_data.get("init_msg_id")
    cmd_msg_id = game_data.get("cmd_msg_id")

    if amount > 0 and int(round(amount * 100)) % 2 != 0 and len(players) >= 4:
        if cmd_msg_id:
            asyncio.create_task(delete_msg_by_id(chat_id, int(cmd_msg_id), 10))
        await refund_game(chat_id, game_id)
        msg = await bot.send_message(chat_id, f"❌ <b>封车阻断：精度溢出</b>\n尾数为奇数分的金额 ({amount}) 在 {len(players)} 人局结算会导致残余死账。本局已作废并退款！", message_thread_id=ALLOWED_THREAD_ID or None)
        asyncio.create_task(delete_msgs([msg], 10))
        return

    if init_msg_id:
        try:
            player_list_str = "、".join([get_mention(p, names[p]) for p in players])
            direction = game_data['direction']
            if game_data.get("game_mode") in ["multi_exact", "multi_dynamic"]:
                txt = f"🎲 <b>组局已发车！</b> 比{direction} · {amount:g}/人\n名单：{player_list_str}"
            else:
                txt = f"🎯 <b>决斗已发车！</b> 比{direction} · {amount:g}/人\n名单：{player_list_str}"
            await bot.edit_message_text(txt, chat_id, int(init_msg_id), reply_markup=None)
        except:
            pass

    if cmd_msg_id:
        asyncio.create_task(delete_msg_by_id(chat_id, int(cmd_msg_id), 10))

    await redis.hset(game_key, mapping={
        "status": "rolling",
        "queue": json.dumps(players),
        "rolls": json.dumps({uid: [] for uid in players}),
        "target_lengths": json.dumps({uid: dice_count for uid in players}),
        "last_action_time": str(time.time()),
        "tie_rounds": "0",
        "escaped_players": "[]"
    })
    first_uid = players[0]
    mention = get_mention(first_uid, names[first_uid])
    rule_desc = f"比{game_data['direction']}局 · 押注 {amount:g}/人 · 同点加成 · 顺子翻倍"
    player_list_str = "、".join([get_mention(p, names[p]) for p in players])

    msg = await bot.send_message(
        chat_id,
        f"🚦 <b>发车！{len(players)}人局</b>\n<i>{rule_desc}</i>\n👥 {player_list_str}\n\n👉 请 {mention} 投出 <b>{dice_count}</b> 颗骰子！",
        reply_markup=get_roll_keyboard(game_id, first_uid),
        message_thread_id=ALLOWED_THREAD_ID or None
    )
    await redis.rpush(f"game_msgs:{game_id}", msg.message_id)
    asyncio.create_task(rolling_timeout_watcher(chat_id, game_id))


async def rolling_timeout_watcher(chat_id: int, game_id: str):
    game_key = f"game:{game_id}"
    while True:
        await asyncio.sleep(5)
        game_data = await redis.hgetall(game_key)
        if not game_data:
            break
        status = game_data.get("status")
        if status not in ["rolling", "tie_break"]:
            break

        last_time = float(game_data.get("last_action_time", 0))
        elapsed = time.time() - last_time

        if elapsed > 30:
            uid = None
            if status == "rolling":
                queue = json.loads(game_data.get("queue", "[]"))
                if queue:
                    uid = queue[0]
            else:
                tie_queue = json.loads(game_data.get("tie_queue", "[]"))
                g_idx = int(game_data.get("current_tie_group", "0"))
                t_idx = int(game_data.get("current_turn", "0"))
                if g_idx < len(tie_queue) and t_idx < len(tie_queue[g_idx]):
                    uid = tie_queue[g_idx][t_idx]

            if not uid:
                break

            target = json.loads(game_data["target_lengths"])[uid]
            rolls = json.loads(game_data["rolls"])
            rem = target - len(rolls.get(uid, []))

            if rem > 0:
                names = json.loads(game_data["names"])

                if elapsed > 60:
                    escaped_str = await redis.hget(game_key, "escaped_players")
                    escaped_list = json.loads(escaped_str) if escaped_str else []
                    if uid not in escaped_list:
                        escaped_list.append(uid)
                        await redis.hset(game_key, "escaped_players", json.dumps(escaped_list))

                    msg = await bot.send_message(chat_id, f"⏰ {get_mention(uid, names[uid])} 投掷严重超时，已标记为逃跑并垫底！", message_thread_id=ALLOWED_THREAD_ID or None)
                    asyncio.create_task(delete_msgs([msg], 10))

                    for _ in range(rem):
                        fresh = await redis.hgetall(game_key)
                        if not fresh or fresh.get("status") != status:
                            break
                        await process_dice_value(chat_id, game_id, uid, -1, None)
                        await asyncio.sleep(0.5)

                elif elapsed > 30:
                    warned = game_data.get(f"warned_{uid}", "0")
                    if warned == "0":
                        await redis.hset(game_key, f"warned_{uid}", "1")
                        _dir = game_data.get("direction", "?")
                        _amt = float(game_data.get("amount", 0))
                        msg = await bot.send_message(
                            chat_id,
                            f"⚠️ <b>催投警告 · 比{_dir} · {_amt:g}/人</b>\n{get_mention(uid, names[uid])} 还有 <b>30 秒</b>！请尽快投出剩余 <b>{rem}</b> 颗骰子，超时将被判负扣分！",
                            reply_markup=get_roll_keyboard(game_id, uid),
                            message_thread_id=ALLOWED_THREAD_ID or None
                        )
                        asyncio.create_task(delete_msgs([msg], 30))
                        await redis.rpush(f"game_msgs:{game_id}", msg.message_id)
    game_locks.pop(game_id, None)


async def rank_panel_watcher(chat_id: int, msg_id: int, cmd_msg_id: int):
    while True:
        await asyncio.sleep(5)
        ttl = await redis.ttl(f"rank_msg:{chat_id}:{msg_id}")
        if ttl <= 0:
            asyncio.create_task(delete_msgs_by_ids(chat_id, [msg_id, cmd_msg_id]))
            break


async def start_game_creation(chat_id: int, uid: str, name: str, pending_data: dict):
    direction = pending_data["direction"]
    amount = float(pending_data["amount"])
    dice_count = int(pending_data["dice_count"])
    is_multi = pending_data["is_multi"] if isinstance(pending_data["is_multi"], bool) else str(pending_data["is_multi"]).lower() == "true"
    is_exact = pending_data["is_exact"] if isinstance(pending_data["is_exact"], bool) else str(pending_data["is_exact"]).lower() == "true"
    target_players = int(pending_data["target_players"])
    target_uid = pending_data.get("target_uid", "")
    target_name = pending_data.get("target_name", "")

    if is_multi:
        game_mode = "multi_exact" if is_exact else "multi_dynamic"
    elif target_uid:
        game_mode = "targeted"
    else:
        game_mode = "single"

    if amount > 0:
        bal = await get_or_init_balance(uid)
        if bal < amount:
            msg = await bot.send_message(chat_id, f"❌ <b>余额不足</b>\n需要 {amount:g}，你仅有 {bal}。", message_thread_id=ALLOWED_THREAD_ID or None)
            asyncio.create_task(delete_msgs([msg], 10))
            return
        await update_balance(uid, -amount)

    game_id = str(uuid.uuid4())[:8]
    game_key = f"game:{game_id}"
    players = [uid]
    names = {uid: name}

    if game_mode in ("single", "targeted"):
        join_deadline = time.time() + 60
    elif game_mode == "multi_dynamic":
        join_deadline = time.time() + 60
    else:  # multi_exact
        join_deadline = time.time() + 300

    await redis.set(f"user_game:{uid}", game_id)
    await redis.sadd(f"chat_games:{chat_id}", game_id)
    await suspend_dice_redpacks(chat_id)
    await redis.hset(game_key, mapping={
        "status": "waiting_join",
        "chat_id": str(chat_id),
        "players": json.dumps(players),
        "names": json.dumps(names),
        "amount": str(amount),
        "dice_count": str(dice_count),
        "direction": direction,
        "game_mode": game_mode,
        "target_players": str(target_players),
        "target_uid": target_uid,
        "join_deadline": str(join_deadline),
    })
    await redis.expire(game_key, 3600)

    mention = get_mention(uid, name)
    if game_mode == "single":
        txt = (f"🎯 <b>决斗发起！</b>\n"
               f"{mention} 向群友发起对决！\n"
               f"押注：<b>{amount:g}</b> | 骰子：<b>{dice_count}</b>颗 | 比<b>{direction}</b>\n"
               f"60秒无人应答自动退款，快来接单👇")
        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="⚔️ 接单", callback_data=f"jg:{game_id}")
        ]])
    elif game_mode == "targeted":
        target_mention = get_mention(target_uid, target_name)
        txt = (f"🎯 <b>指定决斗！</b>\n"
               f"{mention} 向 {target_mention} 发起专属对决！\n"
               f"押注：<b>{amount:g}</b> | 骰子：<b>{dice_count}</b>颗 | 比<b>{direction}</b>\n"
               f"1分钟内不应答自动退款！")
        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="⚔️ 应战！", callback_data=f"jg:{game_id}")
        ]])
    elif game_mode == "multi_exact":
        txt = (f"🎲 <b>定员组局 (1/{target_players})</b>\n"
               f"押注：<b>{amount:g}</b> | 骰子：<b>{dice_count}</b>颗 | 比<b>{direction}</b>\n"
               f"当前：{mention}\n死等满员👇")
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⚔️ 接单", callback_data=f"jg:{game_id}")],
            [types.InlineKeyboardButton(text="🚀 发起人强行发车", callback_data=f"fs:{game_id}:{uid}")]
        ])
    else:  # multi_dynamic
        txt = (f"🎲 <b>多人发车 (1/5)</b>\n"
               f"押注：<b>{amount:g}</b> | 骰子：<b>{dice_count}</b>颗 | 比<b>{direction}</b>\n"
               f"当前：{mention}\n有人进就开始15秒倒计时👇")
        kb = types.InlineKeyboardMarkup(inline_keyboard=[[
            types.InlineKeyboardButton(text="⚔️ 接单", callback_data=f"jg:{game_id}")
        ]])

    init_msg = await bot.send_message(chat_id, txt, reply_markup=kb, message_thread_id=ALLOWED_THREAD_ID or None)
    await redis.hset(game_key, "init_msg_id", str(init_msg.message_id))
    await redis.rpush(f"game_msgs:{game_id}", init_msg.message_id)
    asyncio.create_task(join_timer_watcher(chat_id, game_id))
