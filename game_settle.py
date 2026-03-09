import asyncio
import json
import logging
import time
from collections import Counter

from aiogram import types

from config import game_locks, get_lock, ALLOWED_THREAD_ID
from core import bot, redis
from utils import get_mention, safe_html, delete_msg_by_id, delete_msgs, delete_msgs_by_ids
from balance import update_balance, get_period_keys, release_user_locks
from redpack import resume_dice_redpacks


def get_roll_keyboard(game_id: str, target_uid: str):
    return types.InlineKeyboardMarkup(inline_keyboard=[[
        types.InlineKeyboardButton(text="🎲 投1颗", callback_data=f"r1:{game_id}:{target_uid}"),
        types.InlineKeyboardButton(text="🎲 投全部", callback_data=f"ra:{game_id}:{target_uid}")
    ]])


async def session_timeout_watcher(chat_id: int, session_key: str):
    await asyncio.sleep(300)
    data = await redis.hgetall(session_key)
    if not data:
        return

    last_active = float(data.get("last_active", 0))
    if time.time() - last_active >= 300:
        title = data.get("title", "📊 阶段战报看板")
        game_count = data.get("game_count", "0")
        lines = [f"{title}\n<i>(连续 {game_count} 局 | 闲置5分钟自动清算)</i>\n"]

        profits = []
        for k, v in data.items():
            if k.startswith("p_"):
                uid = k[2:]
                prof = float(v)
                name = data.get(f"name_{uid}", "未知玩家")
                profits.append((uid, name, prof))

        profits.sort(key=lambda x: x[2], reverse=True)
        for uid, name, prof in profits:
            sign = "+" if prof > 0 else ""
            lines.append(f"• {get_mention(uid, name)} 累计盈亏: <b>{sign}{prof:.2f}</b>")

        try:
            board_msg = await bot.send_message(chat_id, "\n".join(lines), message_thread_id=ALLOWED_THREAD_ID or None)
            asyncio.create_task(delete_msgs([board_msg], 60))
        except:
            pass
        await redis.delete(session_key)


def calculate_score_with_details(dice_list):
    if not dice_list:
        return 0, "无"
    if -1 in dice_list:
        return 0, "🚫 逃跑判负"

    base_sum = sum(dice_list)
    counts = Counter(dice_list)
    pair_bonus = sum(count - 1 for count in counts.values() if count >= 2)
    is_straight = len(counts) == len(dice_list) > 2 and (max(dice_list) - min(dice_list) == len(dice_list) - 1)

    detail_str = f"底{base_sum}"
    total = base_sum + pair_bonus
    if pair_bonus > 0:
        detail_str += f"+同点{pair_bonus}"
    if is_straight:
        total *= 2
        detail_str = f"({detail_str})x顺2"

    return total % 10, detail_str


def calc_half_int(value: float) -> int:
    """按 20% 计算并四舍五入取整（0.5 进位）。"""
    return int(value * 0.2 + 0.5)


async def process_round_end_or_settle(chat_id: int, game_id: str, game_data: dict):
    game_key = f"game:{game_id}"
    players = json.loads(game_data["players"])
    names = json.loads(game_data["names"])
    rolls = json.loads(game_data["rolls"])
    target_lengths = json.loads(game_data["target_lengths"])
    direction = game_data["direction"]
    initial_count = int(game_data["dice_count"])
    amount = float(game_data["amount"])
    tie_rounds = int(game_data.get("tie_rounds", 0))
    escaped_list = json.loads(game_data.get("escaped_players", "[]"))
    session_key = game_data.get("session_key")

    def get_hist(uid):
        r = rolls.get(uid, [])
        is_escaped = uid in escaped_list
        escape_idx = escaped_list.index(uid) if is_escaped else -1
        score_tuple = []
        if len(r) >= initial_count:
            slice_r = r[:initial_count]
            if -1 in slice_r:
                score_tuple.append(-9999)
            else:
                sc = calculate_score_with_details(slice_r)[0]
                score_tuple.append(sc if direction == "大" else -sc)

        for i in range(initial_count + 1, len(r) + 1):
            slice_r = r[:i]
            if -1 in slice_r:
                score_tuple.append(-9999)
            else:
                sc = calculate_score_with_details(slice_r)[0]
                score_tuple.append(sc if direction == "大" else -sc)
        if not score_tuple:
            score_tuple.append(-9999)
        return (not is_escaped, escape_idx, tuple(score_tuple))

    histories = {p: get_hist(p) for p in players}
    groups = {}
    for p, h in histories.items():
        groups.setdefault(h, []).append(p)
    sorted_hists = sorted(groups.keys(), reverse=True)

    force_settle = any(len(r) >= 20 for r in rolls.values())
    new_queue = []

    if not force_settle:
        for h in sorted_hists:
            if len(groups[h]) > 1:
                for p in groups[h]:
                    new_queue.append(p)
                    target_lengths[p] += 1

    if not new_queue:
        total_cents = int(round(amount * 100))
        num_p = len(players)
        base_profits = []
        if num_p == 2:
            base_profits = [total_cents, -total_cents]
        elif num_p == 3:
            base_profits = [total_cents, 0, -total_cents]
        elif num_p == 4:
            base_profits = [total_cents, total_cents // 2, -total_cents // 2, -total_cents]
        elif num_p == 5:
            base_profits = [total_cents, total_cents // 2, 0, -total_cents // 2, -total_cents]

        player_profit_cents = {}
        current_rank = 0
        sorted_players = []

        for h in sorted_hists:
            group = groups[h]
            g_size = len(group)
            sorted_players.extend(group)

            group_total_profit = sum(base_profits[current_rank: current_rank + g_size])
            base_share = group_total_profit // g_size
            rem = group_total_profit % g_size

            for idx, p in enumerate(group):
                player_profit_cents[p] = base_share + (1 if idx < rem else 0)
            current_rank += g_size

        daily_k, weekly_k, monthly_k = get_period_keys()
        tie_txt = f" <i>(加赛{tie_rounds}轮)</i>" if tie_rounds > 0 else ""
        if force_settle:
            tie_txt += "\n⚠️ <b>[已达20颗极限强制平分清算]</b>"

        final_text = [f"🎲 <b>终局结算单 (比{direction} · 押注{amount:g}/人)</b>{tie_txt}"]
        extreme_compensations = []  # (uid, name, score, kind, bonus_abs) — 极端点数奖惩
        extreme_bonus_abs = calc_half_int(abs(amount))

        if session_key:
            await redis.hset(session_key, "last_active", str(time.time()))
            await redis.hincrby(session_key, "game_count", 1)

        for i, p in enumerate(sorted_players):
            win_lose_profit = player_profit_cents[p] / 100.0
            actual_payout = amount + win_lose_profit

            if actual_payout > 0:
                await update_balance(p, actual_payout)
            await redis.hset("user_names", p, names[p])

            if session_key:
                await redis.hincrbyfloat(session_key, f"p_{p}", win_lose_profit)
                await redis.hset(session_key, f"name_{p}", names[p])

            for period, prefix in [(daily_k, "daily"), (weekly_k, "weekly"), (monthly_k, "monthly")]:
                await redis.zincrby(f"rank_points:{prefix}:{period}", win_lose_profit, p)
                if win_lose_profit > 0:
                    await redis.zincrby(f"rank_gross_wins:{prefix}:{period}", win_lose_profit, p)
                    await redis.zincrby(f"rank_wins:{prefix}:{period}", 1, p)
                elif win_lose_profit < 0:
                    await redis.zincrby(f"rank_gross_losses:{prefix}:{period}", abs(win_lose_profit), p)
                    await redis.zincrby(f"rank_losses:{prefix}:{period}", 1, p)
                await redis.expire(f"rank_points:{prefix}:{period}", 86400 * 60)
                await redis.expire(f"rank_gross_wins:{prefix}:{period}", 86400 * 60)
                await redis.expire(f"rank_gross_losses:{prefix}:{period}", 86400 * 60)
                await redis.expire(f"rank_wins:{prefix}:{period}", 86400 * 60)
                await redis.expire(f"rank_losses:{prefix}:{period}", 86400 * 60)

            sign = "+" if win_lose_profit > 0 else ""
            p_rolls = rolls.get(p, [])
            if -1 in p_rolls or not p_rolls:
                final_text.append(f"第{i+1}名: {get_mention(p, names[p])} | 🚫 逃跑弃权 | 盈亏: <b>{sign}{win_lose_profit:.2f}</b>")
            else:
                score, detail = calculate_score_with_details(p_rolls)
                extra_rounds = len(p_rolls) - initial_count
                p_tie_tag = f" <i>(共投{len(p_rolls)}颗)</i>" if extra_rounds > 0 else ""
                final_text.append(f"第{i+1}名: {get_mention(p, names[p])}{p_tie_tag} | {p_rolls}={detail} ➡ <b>{score}点</b> | 盈亏: <b>{sign}{win_lose_profit:.2f}</b>")
                if (direction == "大" and score == 0) or (direction == "小" and score == 9):
                    extreme_compensations.append((p, names[p], score, "unlucky", extreme_bonus_abs))
                elif (direction == "大" and score == 9) or (direction == "小" and score == 0):
                    extreme_compensations.append((p, names[p], score, "lucky", extreme_bonus_abs))

        await bot.send_message(chat_id, "\n".join(final_text), message_thread_id=ALLOWED_THREAD_ID or None)

        # ── 连胜/连败奖惩 ──
        streak_notifs = []
        for p in sorted_players:
            win_lose_profit = player_profit_cents[p] / 100.0
            streak_key = f"game_streak:{p}"
            streak_bets_key = f"game_streak_bets:{p}"
            current = int(await redis.get(streak_key) or 0)
            current_bets_raw = await redis.get(streak_bets_key)
            try:
                current_bets = json.loads(current_bets_raw) if current_bets_raw else []
            except Exception:
                current_bets = []

            if win_lose_profit > 0:
                new_streak = current + 1 if current > 0 else 1
                current_bets = (current_bets + [amount]) if current > 0 else [amount]
            elif win_lose_profit < 0:
                new_streak = current - 1 if current < 0 else -1
                current_bets = (current_bets + [amount]) if current < 0 else [amount]
            else:
                new_streak = 0  # 平局/无盈亏重置
                current_bets = []

            if new_streak >= 3:
                avg_bet = (sum(current_bets[-3:]) / 3.0) if len(current_bets) >= 3 else amount
                bonus_abs = calc_half_int(abs(avg_bet))
                if bonus_abs:
                    await update_balance(p, -bonus_abs)
                streak_notifs.append((p, names[p], "乐善好施", -bonus_abs, new_streak))
                new_streak = 0
                current_bets = []
            elif new_streak <= -3:
                avg_bet = (sum(current_bets[-3:]) / 3.0) if len(current_bets) >= 3 else amount
                bonus_abs = calc_half_int(abs(avg_bet))
                if bonus_abs:
                    await update_balance(p, bonus_abs)
                streak_notifs.append((p, names[p], "同舟共济", bonus_abs, new_streak))
                new_streak = 0
                current_bets = []

            if new_streak == 0:
                await redis.delete(streak_key)
                await redis.delete(streak_bets_key)
            else:
                await redis.setex(streak_key, 86400 * 7, str(new_streak))
                await redis.setex(streak_bets_key, 86400 * 7, json.dumps(current_bets))

        if streak_notifs:
            lines = []
            for p, name, title, bonus, streak_val in streak_notifs:
                abs_streak = abs(streak_val)
                sign = "+" if bonus > 0 else ""
                if bonus < 0:
                    lines.append(f"💸 <b>【{title}】</b> {get_mention(p, name)} 连赢 {abs_streak} 局，慷慨散财 <b>{sign}{bonus}</b> 积分！")
                else:
                    lines.append(f"🤝 <b>【{title}】</b> {get_mention(p, name)} 连败 {abs_streak} 局，系统补贴 <b>{sign}{bonus}</b> 积分！")
            notif_msg = await bot.send_message(chat_id, "\n".join(lines), message_thread_id=ALLOWED_THREAD_ID or None)
            asyncio.create_task(delete_msgs([notif_msg], 30))

        # ── 极端点数奖惩：比大0点/比小9点补偿 + 比大9点/比小0点回馈 ──
        if extreme_compensations:
            comp_lines = []
            for p, name, sc, kind, bonus_abs in extreme_compensations:
                if kind == "unlucky":
                    if bonus_abs:
                        await update_balance(p, bonus_abs)
                    if sc == 0:
                        comp_lines.append(f"🫡 {get_mention(p, name)} 比大出 <b>0点</b>，太惨了！系统补偿 <b>+{bonus_abs}</b> 积分")
                    else:
                        comp_lines.append(f"🫡 {get_mention(p, name)} 比小出 <b>9点</b>，太倒霉了！系统补偿 <b>+{bonus_abs}</b> 积分")
                else:  # lucky
                    if bonus_abs:
                        await update_balance(p, -bonus_abs)
                    if sc == 9:
                        comp_lines.append(f"🍀 {get_mention(p, name)} 比大出 <b>9点</b>，太幸运了！回馈社会 <b>-{bonus_abs}</b> 积分")
                    else:
                        comp_lines.append(f"🍀 {get_mention(p, name)} 比小出 <b>0点</b>，太幸运了！回馈社会 <b>-{bonus_abs}</b> 积分")
            comp_msg = await bot.send_message(chat_id, "\n".join(comp_lines), message_thread_id=ALLOWED_THREAD_ID or None)
            asyncio.create_task(delete_msgs([comp_msg], 30))

        tie_panel_id = game_data.get("tie_panel_msg_id")
        if tie_panel_id:
            asyncio.create_task(delete_msg_by_id(chat_id, int(tie_panel_id)))
        await release_user_locks(players)
        await redis.srem(f"chat_games:{chat_id}", game_id)

        msg_ids = await redis.lrange(f"game_msgs:{game_id}", 0, -1)
        if msg_ids:
            asyncio.create_task(delete_msgs_by_ids(chat_id, msg_ids))
        await redis.delete(f"game_msgs:{game_id}")
        await redis.delete(game_key)
        game_locks.pop(game_id, None)

        await resume_dice_redpacks(chat_id)

        if session_key:
            asyncio.create_task(session_timeout_watcher(chat_id, session_key))

    else:
        tie_groups = [groups[h] for h in sorted_hists if len(groups[h]) > 1]
        tie_rounds = int(game_data.get("tie_rounds", "0")) + 1
        await redis.hset(game_key, mapping={
            "status": "tie_break",
            "tie_queue": json.dumps(tie_groups),
            "current_tie_group": "0",
            "current_turn": "0",
            "target_lengths": json.dumps(target_lengths),
            "tie_rounds": str(tie_rounds),
            "last_action_time": str(time.time())
        })
        msg_lines = [f"⚔️ <b>触发同分加赛！(比{direction} · {amount:g}/人)</b>"]
        for h in sorted_hists:
            if len(groups[h]) > 1:
                mentions = [get_mention(p, names[p]) for p in groups[h]]
                score_val = h[-1][-1] if direction == "大" else -h[-1][-1]
                msg_lines.append(f"• <b>{score_val}点并列</b>: {', '.join(mentions)}")
        first_uid = tie_groups[0][0]
        msg_lines.append(f"\n👉 {get_mention(first_uid, names[first_uid])} 强制进入加赛池投掷 <b>1</b> 颗骰子！")
        old_tie_panel = game_data.get("tie_panel_msg_id")
        if old_tie_panel:
            asyncio.create_task(delete_msg_by_id(chat_id, int(old_tie_panel)))
        msg = await bot.send_message(chat_id, "\n".join(msg_lines), reply_markup=get_roll_keyboard(game_id, first_uid), message_thread_id=ALLOWED_THREAD_ID or None)
        await redis.hset(game_key, "tie_panel_msg_id", str(msg.message_id))


async def process_dice_value(chat_id: int, game_id: str, uid: str, dice_value: int, msg_id: int = None):
    game_key = f"game:{game_id}"

    # 核心互斥锁，确保数组操作原子性
    async with get_lock(game_id):
        fresh_data = await redis.hgetall(game_key)
        if not fresh_data:
            return

        status = fresh_data.get("status")

        # --- 极严格回合校验（防乱掷与多投） ---
        is_my_turn = False
        if status == "rolling":
            queue = json.loads(fresh_data.get("queue", "[]"))
            if queue and queue[0] == uid:
                is_my_turn = True
        elif status == "tie_break":
            tie_queue = json.loads(fresh_data.get("tie_queue", "[]"))
            g_idx = int(fresh_data.get("current_tie_group", "0"))
            t_idx = int(fresh_data.get("current_turn", "0"))
            if g_idx < len(tie_queue) and t_idx < len(tie_queue[g_idx]) and tie_queue[g_idx][t_idx] == uid:
                is_my_turn = True

        rolls = json.loads(fresh_data.get("rolls", "{}"))
        target_lengths = json.loads(fresh_data.get("target_lengths", "{}"))
        target = target_lengths.get(uid, 0)
        current_rolls_len = len(rolls.get(uid, []))

        # 核心防线：已投完的玩家，任何骰子（包括逃跑-1）都不再计入
        if current_rolls_len >= target:
            if msg_id and dice_value != -1:
                asyncio.create_task(delete_msg_by_id(chat_id, msg_id))
            return
        # 非逃跑骰子额外检查回合
        if dice_value != -1 and not is_my_turn:
            if msg_id:
                asyncio.create_task(delete_msg_by_id(chat_id, msg_id))
            return
        # --------------------------------------

        rolls.setdefault(uid, []).append(dice_value)
        await redis.hset(game_key, "rolls", json.dumps(rolls))
        await redis.hset(game_key, "last_action_time", str(time.time()))
        await redis.hset(game_key, f"warned_{uid}", "0")

        names = json.loads(fresh_data["names"])
        _dir = fresh_data.get("direction", "?")
        _amt = float(fresh_data.get("amount", 0))

        if status == "rolling":
            queue = json.loads(fresh_data.get("queue", "[]"))
            if len(rolls[uid]) >= target:
                msg_ids = await redis.lrange(f"game_msgs:{game_id}", -1, -1)
                if msg_ids:
                    try:
                        await bot.edit_message_reply_markup(chat_id, int(msg_ids[0]), reply_markup=None)
                    except:
                        pass

                if queue and queue[0] == uid:
                    queue.pop(0)

                if queue:
                    next_uid = queue[0]
                    rem = target_lengths[next_uid] - len(rolls.get(next_uid, []))
                    await redis.hset(game_key, "queue", json.dumps(queue))

                    finished_text = []
                    all_players = json.loads(fresh_data["players"])
                    for p in all_players:
                        if p not in queue and len(rolls.get(p, [])) >= target_lengths[p]:
                            if -1 in rolls.get(p, []):
                                finished_text.append(f"{safe_html(names[p])}:逃跑")
                            else:
                                sc, _ = calculate_score_with_details(rolls[p])
                                finished_text.append(f"{safe_html(names[p])}:{sc}点")

                    status_str = " | ".join(finished_text)
                    waiting_names = [safe_html(names[p]) for p in queue[1:]]
                    waiting_str = f"\n⏳ 等候：{'、'.join(waiting_names)}" if waiting_names else ""
                    prompt_text = f"✅ 赛况（比{_dir}｜{_amt:g}/人｜{len(all_players)}人局）：{status_str}{waiting_str}\n\n👉 轮到 {get_mention(next_uid, names[next_uid])} 投掷 <b>{rem}</b> 颗！"
                    for _retry in range(3):
                        try:
                            msg = await bot.send_message(chat_id, prompt_text, reply_markup=get_roll_keyboard(game_id, next_uid), message_thread_id=ALLOWED_THREAD_ID or None)
                            await redis.rpush(f"game_msgs:{game_id}", msg.message_id)
                            break
                        except Exception:
                            if _retry < 2:
                                await asyncio.sleep(1)
                            else:
                                logging.warning(f"[game] 发送下一位投掷提示失败 game={game_id} next={next_uid}")
                else:
                    await process_round_end_or_settle(chat_id, game_id, await redis.hgetall(game_key))

        elif status == "tie_break":
            tie_queue = json.loads(fresh_data["tie_queue"])
            g_idx = int(fresh_data["current_tie_group"])
            t_idx = int(fresh_data["current_turn"])

            if len(rolls[uid]) >= target:
                msg_ids = await redis.lrange(f"game_msgs:{game_id}", -1, -1)
                if msg_ids:
                    try:
                        await bot.edit_message_reply_markup(chat_id, int(msg_ids[0]), reply_markup=None)
                    except:
                        pass

                if -1 in rolls[uid]:
                    sc_text = "被判定为 <b>逃跑</b>"
                else:
                    sc, _ = calculate_score_with_details(rolls[uid])
                    sc_text = f"得 <b>{sc}</b> 点"

                next_turn = t_idx + 1
                if next_turn < len(tie_queue[g_idx]):
                    await redis.hset(game_key, "current_turn", str(next_turn))
                    next_uid = tie_queue[g_idx][next_turn]
                    tie_prompt = f"✅ {safe_html(names[uid])} 加赛{sc_text}！（比{_dir}｜{_amt:g}/人）\n👉 同组并列：{get_mention(next_uid, names[next_uid])} 补投！"
                    for _retry in range(3):
                        try:
                            msg = await bot.send_message(chat_id, tie_prompt, reply_markup=get_roll_keyboard(game_id, next_uid), message_thread_id=ALLOWED_THREAD_ID or None)
                            await redis.rpush(f"game_msgs:{game_id}", msg.message_id)
                            break
                        except Exception:
                            if _retry < 2:
                                await asyncio.sleep(1)
                            else:
                                logging.warning(f"[game] 加赛提示发送失败 game={game_id}")
                else:
                    next_group = g_idx + 1
                    if next_group < len(tie_queue):
                        await redis.hset(game_key, "current_tie_group", str(next_group))
                        await redis.hset(game_key, "current_turn", "0")
                        first_next_uid = tie_queue[next_group][0]
                        tie_prompt2 = f"✅ {safe_html(names[uid])} 加赛{sc_text}！（比{_dir}｜{_amt:g}/人）\n👉 下一组并列：{get_mention(first_next_uid, names[first_next_uid])} 补投！"
                        for _retry in range(3):
                            try:
                                msg = await bot.send_message(chat_id, tie_prompt2, reply_markup=get_roll_keyboard(game_id, first_next_uid), message_thread_id=ALLOWED_THREAD_ID or None)
                                await redis.rpush(f"game_msgs:{game_id}", msg.message_id)
                                break
                            except Exception:
                                if _retry < 2:
                                    await asyncio.sleep(1)
                                else:
                                    logging.warning(f"[game] 加赛提示发送失败 game={game_id}")
                    else:
                        await process_round_end_or_settle(chat_id, game_id, await redis.hgetall(game_key))
