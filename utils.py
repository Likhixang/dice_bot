import asyncio
import html
import logging

from aiogram import types
from aiogram.methods import PinChatMessage

from core import bot, redis
from config import ALLOWED_THREAD_ID


def safe_html(text: str) -> str:
    return html.escape(str(text))


def get_mention(user_id, name):
    return f"<a href='tg://user?id={user_id}'>{safe_html(name)}</a>"


# 双栖兼容倒序查询（彻底解决版本弃用导致的 /rank 崩溃）
async def safe_zrevrange(key, start, end, withscores=False):
    if hasattr(redis, 'zrevrange'):
        return await redis.zrevrange(key, start, end, withscores=withscores)
    else:
        return await redis.zrange(key, start, end, desc=True, withscores=withscores)


async def safe_zrange(key, start, end, withscores=False):
    return await redis.zrange(key, start, end, withscores=withscores)


async def delete_msgs(msgs: list, delay: int = 10):
    if delay > 0:
        await asyncio.sleep(delay)
    for m in msgs:
        try:
            await m.delete()
        except:
            pass


async def delete_msg_by_id(chat_id: int, msg_id: int, delay: int = 0):
    if delay > 0:
        await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, msg_id)
    except:
        pass


async def delete_msgs_by_ids(chat_id: int, msg_ids: list, delay: int = 0):
    if delay > 0:
        await asyncio.sleep(delay)
    for mid in msg_ids:
        try:
            await bot.delete_message(chat_id, int(mid))
        except:
            pass


async def unpin_and_delete_after(chat_id: int, msg_id: int, delay: float, redis_key: str = None):
    await asyncio.sleep(delay)
    try:
        await bot.unpin_chat_message(chat_id=chat_id, message_id=msg_id)
    except:
        pass
    try:
        await bot.delete_message(chat_id=chat_id, message_id=msg_id)
    except:
        pass
    if redis_key:
        try:
            await redis.delete(redis_key)
        except:
            pass


async def reply_and_auto_delete(message: types.Message, text: str, delay: int = 10):
    bot_msg = await message.reply(text)
    asyncio.create_task(delete_msgs([message, bot_msg], delay))


async def pin_in_topic(chat_id: int, message_id: int, disable_notification: bool = False):
    """pin_chat_message 的话题感知包装：ALLOWED_THREAD_ID 存在时带上 message_thread_id"""
    kwargs = {"chat_id": chat_id, "message_id": message_id, "disable_notification": disable_notification}
    if ALLOWED_THREAD_ID:
        kwargs["message_thread_id"] = ALLOWED_THREAD_ID
    await bot(PinChatMessage(**kwargs))
