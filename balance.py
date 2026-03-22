import datetime

from config import TZ_BJ
from core import redis


async def get_or_init_balance(uid: str) -> float:
    key = f"user_balance:{uid}"
    if not await redis.exists(key):
        await redis.set(key, 20000.0)
        return 20000.0
    return round(float(await redis.get(key)), 2)


async def update_balance(uid: str, amount: float) -> float:
    if amount == 0:
        return await get_or_init_balance(uid)
    await get_or_init_balance(uid)
    val = await redis.incrbyfloat(f"user_balance:{uid}", round(amount, 2))
    return round(val, 2)


def get_period_keys():
    now = datetime.datetime.now(TZ_BJ)
    return now.strftime("%Y%m%d"), now.strftime("%Y-%W"), now.strftime("%Y%m")


async def release_user_locks(players: list):
    if not players:
        return
    keys = [f"user_game:{uid}" for uid in players]
    await redis.delete(*keys)
