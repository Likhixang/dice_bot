from aiogram import Bot, Dispatcher, types
from aiogram.filters import BaseFilter
from aiogram.client.default import DefaultBotProperties
from redis.asyncio import Redis

from config import TOKEN

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode='HTML'))
dp = Dispatcher()
redis = Redis(host='redis', port=6379, db=0, decode_responses=True)


class CleanTextFilter(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        if not message.entities:
            return True
        for ent in message.entities:
            if ent.type not in ["bot_command", "mention", "phone_number"]:
                return False
        return True
