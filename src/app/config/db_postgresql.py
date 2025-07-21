import asyncio
from app.config.settings import settings

import asyncpg

import logging
log = logging.getLogger(__name__)

class PostgresDB:
    def __init__(self):
        self.dsn = settings.postgres_url
        self.pool = None

    async def connect(self, retries: int = 3, delay: int = 2):
        for attempt in range(1, retries + 1):
            try:
                self.pool = await asyncpg.create_pool(dsn=self.dsn)
                log.info("[Database] PostgreSQL pool đã kết nối thành công")
                return
            except Exception as e:
                log.error(f"Số lần thử kết nối {attempt} thất bại: {e}")
                if attempt == retries:
                    log.error("Tất cả các lần thử đều thất bại, thoát.")
                    exit(1)
                await asyncio.sleep(delay)

    async def close(self):
        if self.pool:
            await self.pool.close()
            log.info("PostgreSQL kết nối đã đóng")

    async def fetch_all(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetch_one(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def execute(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)