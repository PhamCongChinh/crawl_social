from contextlib import asynccontextmanager
import aioredlock

redis_lock = aioredlock.Aioredlock([{"host": "redis_server", "port": 6379}])

@asynccontextmanager
async def acquire_scrapfly_lock(session_id: str, timeout=60000):
    lock_name = f"scrapfly-session-lock:{session_id}"
    lock = await redis_lock.lock(lock_name, lock_timeout=timeout)
    try:
        yield
    finally:
        await redis_lock.unlock(lock)
