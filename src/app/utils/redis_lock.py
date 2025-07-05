import time
import redis
from app.config import Setting

redis_client = redis.Redis(
    host=Setting.REDIS_HOST,
    port=Setting.REDIS_PORT,
    db=Setting.REDIS_BROKER_DB,
    decode_responses=True
)

def redis_lock(key: str, timeout: int = 600):
    now = int(time.time())
    if redis_client.setnx(key, now + timeout):
        redis_client.expire(key, timeout)
        return True
    else:
        expire_at = redis_client.get(key)
        if expire_at and int(expire_at) < now:
            redis_client.set(key, now + timeout)
            return True
    return False