import asyncio
import random
import logging
logger = logging.getLogger(__name__)

""" Hàm delay bất đồng bộ để dùng trong crawl. Bật tắt log bằng tham số `log`. """

async def async_delay(min_sec: float = 1.0, max_sec: float = None, log: bool = True):
    """
    Hàm delay bất đồng bộ để dùng trong crawl.
    
    - Nếu chỉ truyền `min_sec`: delay đúng thời gian đó.
    - Nếu truyền cả `max_sec`: delay ngẫu nhiên từ min đến max.
    """
    delay_time = (
        random.uniform(min_sec, max_sec) if max_sec and max_sec > min_sec else min_sec
    )

    if log:
        logger.info(f"[DELAY] ⏱ Đang chờ {delay_time:.2f} giây...")

    await asyncio.sleep(delay_time)