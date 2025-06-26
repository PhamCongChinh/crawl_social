import asyncio
from typing import Awaitable, List


async def limited_gather(tasks: List[Awaitable], limit: int = 3):
    """
    Chạy đồng thời tối đa `limit` coroutine cùng lúc.
    Giống asyncio.gather nhưng có giới hạn concurrency.

    :param tasks: Danh sách coroutine
    :param limit: Số lượng coroutine tối đa chạy song song
    :return: Kết quả giống như asyncio.gather
    """
    semaphore = asyncio.Semaphore(limit)

    async def sem_task(task: Awaitable):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(t) for t in tasks))