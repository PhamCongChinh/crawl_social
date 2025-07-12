import asyncio
from typing import Callable, Awaitable, Any

async def run_async_limited(
    tasks: list[Callable[[], Awaitable[Any]]],
    concurrency: int = 1
) -> list[Any]:
    """
    Chạy list các async task (callable không đối số), giới hạn số lượng đồng thời.

    Args:
        tasks: List các async callable dạng lambda: func(...)
        concurrency: Số lượng task tối đa chạy đồng thời.

    Returns:
        List kết quả của các task, giữ đúng thứ tự.
    """
    sem = asyncio.Semaphore(concurrency)

    async def sem_wrap(task_func: Callable[[], Awaitable[Any]]):
        async with sem:
            return await task_func()

    return await asyncio.gather(*[sem_wrap(task) for task in tasks])


async def run_async_limited_with_retry(
    tasks: list[Callable[[], Awaitable[Any]]],
    concurrency: int = 1,
    max_retries: int = 1,
    backoff_seconds: float = 1.0,
    timeout_seconds=10  # timeout sau 10s nếu chưa xong
    
) -> list[Any]:
    """
    Chạy async task có giới hạn đồng thời, retry nếu lỗi, timeout từng task.

    Args:
        tasks: List các async callable (dạng lambda: func(...))
        concurrency: Số task chạy song song tối đa.
        max_retries: Số lần thử lại nếu task lỗi.
        backoff_seconds: Delay tăng dần giữa lần retry.
        timeout_seconds: Timeout tối đa cho mỗi task.

    Returns:
        List kết quả task (thứ tự giữ nguyên). Nếu lỗi: {"error": ..., "ok": False}
    """
    sem = asyncio.Semaphore(concurrency)

    async def retry_task(task_func: Callable[[], Awaitable[Any]]):
        for attempt in range(max_retries):
            try:
                async with sem:
                    return await asyncio.wait_for(
                        task_func(), timeout=timeout_seconds
                    )
            except Exception as e:
                if attempt == max_retries - 1:
                    return {"error": str(e), "ok": False}
                await asyncio.sleep(backoff_seconds * (attempt + 1))

    return await asyncio.gather(*[retry_task(task) for task in tasks])
