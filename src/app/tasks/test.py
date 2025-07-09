import asyncio
from app.worker import celery_app
import logging
log = logging.getLogger(__name__)


@celery_app.task(
    name="app.tasks.tiktok.channel.count_down_1",
    queue="tiktok_platform",
)
def count_down_1(job_id: str, channel_id: str):
    print("CHANNEL bắt đầu")
    asyncio.run(async_countdown(1, "CHANNEL"))


@celery_app.task(
    name="app.tasks.tiktok.channel.count_down_2",
    queue="tiktok_platform",
)
def count_down_2(job_id: str, channel_id: str):
    print("POST bắt đầu")
    asyncio.run(async_countdown(1,"POST"))


@celery_app.task(
    name="app.tasks.tiktok.channel.count_down_3",
    queue="thread_platform",
)
def count_down_3(job_id: str, channel_id: str):
    print("COMMENT bắt đầu")
    asyncio.run(async_countdown(1, "COMMENT"), )


async def async_countdown(seconds: int, id: str):
    for i in range(seconds, 0, -1):
        print(f"⏳ {i}... - {id}")
        await asyncio.sleep(1)
    print("🚀 Bắt đầu!")