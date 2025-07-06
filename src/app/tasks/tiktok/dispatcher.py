import asyncio
from celery import shared_task
import redis

from app.modules.tiktok_scraper.models.channel import ChannelModel
from app.config import mongo_connection
from celery import group
from beanie.operators import In

from app.tasks.tiktok.worker import crawl_video_batch

BATCH_SIZE = 50
LOCK_EXPIRE = 600
import logging
log = logging.getLogger(__name__)

from app.config.settings import Settings

@shared_task
def dispatch_video_batches():

    # r = redis.Redis.from_url("redis://redis_server:6379/0")  # sửa URL nếu Redis khác

    # if not r.set("lock:dispatch_video_batches", "1", nx=True, ex=LOCK_EXPIRE):
    #     log.warning("⛔ Task dispatch đang chạy, bỏ qua lần này.")
    #     return

    async def inner():
        await mongo_connection.connect()
        # videos = await ChannelModel.find(ChannelModel.status == "pending").limit(max_video).to_list()
        videos = await ChannelModel.find(
            (ChannelModel.status == "pending") & (ChannelModel.create_time > 1750525200)
        ).to_list()
        ids = [str(v.id) for v in videos]
        await ChannelModel.find(In(ChannelModel.id, ids)).update_many({"$set": {"status": "processing"}})
        video_dicts = [v.model_dump() for v in videos]
        batches = [video_dicts[i:i + BATCH_SIZE] for i in range(0, len(video_dicts), BATCH_SIZE)]
        log.info(f"📦 Tổng cộng {len(videos)} video, chia thành {len(batches)} batch (mỗi batch {BATCH_SIZE} video)")
        group([crawl_video_batch.s(batch, i + 1, len(batches)) for i, batch in enumerate(batches)]).apply_async()

    try:
        asyncio.run(inner())
    except Exception as e:
        log.error(f"❌ Lỗi khi chạy task dispatch: {e}")
    # finally:
        # r.delete("lock:dispatch_video_batches")