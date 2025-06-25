import logging

from app.utils import log_job_timing
log = logging.getLogger(__name__)

from app.config import mongo_connection
from app.worker import celery_app

from app.tasks.tiktok.channel import crawl_tiktok_channels
from app.tasks.tiktok.post import crawl_tiktok_posts

@celery_app.task(
    name="app.tasks.tiktok.chain_channel_post",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3}
)
def crawl_channel_then_post(job_id: str, channel_id: str):
    log.info(f"🚀 [START] {job_id} - Crawl channel → post cho {channel_id}")
    log_job_timing(job_id)
    try:
        # Step 1: Crawl channels
        log.info(f"📡 Đang crawl channel {channel_id}")
        result1 = crawl_tiktok_channels(job_id, channel_id)
        if not result1 or result1.get("status") != "success":
            raise Exception(f"❌ Crawl channel thất bại: {result1}")

        # Step 2: Crawl posts
        log.info(f"📨 Channel xong. Đang crawl post cho {channel_id}")
        result2 = crawl_tiktok_posts(job_id, channel_id)
        if not result2 or result2.get("status") != "success":
            raise Exception(f"❌ Crawl post thất bại: {result2}")

        log.info(f"✅ [DONE] {job_id} - Crawl channel + post hoàn tất cho {channel_id}")
        return {
            "status": "success",
            "job_id": job_id,
            "channel_id": channel_id
        }

    except Exception as e:
        log.error(f"❌ [ERROR] {job_id} - Crawl thất bại cho {channel_id}: {e}")
        return {
            "status": "error",
            "message": str(e),
            "type": type(e).__name__,
            "job_id": job_id,
            "channel_id": channel_id
        }