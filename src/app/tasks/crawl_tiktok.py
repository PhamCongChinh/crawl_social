import asyncio
from app.modules.scheduler.models.jobs_log import JobLog
from app.modules.tiktok_scraper.api.channel import get_all_sources
from app.modules.tiktok_scraper.api.post import crawl_posts
from app.worker import celery_app

from app.config import mongo_connection

@celery_app.task(name="app.tasks.crawl_tiktok", bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def crawl_tiktok(self, job_id: str, channel_id: str):
    async def do_crawl():
        try:
            await mongo_connection.connect()

            print(f"[{job_id}] Crawling TikTok for channel {channel_id}")
            await get_all_sources()

            await crawl_posts()

            await JobLog(job_id=job_id, status="success", message="Crawl thành công").insert()

        except Exception as e:
            await JobLog(job_id=job_id, status="error", message=str(e)).insert()
            raise e  # để Celery tự retry

        finally:
            await mongo_connection.disconnect()  # 👈 chỉ chạy nếu connect thành công

    asyncio.run(do_crawl())  # ✅ chỉ gọi 1 lần