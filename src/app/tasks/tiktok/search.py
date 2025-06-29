import asyncio
import logging

from app.modules.tiktok_scraper.scrapers.search import scrape_search
log = logging.getLogger(__name__)

from app.config import mongo_connection
from app.worker import celery_app

@celery_app.task(
    name="app.tasks.tiktok.post.crawl_tiktok_search",
    bind=True
)
def crawl_tiktok_search(self, job_id: str, channel_id: str):
    print(f"Task search {job_id} - {channel_id}")
    async def do_crawl():
        try:
            await mongo_connection.connect()
            search = await scrape_search("hanoi fc", 1)
            print(search)
        except Exception as e:
            log.error(e)
    return asyncio.run(do_crawl())
