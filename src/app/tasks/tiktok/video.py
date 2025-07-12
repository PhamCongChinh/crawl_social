import asyncio
import random
from typing import List
from app.core.lifespan_mongo import lifespan_mongo
from app.modules.tiktok_scraper.scrapers.channel import scrape_channel
from app.modules.tiktok_scraper.scrapers.search import scrape_search
from app.modules.tiktok_scraper.services.search import SearchService
from app.modules.tiktok_scraper.services.source import SourceService
from app.modules.tiktok_scraper.services.video import VideoService
from app.worker import celery_app
from app.config import constant

from asgiref.sync import async_to_sync

import logging
log = logging.getLogger(__name__)


async def save_to_mongo(data: List[dict], source: dict):
    try:
        await VideoService.upsert_channels_bulk_channel(data, source)
    except Exception as e:
        log.error(f"❌ Lỗi upsert video: {e}")

async def save_to_mongo_keyword(data: List[dict], source: dict):
    try:
        await VideoService.upsert_channels_bulk_keyword(data, source)
    except Exception as e:
        log.error(f"❌ Lỗi upsert video: {e}")
# Classified
@celery_app.task(
    queue="tiktok_videos",
    name="app.tasks.tiktok.video.crawl_video_all_classified"
)
def crawl_video_all_classified(job_id: str):
    async_to_sync(_crawl_video_all_classified)(job_id)
    
async def _crawl_video_all_classified(job_id: str):
    try:
        async with lifespan_mongo():
            sources = await SourceService.get_sources_classified() # Lấy url ưu tiên
            log.info(f"📦 Tổng số url: {len(sources)}")

            chunk_size = constant.CHUNK_SIZE
            for i in range(0, len(sources), chunk_size):
                chunk = sources[i:i + chunk_size]
                source_dicts = [s.model_dump(mode="json") for s in chunk]
                countdown = random.randint(1, 4)

                log.info(f"[{job_id}] 🚀 Gửi batch {i//chunk_size + 1}: {source_dicts}")
                crawl_video_batch_classified.apply_async(
                    kwargs={"source_dicts": source_dicts, "job_id": job_id},
                    queue="tiktok_videos",
                    countdown=countdown,
                )
                break
    except Exception as e:
        log.error(f"❌ Lỗi crawl_video_all: {e}")

@celery_app.task(
    queue="tiktok_videos",
    name="app.tasks.tiktok.video.crawl_video_batch_classified"
)
def crawl_video_batch_classified(source_dicts: list[dict], job_id: str = None):
    async_to_sync(_crawl_video_batch_classified)(source_dicts, job_id)

async def _crawl_video_batch_classified(source_dicts: list[dict], job_id: str = None):
    try:
        async with lifespan_mongo():
            sem = asyncio.Semaphore(constant.CONCURRENCY)  # chỉ chạy 2 video cùng lúc
            async def crawl_one(source):
                async with sem:
                    for attempt in range(3):
                        try:
                            log.info(f"[{job_id}] 🔍 Crawling: {source['source_url']} | attempt {attempt+1}")
                            data = await scrape_channel(source['source_url'])
                            log.info(f"[{job_id}] ✅ Done: {source['source_url']} ({len(data)} items)")
                            await save_to_mongo(data=data, source=source)
                            return {"url": source['source_url'], "ok": True, "data_len": len(data)}
                        except Exception as e:
                            log.warning(f"[{job_id}] ❌ Failed: {source['source_url']} | attempt {attempt+1} → {e}")
                            if attempt == 2:
                                return {"url": source['source_url'], "ok": False, "error": str(e)}
                            await asyncio.sleep(2 + attempt)
                    log.info(f"[{job_id}] ✅ Done: {source['source_url']} ({len(data)} items)")
                    return {"url": source['source_url'], "ok": True}
            results = await asyncio.gather(*[crawl_one(source) for source in source_dicts])
            log.info(f"[{job_id}] 🧾 Batch done: {results}")
    except Exception as e:
        log.error(f"❌ Lỗi crawl_video_all: {e}")


# Unclassified
@celery_app.task(
    queue="tiktok_videos",
    name="app.tasks.tiktok.video.crawl_video_all_unclassified"
)
def crawl_video_all_unclassified(job_id: str):
    async_to_sync(_crawl_video_all_unclassified)(job_id)
    
async def _crawl_video_all_unclassified(job_id: str):
    try:
        async with lifespan_mongo():
            sources = await SourceService.get_sources_unclassified() # Lấy url ưu tiên
            log.info(f"📦 Tổng số url: {len(sources)}")

            chunk_size = constant.CHUNK_SIZE
            for i in range(0, len(sources), chunk_size):
                chunk = sources[i:i + chunk_size]
                source_dicts = [s.model_dump(mode="json") for s in chunk]
                countdown = random.randint(1, 4)

                log.info(f"[{job_id}] 🚀 Gửi batch {i//chunk_size + 1}: {source_dicts}")
                crawl_video_batch_unclassified.apply_async(
                    kwargs={"source_dicts": source_dicts, "job_id": job_id},
                    queue="tiktok_videos",
                    countdown=countdown,
                )
                break
    except Exception as e:
        log.error(f"❌ Lỗi crawl_video_all: {e}")

@celery_app.task(
    queue="tiktok_videos",
    name="app.tasks.tiktok.video.crawl_video_batch_unclassified"
)
def crawl_video_batch_unclassified(source_dicts: list[dict], job_id: str = None):
    async_to_sync(_crawl_video_batch_unclassified)(source_dicts, job_id)

async def _crawl_video_batch_unclassified(source_dicts: list[dict], job_id: str = None):
    try:
        async with lifespan_mongo():
            sem = asyncio.Semaphore(constant.CONCURRENCY)  # chỉ chạy 2 video cùng lúc
            async def crawl_one(source):
                async with sem:
                    for attempt in range(3):
                        try:
                            log.info(f"[{job_id}] 🔍 Crawling: {source['source_url']} | attempt {attempt+1}")
                            data = await scrape_channel(source['source_url'])
                            log.info(f"[{job_id}] ✅ Done: {source['source_url']} ({len(data)} items)")
                            await save_to_mongo(data=data, source=source)
                            return {"url": source['source_url'], "ok": True, "data_len": len(data)}
                        except Exception as e:
                            log.warning(f"[{job_id}] ❌ Failed: {source['source_url']} | attempt {attempt+1} → {e}")
                            if attempt == 2:
                                return {"url": source['source_url'], "ok": False, "error": str(e)}
                            await asyncio.sleep(2 + attempt)
                    log.info(f"[{job_id}] ✅ Done: {source['source_url']} ({len(data)} items)")
                    return {"url": source['source_url'], "ok": True}
            results = await asyncio.gather(*[crawl_one(source) for source in source_dicts])
            log.info(f"[{job_id}] 🧾 Batch done: {results}")
    except Exception as e:
        log.error(f"❌ Lỗi crawl_video_all: {e}")

# Keyword
@celery_app.task(
    queue="tiktok_videos",
    name="app.tasks.tiktok.video.crawl_video_all_keyword"
)
def crawl_video_all_keyword(job_id: str):
    async_to_sync(_crawl_video_all_keyword)(job_id)

async def _crawl_video_all_keyword(job_id: str):
    try:
        async with lifespan_mongo():
            keywords = await SearchService.get_keywords() # Lấy url ưu tiên
            log.info(f"📦 Tổng số từ khóa: {len(keywords)}")

            chunk_size = constant.CHUNK_SIZE
            for i in range(0, len(keywords), chunk_size):
                chunk = keywords[i:i + chunk_size]
                source_dicts = [k.model_dump(mode="json") for k in chunk]
                countdown = random.randint(1, 4)

                log.info(f"[{job_id}] 🚀 Gửi batch {i//chunk_size + 1}: {source_dicts}")
                crawl_video_batch_keyword.apply_async(
                    kwargs={"source_dicts": source_dicts, "job_id": job_id},
                    queue="tiktok_videos",
                    countdown=countdown,
                )
                break
    except Exception as e:
        log.error(f"❌ Lỗi crawl_video_all: {e}")

@celery_app.task(
    queue="tiktok_videos",
    name="app.tasks.tiktok.video.crawl_video_batch_keyword"
)
def crawl_video_batch_keyword(source_dicts: list[dict], job_id: str = None):
    async_to_sync(_crawl_video_batch_keyword)(source_dicts, job_id)

async def _crawl_video_batch_keyword(source_dicts: list[dict], job_id: str = None):
    try:
        async with lifespan_mongo():
            sem = asyncio.Semaphore(constant.CONCURRENCY)  # chỉ chạy 2 video cùng lúc
            async def crawl_one(source):
                async with sem:
                    for attempt in range(3):
                        try:
                            log.info(f"[{job_id}] 🔍 Crawling: {source['keyword']} | attempt {attempt+1}")
                            data = await scrape_search(source['keyword'], max_search=12)
                            log.info(f"[{job_id}] ✅ Done: {source['keyword']} ({len(data)} items)")
                            await save_to_mongo_keyword(data=data, source=source)
                            return {"Keyword": source['keyword'], "ok": True, "data_len": len(data)}
                        except Exception as e:
                            log.warning(f"[{job_id}] ❌ Failed: {source['keyword']} | attempt {attempt+1} → {e}")
                            if attempt == 2:
                                return {"url": source['keyword'], "ok": False, "error": str(e)}
                            await asyncio.sleep(2 + attempt)
                    log.info(f"[{job_id}] ✅ Done: {source['keyword']} ({len(data)} items)")
                    return {"Keyword": source['keyword'], "ok": True}
            results = await asyncio.gather(*[crawl_one(source) for source in source_dicts])
            log.info(f"[{job_id}] 🧾 Batch done: {results}")
    except Exception as e:
        log.error(f"❌ Lỗi crawl_video_all: {e}")