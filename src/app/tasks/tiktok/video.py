import asyncio
import json
from pathlib import Path
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

# output = Path(__file__).parent / "results"
# output.mkdir(exist_ok=True)

error_keywords = []
output = Path("logs")
output.mkdir(parents=True, exist_ok=True)
error_log_file = output / "keyword_error.json"

# output = Path("logs")
# output.mkdir(parents=True, exist_ok=True)
# error_log_file = output / "keyword_error.json"


async def save_to_mongo(data: List[dict], source: dict):
    try:
        result = await VideoService.upsert_channels_bulk_channel(data, source)
        log.info(f"[Database] Đã lưu: {result['inserted']} mới | {result['matched']} khớp | {result['modified']} cập nhật | {result['upserted']} upsert ID")
            
    except Exception as e:
        log.error(f"[ERROR] Lỗi upsert video: {e}")

async def save_to_mongo_keyword(data: List[dict], source: dict):
    try:
        await VideoService.upsert_channels_bulk_keyword(data, source)
    except Exception as e:
        log.error(f"[ERROR] Lỗi upsert video: {e}")
        
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
            log.info(f"[Classified] Tổng số url: {len(sources)}")

            chunk_size = constant.CHUNK_SIZE
            for i in range(0, len(sources), chunk_size):
                chunk = sources[i:i + chunk_size]
                source_dicts = [s.model_dump(mode="json") for s in chunk]
                source_names = [s.source_name for s in chunk]
                countdown = random.randint(1, 4)

                log.info(f"[{job_id}] Gửi batch {i//chunk_size + 1}: {source_names}")
                crawl_video_batch_classified.apply_async(
                    kwargs={"source_dicts": source_dicts, "job_id": job_id},
                    queue="tiktok_videos",
                    countdown=countdown,
                )
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
            sem = asyncio.Semaphore(constant.CONCURRENCY)  # chỉ chạy n video cùng lúc
            async def crawl_one(source):
                async with sem:
                    try:
                        log.info(f"---------------------------------------------------------------------")
                        log.info(f"[{job_id}] Đang cào: {source['source_url']}")
                        data = await scrape_channel(source['source_url'])
                        await save_to_mongo(data=data, source=source)
                        log.info(f"[{job_id}] Cào xong: {source['source_name']} ({len(data)} bài viết)")
                        return {
                            "url": source['source_url'],
                            "source_name": source['source_name'],
                            "ok": True,
                            "data_len": len(data)
                        }
                    except Exception as e:
                        log.warning(f"[{job_id}] Lỗi: {source['source_url']} → {e}")
                        raise
                        
            log.info(f"[{job_id}] Bắt đầu cào batch {len(source_dicts)} nguồn...")
            results = await asyncio.gather(*[crawl_one(source) for source in source_dicts])

            # Tổng kết kết quả
            log.info(f"[{job_id}] Kết quả batch:")
            for r in results:
                if r["ok"]:
                    log.info(f"{r['url']} ({r.get('data_len', '?')} bài viết)")
                else:
                    log.info(f"{r['url']} → {r.get('error')}")

            success_count = sum(1 for r in results if r["ok"])
            fail_count = len(results) - success_count

            return {
                "job_id": job_id,
                "total": len(results),
                "success": success_count,
                "failed": fail_count,
                "results": results
            }
    except Exception as e:
        log.error(f"Lỗi cào batch: {e}")


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
            log.info(f"[Unclassified] Tổng số url: {len(sources)}")

            chunk_size = constant.CHUNK_SIZE
            for i in range(0, len(sources), chunk_size):
                chunk = sources[i:i + chunk_size]
                source_dicts = [s.model_dump(mode="json") for s in chunk]
                source_names = [s.source_name for s in chunk]
                countdown = random.randint(1, 4)

                log.info(f"[{job_id}] Gửi batch {i//chunk_size + 1}: {source_names}")
                crawl_video_batch_unclassified.apply_async(
                    kwargs={"source_dicts": source_dicts, "job_id": job_id},
                    queue="tiktok_videos",
                    countdown=countdown,
                )
    except Exception as e:
        log.error(f"Lỗi crawl_video_all: {e}")

@celery_app.task(
    queue="tiktok_videos",
    name="app.tasks.tiktok.video.crawl_video_batch_unclassified"
)
def crawl_video_batch_unclassified(source_dicts: list[dict], job_id: str = None):
    async_to_sync(_crawl_video_batch_unclassified)(source_dicts, job_id)

async def _crawl_video_batch_unclassified(source_dicts: list[dict], job_id: str = None):
    try:
        async with lifespan_mongo():
            sem = asyncio.Semaphore(constant.CONCURRENCY)  # chỉ chạy n video cùng lúc
            async def crawl_one(source):
                async with sem:
                    try:
                        log.info(f"---------------------------------------------------------------------")
                        log.info(f"[{job_id}] Đang cào: {source['source_url']}")
                        data = await scrape_channel(source['source_url'])
                        await save_to_mongo(data=data, source=source)
                        log.info(f"[{job_id}] Cào xong: {source['source_name']} ({len(data)} bài viết)")
                        return {
                            "url": source['source_url'],
                            "source_name": source['source_name'],
                            "ok": True,
                            "data_len": len(data)
                        }
                    except Exception as e:
                        log.warning(f"[{job_id}] Lỗi: {source['source_url']} → {e}")
                        raise
                        
            log.info(f"[{job_id}] Bắt đầu cào batch {len(source_dicts)} nguồn...")
            results = await asyncio.gather(*[crawl_one(source) for source in source_dicts])
            # Tổng kết kết quả
            log.info(f"[{job_id}] Kết quả batch:")
            for r in results:
                if r["ok"]:
                    log.info(f"{r['url']} ({r.get('data_len', '?')} bài viết)")
                else:
                    log.info(f"{r['url']} → {r.get('error')}")

            success_count = sum(1 for r in results if r["ok"])
            fail_count = len(results) - success_count

            return {
                "job_id": job_id,
                "total": len(results),
                "success": success_count,
                "failed": fail_count,
                "results": results
            }
    except Exception as e:
        log.error(f"Lỗi crawl_video_all: {e}")

# Keyword
@celery_app.task(
    queue="tiktok_keywords",
    name="app.tasks.tiktok.video.crawl_video_all_keyword"
)
def crawl_video_all_keyword(job_id: str):
    async_to_sync(_crawl_video_all_keyword)(job_id)

async def _crawl_video_all_keyword(job_id: str):
    try:
        async with lifespan_mongo():
            keywords = await SearchService.get_keywords() # Lấy url ưu tiên
            log.info(f"[Keywords] Tổng số từ khóa: {len(keywords)}")

            chunk_size = constant.CHUNK_SIZE
            for i in range(0, len(keywords), chunk_size):
                chunk = keywords[i:i + chunk_size]
                source_dicts = [k.model_dump(mode="json") for k in chunk]
                countdown = random.randint(1, 4)

                log.info(f"[{job_id}] Gửi batch {i//chunk_size + 1}: {source_dicts}")
                crawl_video_batch_keyword.apply_async(
                    kwargs={"source_dicts": source_dicts, "job_id": job_id},
                    queue="tiktok_keywords",
                    countdown=countdown,
                )
    except Exception as e:
        log.error(f"❌ Lỗi crawl_video_all: {e}")

@celery_app.task(
    queue="tiktok_keywords",
    name="app.tasks.tiktok.video.crawl_video_batch_keyword"
)
def crawl_video_batch_keyword(source_dicts: list[dict], job_id: str = None):
    async_to_sync(_crawl_video_batch_keyword)(source_dicts, job_id)

async def _crawl_video_batch_keyword(source_dicts: list[dict], job_id: str = None):
    try:
        async with lifespan_mongo():
            sem = asyncio.Semaphore(constant.CONCURRENCY_KEYWORD)  # chỉ chạy n video cùng lúc
            async def crawl_one(source):
                async with sem:
                    try:
                        log.info(f"---------------------------------------------------------------------")
                        log.info(f"[{job_id}] Đang cào: {source['keyword']}")
                        data = await scrape_search(source['keyword'], max_search=12)
                        await save_to_mongo_keyword(data=data, source=source)
                        log.info(f"[{job_id}] Cào xong: {source['keyword']} ({len(data)} items)")
                        return {
                            "keyword": source['keyword'],
                            "ok": True,
                            "data_len": len(data)
                        }
                    except Exception as e:
                        log.warning(f"[{job_id}] Lỗi: {source['keyword']} → {e}")

                        # with open(output.joinpath("keyword_error.json"), "w", encoding="utf-8") as file:
                        #     json.dump(source['keyword'], file, indent=2, ensure_ascii=False)
                        error_keywords.append(source['keyword'])
                        return {
                            "keyword": source['keyword'],
                            "ok": False,
                            "error": str(e)
                        }
            log.info(f"[{job_id}] Bắt đầu cào batch {len(source_dicts)} từ khóa...")
            results = await asyncio.gather(*[crawl_one(source) for source in source_dicts])
            log.info(f"[{job_id}] Batch done: {results}")
    except Exception as e:
        log.error(f"❌ Lỗi crawl_video_all: {e}")