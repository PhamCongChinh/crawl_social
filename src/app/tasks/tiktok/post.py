import asyncio
from datetime import datetime
import json
import random
from typing import List
from zoneinfo import ZoneInfo

from bson import Int64
from app.core.lifespan_mongo import lifespan_mongo
from app.modules.elastic_search.request import RequestToES
from app.modules.elastic_search.service import postToESClassified, postToESUnclassified
from app.modules.tiktok_scraper.models.video import VideoModel
from app.modules.tiktok_scraper.scrapers.post import scrape_posts
from app.modules.tiktok_scraper.services.video import VideoService
from app.utils.delay import async_delay
from app.worker import celery_app
from beanie.operators import In, And
from app.config import constant
import logging
log = logging.getLogger(__name__)

from asgiref.sync import async_to_sync
from app.config import mongo_connection
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

BATCH_SIZE = 50

@celery_app.task(
    queue="tiktok_posts",
    name="app.tasks.tiktok.post.crawl_tiktok_all_posts"
)
def crawl_tiktok_all_posts(job_id: str):
    async_to_sync(_crawl_video_all_posts)(job_id)

async def _crawl_video_all_posts(job_id: str):
    try:
        async with lifespan_mongo():
            # 1. Upsert processing to pending
            updated = await VideoService.upsert_processing_to_pending()
            log.info(f"[POST] Đã cập nhật {updated.modified_count} video từ 'processing' → 'pending'")
            await async_delay(1,2)
            videos = await VideoService.get_videos_daily()
            log.info(f"[POST] Số video chưa crawl: {len(videos)}")

            # 1. Update status
            video_ids = [v.video_id for v in videos]
            await VideoModel.find(In(VideoModel.video_id, video_ids)).update_many({
                "$set": {"status": "processing"}
            })

            chunk_size = constant.CHUNK_SIZE_POST
            for i in range(0, len(videos), chunk_size):
                chunk = videos[i:i + chunk_size]
                video_dicts = [s.model_dump(mode="json") for s in chunk]
                countdown = random.randint(1, 4)

                log.info(f"[{job_id}] 🚀 Gửi batch {i//chunk_size + 1}: {len(video_dicts)} video")
                crawl_video_batch_posts.apply_async(
                    kwargs={"video_dicts": video_dicts, "job_id": job_id},
                    queue="tiktok_posts",
                    countdown=countdown,
                )
    except Exception as e:
        log.error(f"[ERROR] Lỗi cào bài viết: {e}", exc_info=True)
        raise

@celery_app.task(
    queue="tiktok_posts",
    name="app.tasks.tiktok.post.crawl_tiktok_batch_posts"
)
def crawl_video_batch_posts(video_dicts: list[dict], job_id: str = None):
    async_to_sync(_crawl_video_batch_posts)(video_dicts, job_id)

async def _crawl_video_batch_posts(video_dicts: list[dict], job_id: str = None):
    try:
        async with lifespan_mongo():
            videos = video_dicts
            data_list_classified = []
            data_list_unclassified = []
            for index, video in enumerate(videos):
                log.info(f"[{index+1}/{len(videos)}] {video['video_url']}")
                if video["org_id"] == 0:
                    data_list_unclassified.append(video)
                else:
                    data_list_classified.append(video)
            
            log.info(f"[POST] Tổng : {len(data_list_classified)} classified - {len(data_list_unclassified)} unclassified")
            # Crawl & post classified
            if data_list_classified:
                post_data_classified = await crawl_tiktok_post_list_direct_classified(data_list_classified)
                log.info(post_data_classified)
                if post_data_classified:
                    log.info(f"Đã thêm {len(post_data_classified)} video đã PHÂN LOẠI vào ElasticSearch")
                    result_classified = await postToESClassified(post_data_classified)
                    check_post_result(result_classified)

            # Crawl & post unclassified
            if data_list_unclassified:
                post_data_unclassified = await crawl_tiktok_post_list_direct_unclassified(data_list_unclassified)
                log.info(post_data_unclassified)
                if post_data_unclassified:
                    log.info(f"Đã thêm {len(post_data_unclassified)} video CHƯA PHÂN LOẠI vào ElasticSearch")
                    result_unclassified = await postToESUnclassified(post_data_unclassified)
                    check_post_result(result_unclassified)
                    
            log.info(f"[POST] Tổng số video đã lấy: {len(data_list_classified) + len(data_list_unclassified)}")
    except Exception as e:
        log.error(f"[ERROR] Lỗi cào bài viết: {e}", exc_info=True)
        raise

async def crawl_tiktok_post_list_direct_classified(channels: list[dict]):
    try:
        log.info(f"[CLASSIFIED] Tổng số channel: {len(channels)} classified")
        urls = [item["video_url"] for item in channels]
        data = await scrape_posts(urls)
        posts_data = []
        processed_ids = []
        for channel in channels:
            for post in data:
                if post.get("id") == channel["video_id"]:
                    flatten = RequestToES.flatten_post_data_classified(post, channel)
                    posts_data.append(flatten)
                    processed_ids.append(channel["video_id"])  # Lưu lại _id cần đánh dấu
        if processed_ids:
            await VideoModel.find(In(VideoModel.video_id, processed_ids)).update_many(
                {"$set": {"status": "done"}}
            )
        await async_delay(2, 4)  # Đảm bảo browser session trước shutdown
        return posts_data

    except Exception as e:
        log.error(e)
        raise

async def crawl_tiktok_post_list_direct_unclassified(channels: list[dict]):
    try:
        log.info(f"[UNCLASSIFIED] Tổng số channel: {len(channels)} unclassified")
        urls = [item["video_url"] for item in channels]
        posts_data = []
        data = await scrape_posts(urls)
        processed_ids = []
        for channel in channels:
            for post in data:
                if post.get("id") == channel["video_id"]:
                    flatten = RequestToES.flatten_post_data_unclassified(post, channel)
                    posts_data.append(flatten)
                    processed_ids.append(channel["video_id"])  # Lưu lại _id cần đánh dấu
        if processed_ids:
            await VideoModel.find(In(VideoModel.video_id, processed_ids)).update_many(
                {"$set": {"status": "done"}}
            )
        await async_delay(2, 4)  # Đảm bảo browser session trước shutdown
        return posts_data

    except Exception as e:
        log.error(e)
        raise



# Backdate
@celery_app.task(
    queue="tiktok_posts",
    name="app.tasks.tiktok.post.crawl_tiktok_all_posts_backdate"
)
def crawl_tiktok_all_posts_backdate(job_id: str, from_date: int, to_date: int):
    async_to_sync(_crawl_video_all_posts_backdate)(job_id, from_date, to_date)

async def _crawl_video_all_posts_backdate(job_id: str, from_date, to_date):
    try:
        async with lifespan_mongo():
            # 1. Upsert processing to pending
            updated = await VideoService.upsert_processing_to_pending()
            log.info(f"[BACKDATE] Đã cập nhật {updated.modified_count} video từ 'processing' → 'pending'")
            await async_delay(1,2)
            videos = await VideoService.get_videos_backdate(from_date=from_date, to_date=to_date)
            log.info(f"[BACKDATE] Số video backdate chưa crawl: {len(videos)} từ {from_date} đến {to_date}")

            # 2. Update status
            video_ids = [v.video_id for v in videos]
            await VideoModel.find(In(VideoModel.video_id, video_ids)).update_many({
                "$set": {"status": "processing"}
            })

            chunk_size = constant.CHUNK_SIZE_POST
            for i in range(0, len(videos), chunk_size):
                chunk = videos[i:i + chunk_size]
                video_dicts = [s.model_dump(mode="json") for s in chunk]
                countdown = random.randint(1, 4)

                log.info(f"[{job_id}] Gửi batch {i//chunk_size + 1}: {len(video_dicts)} video")
                crawl_video_batch_posts.apply_async(
                    kwargs={"video_dicts": video_dicts, "job_id": job_id},
                    queue="tiktok_posts",
                    countdown=countdown,
                )
    except Exception as e:
        log.error(f"[BACKDATE] Lỗi cào backdate: {e}", exc_info=True)
        raise


def check_post_result(response_data: dict):
    status_code = response_data.get("statusCode")
    is_success = response_data.get("isSuccess", False)
    errors = response_data.get("data", {}).get("errors", [])
    successes = response_data.get("data", {}).get("successes", [])

    if status_code != 200:
        log.error(f"HTTP Status Code lỗi: {status_code}")
    elif not is_success:
        log.error("API trả về isSuccess=False")
    elif errors:
        log.error(f"Có lỗi trong response: {errors}")
    else:
        log.info(f"POST thành công {len(successes)} bài post")
        for item in successes:
            url = item.get("url")
            log.info(f"Post: {url}")