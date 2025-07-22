from datetime import datetime
import json
from pathlib import Path
import traceback
from zoneinfo import ZoneInfo
from bson import Int64
from fastapi import APIRouter

# from app.modules.tiktok_scraper.models.channel import ChannelModel
from app.modules.tiktok_scraper.scrapers.post import scrape_posts
# from app.modules.tiktok_scraper.services.channel import ChannelService
from app.modules.tiktok_scraper.services.post import PostService

import logging
log = logging.getLogger(__name__)
# from app.tasks.tiktok.dispatcher import dispatch_video_batches
from app.tasks.tiktok.post import crawl_tiktok_all_posts, crawl_tiktok_all_posts_backdate
from app.utils.delay import async_delay


from app.requests import CrawlPostBackdateRequest


router = APIRouter()

@router.get("/posts")
async def get_posts():
    try:
        data = await PostService.get_posts()
        return data
    except Exception as e:
        log.error(f"Lỗi khi lấy dữ liệu post: {e}")
        return {"status": "error", "message": str(e)}


@router.post("/posts/crawl")
async def crawl_posts():
    try:
        job_name = "tiktok_post"
        crawl_type = "tiktok"
        crawl_tiktok_all_posts.delay(job_name)
    except Exception as e:
        return {"status": "error", "message": str(e)}
    

@router.post("/posts/crawl/backdate")
async def crawl_posts_backdate(request: CrawlPostBackdateRequest):
    try:
        log.info(f"Đang lấy dữ liệu post: {request}")
        job_id = "crawl_post"
        crawl_tiktok_all_posts_backdate.delay(job_id, request.from_date, request.to_date)
        return {"message": "Crawl started", "request": request}
    except Exception as e:
        log.error(f"Lỗi khi lấy dữ liệu post: {e}")
        return {"status": "error", "message": str(e)}