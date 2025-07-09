from datetime import datetime
import json
from pathlib import Path
import traceback
from zoneinfo import ZoneInfo
from bson import Int64
from fastapi import APIRouter

from app.modules.tiktok_scraper.models.channel import ChannelModel
from app.modules.tiktok_scraper.scrapers.post import scrape_posts
from app.modules.tiktok_scraper.services.channel import ChannelService
from app.modules.tiktok_scraper.services.post import PostService

import logging
log = logging.getLogger(__name__)
from app.tasks.tiktok.dispatcher import dispatch_video_batches
from app.tasks.tiktok.post import crawl_tiktok_posts, crawl_tiktok_posts_hourly
from app.utils.delay import async_delay


from app.requests import CrawlPostBackdateRequest


router = APIRouter()

@router.get("/posts")
async def get_posts():
    log.info("Đang lấy dữ liệu post")
    try:
        # data = await PostService.get_posts()
        dispatch_video_batches.delay()
        # crawl_tiktok_posts_hourly.delay()
        log.info("Đã lấy dữ liệu post")
        return None
    except Exception as e:
        log.error(f"Lỗi khi lấy dữ liệu post: {e}")
        return {"status": "error", "message": str(e)}



@router.get("/posts/crawl")
async def crawl_posts():
    try:
        job_name = "tiktok"
        crawl_type = "tiktok"
        crawl_tiktok_posts_hourly.delay(job_name, crawl_type)
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.post("/posts/crawl/backdate")
async def crawl_posts_backdate(request: CrawlPostBackdateRequest):
    try:
        log.info(f"Đang lấy dữ liệu post: {request}")
        # dispatch_video_batches.delay()
        crawl_tiktok_posts.delay(request.from_date, request.to_date)
        return {"message": "Crawl started", "request": request}
    except Exception as e:
        log.error(f"Lỗi khi lấy dữ liệu post: {e}")
        return {"status": "error", "message": str(e)}


VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

def flatten_post_data(raw: dict, channel: ChannelModel) -> dict:
    return {
        "id": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
        "doc_type": 1, # POST = 1, COMMENT = 2
        "crawl_source": 2,
        "crawl_source_code": "tt",
        "pub_time": Int64(int(raw.get("createTime", 0))),
        "crawl_time": int(datetime.now(VN_TZ).timestamp()),
        "org_id": channel.org_id,
        "subject_id": "",
        "title": raw.get("desc", ""),
        "description": raw.get("desc", ""),
        "content": raw.get("desc", ""),
        "url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
        # "media_urls": raw.get("video", {}).get("media_urls", ""),#khong duoc rông
        "media_urls": json.dumps(raw.get("media_urls", [])),
        "comments": raw.get("stats", {}).get("commentCount", 0),
        "shares": raw.get("stats", {}).get("shareCount", 0),
        "reactions": raw.get("stats", {}).get("diggCount", 0),
        "favors": int(raw.get("stats", {}).get("collectCount", 0) or 0),
        "views": raw.get("stats", {}).get("playCount", 0),
        # "web_tags": ", ".join(raw.get("diversificationLabels", [])),  #khong duoc rông
        # "web_keywords": "",#khong duoc rông
        # "web_tags": json.dumps(raw.get("diversificationLabels", [])),
        # "web_keywords": json.dumps(raw.get("suggestedWords", [])),
        "web_tags": "[]",
        "web_keywords": "[]",
        "auth_id": raw.get("author", {}).get("id", ""),
        "auth_name": raw.get("author", {}).get("nickname", ""),
        "auth_type": 1,
        "auth_url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
        "source_id": raw.get("id", ""),
        "source_type": 5,
        "source_name": raw.get("author", {}).get("nickname", ""),
        "source_url": channel.source_url,
        "reply_to": "",
        "level": 0 ,
        "sentiment": 0,
        "isPriority": True,
        "crawl_bot": "tiktok_post",
    }