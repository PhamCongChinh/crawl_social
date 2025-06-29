import asyncio
from typing import List
from fastapi import APIRouter, HTTPException

import logging

from app.modules.tiktok_scraper.models.source import SourceModel
from app.modules.tiktok_scraper.scrapers.channel import scrape_channel
from app.modules.tiktok_scraper.services.source import SourceService
from app.tasks.tiktok.channel import crawl_tiktok_channels
from app.tasks.tiktok.comment import crawl_tiktok_comments
from app.tasks.tiktok.search import crawl_tiktok_search
from app.utils.delay import async_delay
log = logging.getLogger(__name__)

from app.modules.tiktok_scraper.services.channel import ChannelService

from app.modules.tiktok_scraper.scrapers.comment import scrape_comments

router = APIRouter()

@router.get("/search")
async def get_comments():
    url = f"https://www.tiktok.com/@officialhanoifc/video/7519445590686567688"
    comments = await scrape_comments(url)
    return comments

@router.get("/search/crawl")
async def crawl_search():
    try:
        job_id = "tiktok1"
        channel_id = "tiktok1"
        crawl_tiktok_search.delay(job_id, channel_id)
    except Exception as e:
        return {"status": "error", "message": str(e)}
