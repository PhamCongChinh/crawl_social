import asyncio
from typing import List
from fastapi import APIRouter, HTTPException

import logging

from app.modules.tiktok_scraper.models.keyword import KeywordModel
from app.modules.tiktok_scraper.models.source import SourceModel
from app.modules.tiktok_scraper.scrapers.channel import scrape_channel
from app.modules.tiktok_scraper.scrapers.search import scrape_search
from app.modules.tiktok_scraper.services.search import SearchService
from app.modules.tiktok_scraper.services.source import SourceService

from app.tasks.tiktok.channel import crawl_tiktok_channels
from app.tasks.tiktok.comment import crawl_tiktok_comments
from app.tasks.tiktok.search import crawl_tiktok_search
from app.utils.delay import async_delay
log = logging.getLogger(__name__)

from app.requests import CreateKeywordRequest

from app.modules.tiktok_scraper.services.channel import ChannelService

from app.modules.tiktok_scraper.scrapers.comment import scrape_comments

router = APIRouter()

@router.post("/search")
async def create_or_update_keyword(request: KeywordModel):
    data = request.model_dump(exclude_unset=True)
    status = await SearchService.upsert_keyword(data)
    return {"status": status}

@router.get("/search")
async def get_keywords():
    keywords = await SearchService.get_keywords()
    print(keywords)
    return {"status": "success", "data": keywords}
    # search = await scrape_search(keyword="Đỗ Mỹ Linh", max_search=18)
    # return search

@router.get("/search/crawl")
async def crawl_keyword():
    try:
        job_id = "tiktok1"
        channel_id = "tiktok1"
        crawl_tiktok_search.delay(job_id, channel_id)
    except Exception as e:
        return {"status": "error", "message": str(e)}
