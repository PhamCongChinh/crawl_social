import asyncio
from typing import List
from fastapi import APIRouter, HTTPException

import logging

from app.modules.tiktok_scraper.models.source import SourceModel
from app.modules.tiktok_scraper.scrapers.channel import scrape_channel
from app.modules.tiktok_scraper.services.source import SourceService
# from app.tasks.tiktok.channel import crawl_tiktok_channels_classified, crawl_tiktok_channels_unclassified
from app.utils.delay import async_delay
log = logging.getLogger(__name__)

from app.modules.tiktok_scraper.services.channel import ChannelService

router = APIRouter()

@router.get("/channels")
async def get_channels():
    try:
        log.info("Đang lấy dữ liệu Channels")
        channels = await ChannelService.get_channels_posts_hourly()
        log.info(f"Đã lấy được {len(channels)} kênh")
        if not channels:
            raise HTTPException(status_code=204, detail="Không có dữ liệu")
        log.info(f"Đã lấy được {len(channels)} kênh")
        return channels
    except Exception as e:
        log.error(f"Lỗi khi lấy URLs: {e}")
        raise HTTPException(status_code=500, detail="Không thể lấy danh sách URLs")

@router.post("/channels/crawl/classified")
async def crawl_channels_classified():
    try:
        # crawl_tiktok_channels_classified.delay("classified", "tiktok")
        pass
    except Exception as e:
        return {"status": "error", "message": str(e)}
    
@router.post("/channels/crawl/unclassified")
async def crawl_channels_unclassified():
    try:
        # crawl_tiktok_channels_unclassified.delay("unclassified", "tiktok")
        pass
    except Exception as e:
        return {"status": "error", "message": str(e)}







def chunk_list(lst: list, size: int) -> list:
    return [lst[i:i + size] for i in range(0, len(lst), size)]

@router.post("/channels/crawl/classified/batch")
async def crawl_channels_classified_batch():
    sources = await SourceService.get_sources_classified()
    batch_size = 5
    batches = chunk_list(sources, batch_size)
    for batch in batches:
        for source in batch:
            data = source.model_dump(by_alias=True)
            data["_id"] = str(data["_id"])
            print(data)

        await async_delay(2, 3)
    return {"status": "dispatched", "total_sources": len(sources)}