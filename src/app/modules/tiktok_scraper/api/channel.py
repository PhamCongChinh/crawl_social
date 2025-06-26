import asyncio
from typing import List
from fastapi import APIRouter, HTTPException

import logging

from app.modules.tiktok_scraper.models.source import SourceModel
from app.modules.tiktok_scraper.scrapers.channel import scrape_channel
from app.modules.tiktok_scraper.services.source import SourceService
from app.tasks.tiktok.channel import crawl_tiktok_channels
from app.utils.delay import async_delay
log = logging.getLogger(__name__)

from app.modules.tiktok_scraper.services.channel import ChannelService

router = APIRouter()

@router.get("/channels")
async def get_channels():
    try:
        log.info("Đang lấy dữ liệu Channels")
        channels = await ChannelService.get_channels()
        if not channels:
            raise HTTPException(status_code=204, detail="Không có dữ liệu")
        log.info(f"Đã tìm thấy {len(channels)} bài viết trong cơ sở dữ liệu")
        return channels
    except Exception as e:
        log.error(f"Lỗi khi lấy URLs: {e}")
        raise HTTPException(status_code=500, detail="Không thể lấy danh sách URLs")

@router.get("/channels/crawl")
async def crawl_channels():
    try:
        job_id = "tiktok1"
        channel_id = "tiktok1"
        crawl_tiktok_channels.delay(job_id, channel_id)
    except Exception as e:
        return {"status": "error", "message": str(e)}

    # except Exception as e:
    #     log.error(f"Lỗi khi lấy URLs: {e}")
    #     raise HTTPException(status_code=500, detail="Không thể lấy danh sách URLs")