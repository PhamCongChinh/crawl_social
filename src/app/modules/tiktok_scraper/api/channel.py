import asyncio
from typing import List
from fastapi import APIRouter, HTTPException

import logging

from app.modules.tiktok_scraper.models.source import SourceModel
from app.modules.tiktok_scraper.schemas.channel import ChannelRequest
from app.modules.tiktok_scraper.scrapers.channel import scrape_channel
from app.modules.tiktok_scraper.services.source import SourceService
from app.tasks.tiktok.channel import crawl_tiktok_channels
from app.utils.delay import async_delay
log = logging.getLogger(__name__)

from app.modules.tiktok_scraper.services.channel import ChannelService

router = APIRouter()

@router.get("/channels")
async def get_all_sources():
    try:
        log.info("Đang lấy dữ liệu Channels")
        channels = await ChannelService.get_all_channels()
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
        job_id = "tiktok"
        channel_id = "tiktok"
        crawl_tiktok_channels.delay(job_id, channel_id)
        # sources = await SourceService.get_sources()
        # for source in sources:
        #     # Dùng `.dict()` nếu là Pydantic object
        #     data = source.model_dump(by_alias=True)
        #     # Chuyển ObjectId về string
        #     data["_id"] = str(data["_id"])
        #     crawl_tiktok_channels.delay(data)
        # return {"status": "submitted", "total": len(sources)}
    except Exception as e:
        return {"status": "error", "message": str(e)}

    # try:
    #     sources = await SourceService.get_sources()
        # for source in sources:
        #     try:
        #         log.info(f"Đang crawl channels cho {source.source_name} từ {source.source_url}")
        #         data = await scrape_channel(url=source.source_url)
        #         log.info(f"Đang upsert {len(data)} channels vào cơ sở dữ liệu")
        #         await async_delay(1,3)
        #         result = await ChannelService.upsert_channels_bulk(data, source=source)
        #         log.info(f"Bulk upsert xong: inserted={result.upserted_count}, modified={result.modified_count}")
        #     except Exception as e:
        #         log.error(f"{e}")
        #         continue
            
        #     await async_delay(1,3)
       
        # return {"message": "Crawl channels completed", "count": len(sources)}
    except Exception as e:
        log.error(f"Lỗi khi lấy URLs: {e}")
        raise HTTPException(status_code=500, detail="Không thể lấy danh sách URLs")
    

# @router.post("/crawl-channels")
# async def crawl_tiktok_channels1(channels: List[ChannelRequest]):
#     for ch in channels:
#         crawl_channels_test.delay(ch.model_dump())
#     return { "message": "Tasks submitted" }