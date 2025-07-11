from typing import List
from fastapi import APIRouter, HTTPException

import logging

# from app.tasks import crawl_source_task

log = logging.getLogger(__name__)

from app.modules.tiktok_scraper.models.source import SourceModel
from app.modules.tiktok_scraper.services.source import SourceService

router = APIRouter()

@router.get("/sources")
async def get_sources():
    try:
        log.info("Đang lấy dữ liệu Sources")
        sources = await SourceService.get_sources_postgre()  # Lấy nguồn đã cập nhật trong 1 giờ qua
        if not sources:
            raise HTTPException(status_code=204, detail="Không có dữ liệu")
        # log.info(f"Đã tìm thấy {len(sources)} bài viết trong cơ sở dữ liệu")
        return sources
    except Exception as e:
        log.error(f"Lỗi khi lấy URLs: {e}")
        raise HTTPException(status_code=500, detail="Không thể lấy danh sách URLs")
    
@router.post("/sources")
async def create_or_update_source(request: SourceModel):
    data = request.model_dump(exclude_unset=True)
    status = await SourceService.upsert_source(data)
    return {"status": status}

@router.post("/sources/batch")
async def create_or_update_source_batch(request: List[SourceModel]):
    data = [item.model_dump(exclude_unset=True) for item in request]
    count = await SourceService.upsert_source_batch(data)
    return {"message": f"Đã xử lý {count} source"}