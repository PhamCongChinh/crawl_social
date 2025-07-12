from fastapi import APIRouter, HTTPException

import logging

from app.modules.tiktok_scraper.services.video import VideoService
from app.tasks.tiktok.video import crawl_video_all_classified, crawl_video_all_keyword, crawl_video_all_unclassified
log = logging.getLogger(__name__)

router = APIRouter()

@router.get("/videos")
async def get_videos():
    try:
        log.info("Đang lấy dữ liệu Videos")
        videos = await VideoService.get_videos()
        log.info(f"Đã lấy được {len(videos)} videos")
        if not videos:
            raise HTTPException(status_code=204, detail="Không có dữ liệu")
        log.info(f"Đã lấy được {len(videos)} videos")
        return videos
    except Exception as e:
        log.error(f"Lỗi khi lấy URLs: {e}")
        raise HTTPException(status_code=500, detail="Không thể lấy danh sách URLs")
    
@router.post("/videos/crawl/classified")
async def crawl_videos_classified():
    crawl_video_all_classified.delay(job_id="request.job_id")
    return {"message": f"Đã trigger job {'request.job_id'}"}

@router.post("/videos/crawl/unclassified")
async def crawl_videos_unclassified():
    crawl_video_all_unclassified.delay(job_id="request.job_id")
    return {"message": f"Đã trigger job {'request.job_id'}"}

@router.post("/videos/crawl/keyword")
async def crawl_videos_keyword():
    crawl_video_all_keyword.delay(job_id="request.job_id")
    return {"message": f"Đã trigger job {'request.job_id'}"}