import json
import logging
from pathlib import Path

from fastapi import APIRouter

from app.modules.instagram_scraper.scrapers.post import scrape_post
from app.modules.thread_scraper.scrapers.post import scrape_thread
log = logging.getLogger(__name__)

router = APIRouter()

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)
# import instagram

@router.get("/posts/crawl")
async def crawl_posts():
    try:
        log.info("Welcome to Instagram!")
        url = "https://www.threads.com/@domylinh1310"  # example without media
        # # url = "https://www.threads.net/t/C8H5FiCtESk/"  # example with media
        post_video = await scrape_post(url)
        output.joinpath("video-post.json").write_text(json.dumps(post_video, indent=2, ensure_ascii=False), encoding='utf-8')
    except Exception as e:
        return {"status": "error", "message": str(e)}