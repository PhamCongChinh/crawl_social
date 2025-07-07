import json
import logging
from pathlib import Path

from fastapi import APIRouter

from app.modules.thread_scraper.scrapers.post import scrape_thread
log = logging.getLogger(__name__)

router = APIRouter()

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)

@router.get("/posts/crawl")
async def crawl_posts():
    try:
        log.info("Welcome to Thread!")
        url = "https://www.threads.com/@ngoctrinh89?hl=vi"  # example without media
        # url = "https://www.threads.net/t/C8H5FiCtESk/"  # example with media
        thread = await scrape_thread(url)
        output.joinpath("thread.json").write_text(json.dumps(thread, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as e:
        return {"status": "error", "message": str(e)}