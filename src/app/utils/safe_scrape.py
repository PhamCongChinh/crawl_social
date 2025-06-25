import asyncio
import logging

from app.modules.tiktok_scraper.scrapers.channel import scrape_channel

log = logging.getLogger(__name__)

async def safe_scrape(url: str, retries: int = 3, delay: tuple = (1, 3)):
    """
    Gọi scraper có retry và xử lý lỗi timeout, None, empty data.
    """
    for attempt in range(1, retries + 1):
        try:
            log.info(f"[Scrape Attempt {attempt}] Đang crawl: {url}")
            data = await scrape_channel(url)

            if not data:
                log.warning(f"[Scrape Attempt {attempt}] ⚠️ Dữ liệu rỗng hoặc None")
                await asyncio.sleep(1)
                continue

            log.info(f"[Scrape Attempt {attempt}] ✅ Thành công, crawl được {len(data)} bài viết")
            return data

        except Exception as e:
            # Catch timeout hoặc lỗi bất kỳ
            message = str(e)
            log.warning(f"[Scrape Attempt {attempt}] ❌ Lỗi: {message}")

            if "ERR::SCRAPE::OPERATION_TIMEOUT" in message:
                log.warning(f"⏳ Timeout từ Scrapfly - thử lại sau {delay[1]}s")
            await asyncio.sleep(delay[0] + (attempt - 1) * (delay[1] - delay[0]))

    log.error(f"🔥 Thất bại sau {retries} lần retry: {url}")
    return None
