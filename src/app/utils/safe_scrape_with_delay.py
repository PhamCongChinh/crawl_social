from app.modules.tiktok_scraper.scrapers.channel import scrape_channel
from app.utils.delay import async_delay
import logging
log = logging.getLogger(__name__)

async def safe_scrape_with_delay_channel(url: str, max_retries: int = 5):
    for attempt in range(1, max_retries + 1):
        try:
            data = await scrape_channel(url)
            await async_delay(2, 4)  # Đảm bảo browser session trước shutdown
            return data
        except Exception as e:
            log.warning(f"❗ Lần thứ {attempt}/{max_retries} - Lỗi scrape: {e}")
            if attempt < max_retries:
                await async_delay(5, 8)  # Delay lâu hơn nếu lỗi
            else:
                log.error(f"❌ Bỏ qua URL sau {max_retries} lần thử: {url}")
                return None