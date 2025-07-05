import asyncio
from typing import List
from app.modules.tiktok_scraper.scrapers.channel import scrape_channel
from app.modules.tiktok_scraper.services.channel import ChannelService
from app.modules.tiktok_scraper.services.source import SourceService
from app.utils.concurrency import limited_gather
from app.utils.delay import async_delay
from app.worker import celery_app
import logging
log = logging.getLogger(__name__)
from app.modules.tiktok_scraper.models.source import SourceModel
from app.modules.tiktok_scraper.services.channel import ChannelService
from app.config import mongo_connection

@celery_app.task(
    name="app.tasks.tiktok.channel.crawl_tiktok_channels",
    bind=True
)
def crawl_tiktok_channels(self, job_id: str, channel_id: str):
    log.info(f"Task {job_id} - {channel_id} bắt đầu")
    async def do_crawl():
        try:
            await mongo_connection.connect()
            sources = await SourceService.get_sources()
            log.info(f"📦 Tổng số source: {len(sources)}")
            batch_size = 5  # Số lượng source mỗi lần crawl
            batches = _chunk_sources(sources, batch_size)
            log.info(f"📦 Chia thành {len(batches)} batch, mỗi batch {batch_size} nguồn")
            for batch_index, batch in enumerate(batches, start=1):
                log.info(f"🚀 Đang xử lý batch {batch_index}/{len(batches)} với {len(batch)} nguồn")
                coroutines = []
                for idx, source in enumerate(batch):
                    overall_idx = (batch_index - 1) * batch_size + idx + 1
                    log.info(f"🕐 [{overall_idx}/{len(sources)}] {source.source_url}")
                    data = source.model_dump(by_alias=True)
                    data["_id"] = str(data["_id"])
                    coroutines.append(crawl_tiktok_channel_direct(data))
                await limited_gather(coroutines, limit=3)  # Giới hạn 3 request Scrapfly chạy cùng lúc
                await asyncio.sleep(2)  # nghỉ 2 giây giữa batch

            # Trong hàm async
            # coroutines = []
            # for idx, source in enumerate(sources):
            #     log.info(f"🕐 [{idx+1}/{len(sources)}] {source.source_url}")
            #     data = source.model_dump(by_alias=True)
            #     data["_id"] = str(data["_id"])
            #     coroutines.append(crawl_tiktok_channel_direct(data))
            # # Giới hạn 3 request Scrapfly chạy cùng lúc
            # await limited_gather(coroutines, limit=2)

            log.info(f"✅ Task cha {job_id} hoàn tất toàn bộ")
        except Exception as e:
            log.error(e)
    return asyncio.run(do_crawl())

def _chunk_sources(sources: List, batch_size: int) -> List[List]:
    return [sources[i:i + batch_size] for i in range(0, len(sources), batch_size)]

# Task con: xử lý crawl 1 source → Scrapfly → DB
async def crawl_tiktok_channel_direct(source: dict):
    try:
        await mongo_connection.connect()
        source_model = SourceModel(**source)
        log.info(f"🔍 Crawling source: {source_model.source_url}")
        data = await safe_scrape_with_delay(source_model.source_url)
        if not data:
            log.warning(f"⚠️ Không lấy được dữ liệu từ {source_model.source_url}")
            return
        result = await ChannelService.upsert_channels_bulk(data, source=source_model)
        log.info(
            f"✅ Upsert xong {source_model.source_url}: matched={result.matched_count}, "
            f"inserted={result.upserted_count}, modified={result.modified_count}"
        )
    except Exception as e:
        log.error(f"❌ Lỗi crawl {source.get('source_url')}: {e}")


async def safe_scrape_with_delay(url: str, max_retries: int = 3):
    for attempt in range(1, max_retries + 1):
        try:
            data = await scrape_channel(url)
            await async_delay(2, 4)  # Đảm bảo browser session trước shutdown
            return data
        except Exception as e:
            log.warning(f"❗ Attempt {attempt}/{max_retries} - Lỗi scrape: {e}")
            if attempt < max_retries:
                await async_delay(5, 8)  # Delay lâu hơn nếu lỗi
            else:
                log.error(f"❌ Bỏ qua URL sau {max_retries} lần thử: {url}")
                return None