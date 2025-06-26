import asyncio
from datetime import datetime
import json
from zoneinfo import ZoneInfo

from bson import Int64
from app.modules.elastic_search.service import postToES
from app.modules.tiktok_scraper.models.channel import ChannelModel
from app.modules.tiktok_scraper.scrapers.post import scrape_posts
from app.modules.tiktok_scraper.services.channel import ChannelService
from app.utils.concurrency import limited_gather
from app.utils.delay import async_delay
from app.worker import celery_app

import logging
log = logging.getLogger(__name__)

from app.config import mongo_connection


@celery_app.task(
    name="app.tasks.tiktok.post.crawl_tiktok_posts",
    bind=True
)
def crawl_tiktok_posts(self, job_id: str, channel_id: str):
    print(f"Task {job_id} - {channel_id}")
    async def do_crawl():
        try:
            await mongo_connection.connect()
            channels = await ChannelService.get_channels_crawl()
            log.info(f"🚀 Đang cào {len(channels)}")
            # Trong hàm async
            coroutines = []
            for idx, channel in enumerate(channels):
                log.info(f"🕐 [{idx+1}/{len(channels)}] {channel.id}")
                data = channel.model_dump(by_alias=True)
                data["_id"] = str(data["_id"])
                coroutines.append(crawl_tiktok_post_direct(data))
            # Giới hạn 3 request Scrapfly chạy cùng lúc
            await limited_gather(coroutines, limit=3)

        except Exception as e:
            log.error(e)
    return asyncio.run(do_crawl())

async def crawl_tiktok_post_direct(channel: dict):
    try:
        await mongo_connection.connect()
        channel_model = ChannelModel(**channel)

        log.info(f"🔍 Crawling source: {channel_model.id}")
        
        data = await safe_scrape_with_delay(channel_model.id)
        # data = await scrape_posts([channel_model.id])

        if not data or not data[0]:
            log.warning(f"⚠️ Không lấy được dữ liệu từ {channel_model.id}")
            return

        post = flatten_post_data(data[0], channel=channel_model)
        log.info(f"✅ Thêm vào flatten: {post.get('id')}")

        await postToES([post])
        await ChannelService.channel_crawled(channel_model.id)

        # result = await Pos.upsert_channels_bulk(data, channel=channel_model)
        # log.info(
        #     f"✅ Upsert xong {channel_model.source_url}: matched={result.matched_count}, "
        #     f"inserted={result.upserted_count}, modified={result.modified_count}"
        # )
        return {"status": "success"}
    except Exception as e:
        log.error(f"❌ Lỗi crawl {post.get('source_url')}: {e}")


async def safe_scrape_with_delay(url: str, max_retries: int = 3):
    for attempt in range(1, max_retries + 1):
        try:
            data = await scrape_posts([url])
            await async_delay(2, 4)  # Đảm bảo browser session trước shutdown
            return data
        except Exception as e:
            log.warning(f"❗ Attempt {attempt}/{max_retries} - Lỗi scrape: {e}")
            if attempt < max_retries:
                await async_delay(5, 8)  # Delay lâu hơn nếu lỗi
            else:
                log.error(f"❌ Bỏ qua URL sau {max_retries} lần thử: {url}")
                return None

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

def flatten_post_data(raw: dict, channel: ChannelModel) -> dict:
    return {
        "id": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
        "doc_type": 1, # POST = 1, COMMENT = 2
        "crawl_source": 2,
        "crawl_source_code": "tt",
        "pub_time": Int64(int(raw.get("createTime", 0))),
        "crawl_time": int(datetime.now(VN_TZ).timestamp()),
        "org_id": channel.org_id,
        "subject_id": "",
        "title": "",
        "description": raw.get("desc", ""),
        "content": raw.get("desc", ""),
        "url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
        # "media_urls": raw.get("video", {}).get("media_urls", ""),#khong duoc rông
        "media_urls": json.dumps(raw.get("media_urls", [])),
        "comments": raw.get("stats", {}).get("commentCount", 0),
        "shares": raw.get("stats", {}).get("shareCount", 0),
        "reactions": raw.get("stats", {}).get("diggCount", 0),
        "favors": int(raw.get("stats", {}).get("collectCount", 0) or 0),
        "views": raw.get("stats", {}).get("playCount", 0),
        # "web_tags": ", ".join(raw.get("diversificationLabels", [])),  #khong duoc rông
        # "web_keywords": "",#khong duoc rông
        "web_tags": json.dumps(raw.get("diversificationLabels", [])),
        "web_keywords": json.dumps(raw.get("suggestedWords", [])),
        "auth_id": raw.get("author", {}).get("id", ""),
        "auth_name": raw.get("author", {}).get("nickname", ""),
        "auth_type": 1,
        "auth_url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
        "source_id": raw.get("id", ""),
        "source_type": 5,
        "source_name": raw.get("author", {}).get("nickname", ""),
        "source_url": channel.source_url,
        "reply_to": "",
        "level": 0 ,
        "sentiment": 0,
        "isPriority": True,
        "crawl_bot": "tiktok_post",
    }