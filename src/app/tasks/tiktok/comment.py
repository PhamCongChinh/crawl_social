import asyncio
from datetime import datetime
import logging

from bson import Int64

from app.modules.elastic_search.service import postToES
from app.modules.tiktok_scraper.models.channel import ChannelModel
from app.modules.tiktok_scraper.scrapers.comment import scrape_comments
from app.modules.tiktok_scraper.services.post import PostService
from app.utils.concurrency import limited_gather
from app.utils.delay import async_delay
from app.utils.timezone import VN_TZ
log = logging.getLogger(__name__)

from app.modules.tiktok_scraper.services.channel import ChannelService
from app.worker import celery_app

from app.config import mongo_connection

@celery_app.task(
    name="app.tasks.tiktok.post.crawl_tiktok_comments",
    bind=True
)
def crawl_tiktok_comments(self, job_id: str, channel_id: str):
    print(f"Task {job_id} - {channel_id}")
    async def do_crawl():
        try:
            await mongo_connection.connect()
            channels = await ChannelService.get_channels()
            log.info(f"🚀 Đang cào {len(channels)}")

            # Trong hàm async
            coroutines = []
            for idx, channel in enumerate(channels):
                log.info(f"🕐 [{idx+1}/{len(channels)}] {channel.id}")
                data = channel.model_dump(by_alias=True)
                data["_id"] = str(data["_id"])
                coroutines.append(crawl_tiktok_comment_direct(data))
            # Giới hạn 3 request Scrapfly chạy cùng lúc
            await limited_gather(coroutines, limit=2)

        except Exception as e:
            log.error(e)
    return asyncio.run(do_crawl())

async def crawl_tiktok_comment_direct(channel: dict):
    try:
        await mongo_connection.connect()
        channel_model = ChannelModel(**channel)

        log.info(f"🔍 Crawling source: {channel_model.id}")
        
        data = await scrape_comments(channel_model.id)
        
        if not data or not data[0]:
            log.warning(f"⚠️ Không lấy được dữ liệu từ {channel_model.id}")
            return
        
        await async_delay(2,4)
        comment = flatten_post_list(data, channel=channel_model)
        log.info("comment: " + str(comment))
        await postToES(comment)
        await async_delay(2,3)
        # # await ChannelService.channel_crawled(channel_model.id)

        # result = await PostService.upsert_posts_bulk(data, channel=channel_model)
        
        return {"status": "success"}
    except Exception as e:
        log.error(e)

def flatten_post_data_comment(raw: dict, channel: ChannelModel) -> dict:
    print(f"chuyển đổi dữ liệu {raw.get('unique_id', 'unknown')}")
    return {
        "id": raw.get("cid", ""),
        "doc_type": 2, # POST = 1, COMMENT = 2
        "crawl_source": 2,
        "crawl_source_code": channel.source_channel,
        "pub_time": Int64(int(raw.get("create_time", 0))),
        "crawl_time": int(datetime.now(VN_TZ).timestamp()),
        "org_id": channel.org_id,
        "subject_id": "",
        "title": raw.get("text", ""),
        "description": raw.get("text", ""),
        "content": raw.get("text", ""),
        "url": f"https://www.tiktok.com/@{raw.get('unique_id')}",
        "media_urls": "[]",
        "comments": 0,
        "shares": 0,
        "reactions": raw.get("digg_count", 0),
        "favors": 0,
        "views": 0,
        "web_tags": "[]",
        "web_keywords": "[]",
        "auth_id": raw.get("unique_id", ""),
        "auth_name": raw.get("nickname", ""),
        "auth_type": 1,
        "auth_url": f"https://www.tiktok.com/@{raw.get('unique_id')}",
        "source_id": raw.get("cid", ""),
        "source_type": 5,
        "source_name": raw.get("nickname", ""),
        "source_url": channel.id,
        "reply_to": "",
        "level": 0 ,
        "sentiment": 0,
        "isPriority": True,
        "crawl_bot": "tiktok_comment",
    }

def flatten_post_list(raw_list: list[dict], channel: ChannelModel) -> list[dict]:
    print(f"Đang crawl {len(raw_list)} comments for channel {channel.id}")
    flattened = []
    for item in raw_list:
        if not isinstance(item, dict):
            print(f"⚠️ Bỏ qua phần tử không hợp lệ (not dict): {item}")
            continue
        try:
            print(f"Flattening comment item: {item.get('cid', 'unknown')}")
            flat = flatten_post_data_comment(item, channel)
            flattened.append(flat)
        except Exception as e:
            print(f"⚠️ Lỗi khi flatten item: {e}")
    print(f"Đã crawl {len(flattened)} comments for channel {channel.id}")
    return flattened