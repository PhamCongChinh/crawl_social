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

from app.config import mongo_connection, postgres_connection

@celery_app.task(
    name="app.tasks.tiktok.post.crawl_tiktok_comments",
    bind=True
)
def crawl_tiktok_comments(self, job_id: str, channel_id: str):
    print(f"Task {job_id} - {channel_id}")
    async def do_crawl():
        try:
            await postgres_connection.connect()
            # channels = await ChannelService.get_channels_crawl_comments()
            posts = await ChannelService.get_posts_postgre()
            log.info(f"🚀 Đang cào {len(posts)}")

            coroutines = []
            for idx, post in enumerate(posts):
                log.info(f"🕐 [{idx+1}/{len(posts)}] {post['id']}")
                # data = channel.model_dump(by_alias=True)
                # data["_id"] = str(data["_id"])
                print(f"post: {post}")
                print(type(post))
                coroutines.append(crawl_tiktok_comment_direct_1(post))

                break
            # Giới hạn 3 request Scrapfly chạy cùng lúc
            await limited_gather(coroutines, limit=1)

            # # Trong hàm async
            # coroutines = []
            # for idx, channel in enumerate(channels):
            #     log.info(f"🕐 [{idx+1}/{len(channels)}] {channel.id}")
            #     data = channel.model_dump(by_alias=True)
            #     data["_id"] = str(data["_id"])
            #     coroutines.append(crawl_tiktok_comment_direct(data))
            #     break
            # # Giới hạn 3 request Scrapfly chạy cùng lúc
            # await limited_gather(coroutines, limit=1)

        except Exception as e:
            log.error(e)
    return asyncio.run(do_crawl())

# ví dụ
async def crawl_tiktok_comment_direct_1(post: dict):
    try:
        await postgres_connection.connect()
        data = await scrape_comments(post["url"])
        print(f"Đã lấy {len(data)} comments từ {post['id']}")
        await async_delay(2,4)
        comment = flatten_post_list_1(data, post=post)

        result = await postToES(comment)
        if not result:
            log.warning(f"⚠️ Không thể gửi dữ liệu comment lên Elasticsearch cho post {post['id']}")
            return

        log.info(f"🔍 Crawling source: {post['id']}")
        return result
    except Exception as e:
        log.error(e)

def flatten_post_data_comment_1(raw: dict, post: dict) -> dict:
    return {
        "id": raw.get("cid", ""),
        "doc_type": 2, # POST = 1, COMMENT = 2
        "crawl_source": post["crawl_source"], # 1 = Scrapfly, 2 = Direct
        "crawl_source_code": post["crawl_source_code"], # tt = TikTok
        "pub_time": Int64(int(raw.get("create_time", 0))),
        "crawl_time": int(datetime.now(VN_TZ).timestamp()),
        "org_id": post["org_id"],
        "subject_id": raw.get("subject_id", None),
        "title": raw.get("title", None),
        "description": raw.get("description", None),
        "content": raw.get("text", None),
        "url": f"{post['source_url']}/video/{raw.get('aweme_id')}?share_comment_id={raw.get('cid')}", # là duy nhất https://www.tiktok.com/@officialhanoifc/video/7483524591290240264?share_comment_id=7483666889643426561
        "media_urls": "[]",
        "comments": 0,
        "shares": 0,
        "reactions": raw.get("digg_count", 0),
        "favors": 0,
        "views": 0,
        "web_tags": "[]",
        "web_keywords": "[]",
        "auth_id": raw.get("unique_id", None),
        "auth_name": raw.get("nickname", None),
        "auth_type": 1,
        "auth_url": f"https://www.tiktok.com/@{raw.get('unique_id')}",
        "source_id": raw.get("aweme_id", None),
        "source_type": 5,
        "source_name": post["source_name"],
        "source_url": post["source_url"],
        "reply_to": raw.get("reply_to", None),
        "level": raw.get("level", 0),
        "sentiment": raw.get("sentiment", 0),
        "isPriority": True,
        "crawl_bot": "tiktok_comment",
    }

def flatten_post_list_1(raw_list: list[dict], post: dict) -> list[dict]:
    print(f"Đang crawl {len(raw_list)} comments for post {post['id']}")
    flattened = []
    for item in raw_list:
        if not isinstance(item, dict):
            print(f"⚠️ Bỏ qua phần tử không hợp lệ (not dict): {item}")
            continue
        try:
            print(f"Flattening comment item: {item.get('cid', 'unknown')}")
            flat = flatten_post_data_comment_1(item, post)
            flattened.append(flat)
        except Exception as e:
            print(f"⚠️ Lỗi khi flatten item: {e}")
    print(f"Đã crawl {len(flattened)} comments for post {post['id']}")
    return flattened





















# Task con: xử lý crawl 1 source → Scrapfly → DB
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
        await ChannelService.channel_crawled_comments(channel_model.id)

        result = await PostService.upsert_posts_bulk(data, channel=channel_model)
        
        return {"status": "success"}
    except Exception as e:
        log.error(e)

def flatten_post_data_comment(raw: dict, channel: ChannelModel) -> dict:
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
        "url": f"{channel.source_url}/video/{raw.get('aweme_id')}?share_comment_id={raw.get('cid')}", # là duy nhất https://www.tiktok.com/@officialhanoifc/video/7483524591290240264?share_comment_id=7483666889643426561
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
        "source_id": raw.get("aweme_id", ""),
        "source_type": 5,
        "source_name": channel.source_name,
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