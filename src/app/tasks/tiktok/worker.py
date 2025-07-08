from datetime import datetime
import json
from bson import Int64
from celery import shared_task
import asyncio
from app.config import mongo_connection
from app.modules.elastic_search.request import VN_TZ
from app.modules.elastic_search.service import postToES, postToESUnclassified
from app.modules.tiktok_scraper.models.channel import ChannelModel
from beanie.operators import In
import logging

from app.modules.tiktok_scraper.scrapers.post import scrape_posts
from app.utils.delay import async_delay
log = logging.getLogger(__name__)

@shared_task
def crawl_video_batch(videos: list[dict], batch_index: int, total_batches: int):
    asyncio.run(_crawl_batch_async(videos, batch_index, total_batches))


async def _crawl_batch_async(videos: list[dict], batch_index: int, total_batches: int):
    await mongo_connection.connect()
    print(f"🚀 Bắt đầu xử lý batch {batch_index}/{total_batches} với {len(videos)} video")
    data_list_classified = []
    data_list_unclassified = []
    for index, video in enumerate(videos):

        print(f"🕐 [{index+1}/{len(videos)}] {video['id']}")

        if video["org_id"] == 0:
            data_list_unclassified.append(video)
            # log.info(f"Thêm vào đã phân loại: {video['id']}")
        else:
            data_list_classified.append(video)
            # log.info(f"Thêm vào chưa phân loại: {video['id']}")

    # Crawl & post classified
    if data_list_classified:
        post_data_classified = await crawl_tiktok_post_list_direct_classified(data_list_classified)
        if post_data_classified:
            await postToES(post_data_classified)
            log.info(f"Đã thêm {len(post_data_classified)} video đã phân loại vào ElasticSearch")
    # Crawl & post unclassified
    if data_list_unclassified:
        post_data_unclassified = await crawl_tiktok_post_list_direct_unclassified(data_list_unclassified)
        print(f"post_data: {post_data_unclassified}")
        if post_data_unclassified:
            await postToESUnclassified(post_data_unclassified)
            log.info(f"Đã thêm {len(post_data_unclassified)} video chưa phân loại vào ElasticSearch")
    print(f"📦 Tổng số video đã lấy: {len(data_list_classified) + len(data_list_unclassified)}")
    print(f"✅ Hoàn tất batch {batch_index}/{total_batches}")
    await mongo_connection.disconnect()


async def crawl_tiktok_post_list_direct_classified(channels: list[dict]):
    try:
        log.info(f"📦 Tổng số channel: {len(channels)}")
        urls = [item["id"] for item in channels]
        posts_data = []
        data = await scrape_posts(urls)
        processed_ids = []
        for channel in channels:
            for post in data:
                if post.get("id") == channel["id"].strip("/").split("/")[-1]:
                    flatten = flatten_post_data(post, channel)
                    posts_data.append(flatten)
                    processed_ids.append(channel["id"])  # Lưu lại _id cần đánh dấu
        if processed_ids:
            await ChannelModel.find(In(ChannelModel.id, processed_ids)).update_many(
                {"$set": {"status": "done"}}
            )

        await async_delay(10, 12)  # Đảm bảo browser session trước shutdown
        return posts_data

    except Exception as e:
        log.error(e)

async def crawl_tiktok_post_list_direct_unclassified(channels: list[dict]):
    try:
        log.info(f"📦 Tổng số channel: {len(channels)}")
        urls = [item["id"] for item in channels]
        posts_data = []
        data = await scrape_posts(urls)
        processed_ids = []
        for channel in channels:
            for post in data:
                if post.get("id") == channel["id"].strip("/").split("/")[-1]:
                    flatten = flatten_post_data_unclassified(post, channel)
                    posts_data.append(flatten)
                    processed_ids.append(channel["id"])  # Lưu lại _id cần đánh dấu
        if processed_ids:
            await ChannelModel.find(In(ChannelModel.id, processed_ids)).update_many(
                {"$set": {"status": "done"}}
            )

        await async_delay(10, 12)  # Đảm bảo browser session trước shutdown
        return posts_data

    except Exception as e:
        log.error(e)

def flatten_post_data(raw: dict, channel: dict) -> dict:
    return {
        "id": raw.get("id", None),
        "doc_type": 1,  # POST = 1, COMMENT = 2
        "crawl_source": 2,
        "crawl_source_code": channel.get("source_channel", None),
        "pub_time": Int64(int(raw.get("createTime", 0))),
        "crawl_time": int(datetime.now(VN_TZ).timestamp()),
        "org_id": channel.get("org_id", None),
        "subject_id": raw.get("subject_id", None),
        "title": raw.get("title", None),
        "description": raw.get("description", None),
        "content": raw.get("desc", None),
        "url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
        "media_urls": "[]",
        "comments": raw.get("stats", {}).get("commentCount", 0),
        "shares": raw.get("stats", {}).get("shareCount", 0),
        "reactions": raw.get("stats", {}).get("diggCount", 0),
        "favors": int(raw.get("stats", {}).get("collectCount", 0) or 0),
        "views": raw.get("stats", {}).get("playCount", 0),
        "web_tags": "[]",#json.dumps(raw.get("diversificationLabels", [])),
        "web_keywords": "[]",# json.dumps(raw.get("suggestedWords", [])),
        "auth_id": raw.get("author", {}).get("id", ""),
        "auth_name": raw.get("author", {}).get("nickname", ""),
        "auth_type": 1,
        "auth_url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
        "source_id": raw.get("id", None),
        "source_type": channel.get("source_type", None),
        "source_name": channel.get("source_name", None),
        "source_url": channel.get("source_url", None),
        "reply_to": None,
        "level": None,
        "sentiment": 0,
        "isPriority": True,
        "crawl_bot": "tiktok_post",
    }

def flatten_post_data_unclassified (raw: dict, channel: dict) -> dict:
    return {
        # "id": raw.get("id", None),
        "doc_type": 1,  # POST = 1, COMMENT = 2
        "crawl_source": 2,
        "crawl_source_code": channel.get("source_channel", None),
        "pub_time": Int64(int(raw.get("createTime", 0))),
        "crawl_time": int(datetime.now(VN_TZ).timestamp()),
        # "org_id": channel.get("org_id", None),
        "subject_id": raw.get("subject_id", None),
        "title": raw.get("title", None),
        "description": raw.get("description", None),
        "content": raw.get("desc", None),
        "url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
        "media_urls": "[]",
        "comments": raw.get("stats", {}).get("commentCount", 0),
        "shares": raw.get("stats", {}).get("shareCount", 0),
        "reactions": raw.get("stats", {}).get("diggCount", 0),
        "favors": int(raw.get("stats", {}).get("collectCount", 0) or 0),
        "views": raw.get("stats", {}).get("playCount", 0),
        "web_tags": "[]",#json.dumps(raw.get("diversificationLabels", [])),
        "web_keywords": "[]",# json.dumps(raw.get("suggestedWords", [])),
        "auth_id": raw.get("author", {}).get("id", ""),
        "auth_name": raw.get("author", {}).get("nickname", ""),
        "auth_type": 1,
        "auth_url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
        "source_id": raw.get("id", None),
        "source_type": channel.get("source_type", None),
        "source_name": channel.get("source_name", None),
        "source_url": channel.get("source_url", None),
        "reply_to": None,
        "level": None,
        "sentiment": 0,
        # "isPriority": False,
        "crawl_bot": "tiktok_post",
    }