import asyncio
from datetime import datetime
import json
from typing import List
from zoneinfo import ZoneInfo

from bson import Int64
from app.modules.elastic_search.service import postToES, postToESUnclassified
from app.modules.tiktok_scraper.models.channel import ChannelModel
from app.modules.tiktok_scraper.scrapers.comment import scrape_comments
from app.modules.tiktok_scraper.scrapers.post import scrape_posts
from app.modules.tiktok_scraper.services.channel import ChannelService
from app.modules.tiktok_scraper.services.post import PostService
from app.utils.concurrency import limited_gather
from app.utils.delay import async_delay
from app.worker import celery_app
from beanie.operators import In, And

import logging
log = logging.getLogger(__name__)

from app.config import mongo_connection
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


@celery_app.task(
    queue="tiktok_platform",
    name="app.tasks.tiktok.channel.crawl_tiktok_posts_hourly"
)
def crawl_tiktok_posts_hourly(job_name:str, crawl_type: str):
    async def do_crawl():
        try:
            log.info("Lấy dữ liệu bài viết hằng ngày")
            await mongo_connection.connect()
            videos = await ChannelService.get_channels_posts_hourly()
            if len(videos) == 0:
                log.info("Không có dữ liệu trong ngày")
                await mongo_connection.disconnect()
                return
            ids = [str(v.id) for v in videos]
            video_dicts = [v.model_dump() for v in videos]
            await ChannelModel.find(In(ChannelModel.id, ids)).update_many({"$set": {"status": "processing"}})
            log.info(f"🚀 Đang cào {len(video_dicts)} video")
            data_list_classified = []
            data_list_unclassified = []
            for index, video in enumerate(video_dicts):
                log.info(f"🕐 [{index+1}/{len(video_dicts)}] {video['id']}")
                if video["org_id"] == 0:
                    data_list_unclassified.append(video)
                else:
                    data_list_classified.append(video)
            # Crawl & post classified
            if data_list_classified:
                post_data_classified = await crawl_tiktok_post_list_direct_classified(data_list_classified)
                if post_data_classified:
                    await postToES(post_data_classified)
                    log.info(f"Đã thêm {len(post_data_classified)} video đã phân loại vào ElasticSearch")
            # Crawl & post unclassified
            if data_list_unclassified:
                post_data_unclassified = await crawl_tiktok_post_list_direct_unclassified(data_list_unclassified)
                if post_data_unclassified:
                    await postToESUnclassified(post_data_unclassified)
                    log.info(f"Đã thêm {len(post_data_unclassified)} video chưa phân loại vào ElasticSearch")
            log.info(f"📦 Tổng số video đã lấy: {len(data_list_classified) + len(data_list_unclassified)}")
            await mongo_connection.disconnect()
        except Exception as e:
            log.error(f"❌ Lỗi khi cào dữ liệu: {e}")
            await mongo_connection.disconnect()
    return asyncio.run(do_crawl())

async def crawl_tiktok_post_list_direct_classified(channels: list[dict]):
    try:
        log.info(f"📦 Tổng số channel: {len(channels)} classified")
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
        await async_delay(2, 4)  # Đảm bảo browser session trước shutdown
        return posts_data

    except Exception as e:
        log.error(e)

async def crawl_tiktok_post_list_direct_unclassified(channels: list[dict]):
    try:
        log.info(f"📦 Tổng số channel: {len(channels)} unclassified")
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

        await async_delay(2, 4)  # Đảm bảo browser session trước shutdown
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




async def crawl_posts(from_date: int, to_date: int):
    try:
        print(f"Crawling từ {from_date} đến {to_date}")
    except Exception as e:
        log.error(e)


BATCH_SIZE = 50
@celery_app.task(
    name="app.tasks.tiktok.post.crawl_tiktok_posts",
)
def crawl_tiktok_posts(from_date: int, to_date: int):

    async def do_crawl():
        try:
            await mongo_connection.connect()
            videos = await ChannelService.get_posts_backdate(from_date=from_date, to_date=to_date)
            log.info(f"🚀 Đang cào {len(videos)}")
            video_dicts = [v.model_dump() for v in videos]
            # Chia batch
            batches = [video_dicts[i:i + BATCH_SIZE] for i in range(0, len(video_dicts), BATCH_SIZE)]
            log.info(f"📦 Tổng cộng {len(videos)} video, chia thành {len(batches)} batch (mỗi batch {BATCH_SIZE} video)")
            for i, batch in enumerate(batches, start=1):
                log.info(f"🚀 Batch {i}/{len(batches)}: {len(batch)} video")

                # 1. Update status
                ids = [v["id"] for v in batch]
                await ChannelModel.find(In(ChannelModel.id, ids)).update_many({
                    "$set": {"status": "processing"}
                })

                await _crawl_batch_async(batch, i, len(batches))  # ✅ xử lý tuần tự từng batch
                await async_delay(240,300)
            return {"message": "Đã xử lý toàn bộ batch", "total_videos": len(videos)}
        except Exception as e:
            log.error(e)
            await mongo_connection.disconnect()
    return asyncio.run(do_crawl())


async def _crawl_batch_async(videos: list[dict], batch_index: int, total_batches: int):
    log.info(f"🔧 Bắt đầu xử lý batch {batch_index}/{total_batches} với {len(videos)} video")
    data_list_classified = []
    data_list_unclassified = []
    for index, video in enumerate(videos):
        log.info(f"🕐 [{index + 1}/{len(videos)}] Video ID: {video['id']}")
        # Phân loại
        if video["org_id"] == 0:
            data_list_unclassified.append(video)
        else:
            data_list_classified.append(video)

    log.info(f"🎯 Batch {batch_index}: {len(data_list_classified)} classified, {len(data_list_unclassified)} unclassified")
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

# def _chunk_sources(sources: List, batch_size: int) -> List[List]:
#     return [sources[i:i + batch_size] for i in range(0, len(sources), batch_size)]

# async def crawl_tiktok_post_list_direct(channels: list[dict]):
#     try:
#         await mongo_connection.connect()
#         log.info(f"📦 Tổng số channel: {len(channels)}")
#         urls = [item["_id"] for item in channels]
#         posts_data = []
#         data = await scrape_posts(urls)
#         for channel in channels:
#             for post in data:
#                 if post.get("id") == channel["_id"].strip("/").split("/")[-1]:
#                     flatten = flatten_post_data_1(post, channel)
#                     await ChannelService.channel_crawled(channel["_id"])
#                     posts_data.append(flatten)

#         await async_delay(2, 4)  # Đảm bảo browser session trước shutdown
#         return posts_data

#     except Exception as e:
#         log.error(e)

# async def crawl_tiktok_post_list_direct_unclassified(channels: list[dict]):
#     try:
#         await mongo_connection.connect()
#         log.info(f"📦 Tổng số channel: {len(channels)}")
#         urls = [item["_id"] for item in channels]
#         posts_data = []
#         data = await scrape_posts(urls)
#         for channel in channels:
#             for post in data:
#                 if post.get("id") == channel["_id"].strip("/").split("/")[-1]:
#                     flatten = flatten_post_data_unclassified_1(post, channel)
#                     await ChannelService.channel_crawled(channel["_id"])
#                     posts_data.append(flatten)

#         await async_delay(2, 4)  # Đảm bảo browser session trước shutdown
#         return posts_data

#     except Exception as e:
#         log.error(e)

# def flatten_post_data_1(raw: dict, channel: dict) -> dict:
#     return {
#         "id": raw.get("id", None),
#         "doc_type": 1,  # POST = 1, COMMENT = 2
#         "crawl_source": 2,
#         "crawl_source_code": channel.get("source_channel", None),
#         "pub_time": Int64(int(raw.get("createTime", 0))),
#         "crawl_time": int(datetime.now(VN_TZ).timestamp()),
#         "org_id": channel.get("org_id", None),
#         "subject_id": raw.get("subject_id", None),
#         "title": raw.get("title", None),
#         "description": raw.get("description", None),
#         "content": raw.get("desc", None),
#         "url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
#         "media_urls": "[]",
#         "comments": raw.get("stats", {}).get("commentCount", 0),
#         "shares": raw.get("stats", {}).get("shareCount", 0),
#         "reactions": raw.get("stats", {}).get("diggCount", 0),
#         "favors": int(raw.get("stats", {}).get("collectCount", 0) or 0),
#         "views": raw.get("stats", {}).get("playCount", 0),
#         "web_tags": json.dumps(raw.get("diversificationLabels", [])),
#         "web_keywords": json.dumps(raw.get("suggestedWords", [])),
#         "auth_id": raw.get("author", {}).get("id", ""),
#         "auth_name": raw.get("author", {}).get("nickname", ""),
#         "auth_type": 1,
#         "auth_url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
#         "source_id": raw.get("id", None),
#         "source_type": channel.get("source_type", None),
#         "source_name": channel.get("source_name", None),
#         "source_url": channel.get("source_url", None),
#         "reply_to": raw.get("replyTo", None),
#         "level": raw.get("level", None) or 0,
#         "sentiment": 0,
#         "isPriority": True,
#         "crawl_bot": "tiktok_post",
#     }

# def flatten_post_data_unclassified_1 (raw: dict, channel: dict) -> dict:
#     return {
#         # "id": raw.get("id", None),
#         "doc_type": 1,  # POST = 1, COMMENT = 2
#         "crawl_source": 2,
#         "crawl_source_code": channel.get("source_channel", None),
#         "pub_time": Int64(int(raw.get("createTime", 0))),
#         "crawl_time": int(datetime.now(VN_TZ).timestamp()),
#         # "org_id": channel.get("org_id", None),
#         "subject_id": raw.get("subject_id", None),
#         "title": raw.get("title", None),
#         "description": raw.get("description", None),
#         "content": raw.get("desc", None),
#         "url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
#         "media_urls": "[]",
#         "comments": raw.get("stats", {}).get("commentCount", 0),
#         "shares": raw.get("stats", {}).get("shareCount", 0),
#         "reactions": raw.get("stats", {}).get("diggCount", 0),
#         "favors": int(raw.get("stats", {}).get("collectCount", 0) or 0),
#         "views": raw.get("stats", {}).get("playCount", 0),
#         "web_tags": json.dumps(raw.get("diversificationLabels", [])),
#         "web_keywords": json.dumps(raw.get("suggestedWords", [])),
#         "auth_id": raw.get("author", {}).get("id", ""),
#         "auth_name": raw.get("author", {}).get("nickname", ""),
#         "auth_type": 1,
#         "auth_url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
#         "source_id": raw.get("id", None),
#         "source_type": channel.get("source_type", None),
#         "source_name": channel.get("source_name", None),
#         "source_url": channel.get("source_url", None),
#         "reply_to": raw.get("replyTo", None),
#         "level": raw.get("level", None) or 0,
#         "sentiment": 0,
#         # "isPriority": False,
#         "crawl_bot": "tiktok_post",
#     }
































# async def crawl_tiktok_post_direct(channel: dict):
#     try:
#         await mongo_connection.connect()
#         channel_model = ChannelModel(**channel)

#         log.info(f"🔍 Crawling source: {channel_model.id}")
        
#         data = await safe_scrape_with_delay(channel_model.id)
        
#         if not data or not data[0]:
#             log.warning(f"⚠️ Không lấy được dữ liệu từ {channel_model.id}")
#             return
        
#         await async_delay(2,4)

#         # comments = await safe_scrape_with_delay_comments(channel_model.id) # Trả 1 list comment
#         # comments = await safe_scrape_with_delay_comments("https://www.tiktok.com/@vietjetvietnam/video/7520222989380553992")
#         # if not comments or not comments[0]:
#         #     log.warning(f"⚠️ Không lấy được dữ liệu comments từ {channel_model.id}")
#         #     return
#         # comments_to_es = []
#         # for comment in comments:
#         #     c = flatten_post_data_comment(comment, channel=channel_model)
#         #     comments_to_es.append(c)
#         # await async_delay(1,2)
#         # print(comments_to_es)
#         # await postToES(comments_to_es)

#         # Phân loại ở đây
#         if channel_model.org_id == 0:
#             post = flatten_post_data_unclassified(data[0], channel=channel_model)
#             log.info(f"✅ Thêm vào flatten org_id = 0: {channel_model.id}")
#             print(f"post: {post}")
#             await postToESUnclassified([post])
            
#         else:
#             post = flatten_post_data(data[0], channel=channel_model)
#             log.info(f"✅ Thêm vào flatten org_id != 0: {post.get('id')}")
#             await postToES([post])

#         await async_delay(2,3)

#         return {"status": "success"}
#     except Exception as e:
#         # log.error(f"❌ Lỗi crawl {post.get('source_url')}: {e}")
#         log.error(e)


# async def safe_scrape_with_delay(url: str, max_retries: int = 3):
#     for attempt in range(1, max_retries + 1):
#         try:
#             data = await scrape_posts([url])
#             await async_delay(2, 4)  # Đảm bảo browser session trước shutdown
#             return data
#         except Exception as e:
#             log.warning(f"❗ Attempt {attempt}/{max_retries} - Lỗi scrape: {e}")
#             if attempt < max_retries:
#                 await async_delay(5, 8)  # Delay lâu hơn nếu lỗi
#             else:
#                 log.error(f"❌ Bỏ qua URL sau {max_retries} lần thử: {url}")
#                 return None
            


# def flatten_post_data(raw: dict, channel: ChannelModel) -> dict:
#     return {
#         "id": raw.get("id", None),
#         "doc_type": 1, # POST = 1, COMMENT = 2
#         "crawl_source": 2,
#         "crawl_source_code": channel.source_channel,
#         "pub_time": Int64(int(raw.get("createTime", 0))),
#         "crawl_time": int(datetime.now(VN_TZ).timestamp()),
#         "org_id": channel.org_id,
#         "subject_id": raw.get("subject_id", None),
#         "title": raw.get("title", None),
#         "description": raw.get("description", None),
#         "content": raw.get("desc", None),
#         "url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
#         "media_urls": "[]",
#         "comments": raw.get("stats", {}).get("commentCount", 0),
#         "shares": raw.get("stats", {}).get("shareCount", 0),
#         "reactions": raw.get("stats", {}).get("diggCount", 0),
#         "favors": int(raw.get("stats", {}).get("collectCount", 0) or 0),
#         "views": raw.get("stats", {}).get("playCount", 0),
#         "web_tags": json.dumps(raw.get("diversificationLabels", [])),
#         "web_keywords": json.dumps(raw.get("suggestedWords", [])),
#         "auth_id": raw.get("author", {}).get("id", ""),
#         "auth_name": raw.get("author", {}).get("nickname", ""),
#         "auth_type": 1,
#         "auth_url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
#         "source_id": raw.get("id", None),
#         "source_type": channel.source_type,
#         "source_name": channel.source_name,
#         "source_url": channel.source_url,
#         "reply_to": raw.get("replyTo", None),
#         "level": raw.get("level", None) or 0,
#         "sentiment": 0,
#         "isPriority": True,
#         "crawl_bot": "tiktok_post",
#     }


# def flatten_post_data_unclassified (raw: dict, channel: ChannelModel) -> dict:
#     return {
#         # "id": raw.get("id", ""),
#         "doc_type": 1, # POST = 1, COMMENT = 2
#         "crawl_source": 2,
#         "crawl_source_code": channel.source_channel,
#         "pub_time": Int64(int(raw.get("createTime", 0))),
#         "crawl_time": int(datetime.now(VN_TZ).timestamp()),
#         # "org_id": None,#channel.org_id,
#         "subject_id": raw.get("subject_id", None),
#         "title": raw.get("title", None),
#         "description": raw.get("description", None),
#         "content": raw.get("desc", None),
#         "url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
#         "media_urls": "[]",
#         "comments": raw.get("stats", {}).get("commentCount", 0),
#         "shares": raw.get("stats", {}).get("shareCount", 0),
#         "reactions": raw.get("stats", {}).get("diggCount", 0),
#         "favors": int(raw.get("stats", {}).get("collectCount", 0) or 0),
#         "views": raw.get("stats", {}).get("playCount", 0),
#         "web_tags": json.dumps(raw.get("diversificationLabels", [])),
#         "web_keywords": json.dumps(raw.get("suggestedWords", [])),
#         "auth_id": raw.get("author", {}).get("id", None),
#         "auth_name": raw.get("author", {}).get("nickname", None),
#         "auth_type": 1,
#         "auth_url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
#         "source_id": raw.get("id", None),
#         "source_type": channel.source_type,
#         "source_name": channel.source_name,
#         "source_url": channel.source_url,
#         "reply_to": raw.get("replyTo", None),
#         "level": raw.get("level", None) or 0,
#         "sentiment": 0,
#         "isPriority": False,
#         "crawl_bot": "tiktok_post",
#     }