import asyncio
from datetime import datetime
import json
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

from bson import Int64
from beanie.operators import In, And

from app.modules.elastic_search.service import postToES, postToESUnclassified
# from app.modules.tiktok_scraper.models.channel import ChannelModel
from app.modules.tiktok_scraper.models.video import VideoModel
from app.modules.tiktok_scraper.scrapers.post import scrape_posts
from app.modules.tiktok_scraper.scrapers.search import scrape_search
# from app.modules.tiktok_scraper.services.channel import ChannelService
from app.modules.tiktok_scraper.services.video import VideoService
from app.modules.tiktok_scraper.services.search import SearchService
from app.utils.delay import async_delay
log = logging.getLogger(__name__)

from app.config import mongo_connection
from app.worker import celery_app

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

output = Path("logs")
output.mkdir(parents=True, exist_ok=True)
error_log_file = output / "keyword_error.json"

# @celery_app.task(
#     name="app.tasks.tiktok.post.crawl_tiktok_search_hourly",
# )
# def crawl_tiktok_search(job_name: str, crawl_type: str):
#     print(f"Task search {job_name} - {crawl_type}")
#     async def do_crawl():
#         try:
#             await mongo_connection.connect()
#             keywords = await SearchService.get_keywords()
#             if len(keywords) == 0:
#                 await mongo_connection.disconnect()
#                 return
#             BATCH_SIZE = 2
#             keyword_dicts = [k.model_dump() for k in keywords]
#             batches = [keyword_dicts[i:i + BATCH_SIZE] for i in range(0, len(keyword_dicts), BATCH_SIZE)]
#             log.info(f"📦 Tổng cộng {len(keywords)} từ khóa, chia thành {len(batches)} batch (mỗi batch {BATCH_SIZE} từ khóa)")
#             for i, batch in enumerate(batches, start=1):
#                 log.info(f"🚀 Batch {i}/{len(batches)}: {len(batch)} từ khóa")
#                 await _crawl_batch_async(batch, i, len(batches))
#                 await async_delay(20,30)
#         except Exception as e:
#             log.error(e)
#     return asyncio.run(do_crawl())


# async def _crawl_batch_async(keywords: list[dict], batch_index: int, total_batches: int):
#     log.info(f"🔧 Bắt đầu xử lý batch {batch_index}/{total_batches} với {len(keywords)} từ khóa")
#     for index, keyword in enumerate(keywords):
#         log.info(f"🕐 [{index + 1}/{len(keywords)}] Từ khóa: {keyword['keyword']}")
#         scrape_data = await scrape_search(keyword=keyword["keyword"], max_search=18)
#         with open(output.joinpath("search.json"), "w", encoding="utf-8") as file:
#             json.dump(scrape_data, file, indent=2, ensure_ascii=False)
#         if not scrape_data:
#             log.warning(f"⚠️ Không lấy được dữ liệu từ {keyword['keyword']}")
#             return
#         # print(scrape_data)
#         print(keyword)
#         result = await VideoService.upsert_channels_bulk_keyword(scrape_data, keyword)
#         # result = await ChannelService.upsert_channels_bulk_keyword(scrape_data, keyword)
#         # log.info(
#         #     f"✅ Upsert xong: matched={result.matched_count}, "
#         #     f"inserted={result.upserted_count}, modified={result.modified_count}"
#         # )











BATCH_SIZE = 50

@celery_app.task(
    queue="tiktok_posts",
    name="app.tasks.tiktok.channel.crawl_tiktok_posts_keyword"
)
def crawl_tiktok_search_video(job_name: str, crawl_type: str):
    async def do_crawl():
        try:
            log.info("Lấy dữ liệu bài viết hằng ngày")
            await mongo_connection.connect()
            videos = await VideoService.get_videos()
            if len(videos) == 0:
                log.info("Không có dữ liệu trong ngày")
                await mongo_connection.disconnect()
                return
            
            video_dicts = [v.model_dump() for v in videos]
            # Chia batch
            batches = [video_dicts[i:i + BATCH_SIZE] for i in range(0, len(video_dicts), BATCH_SIZE)]
            log.info(f"📦 Tổng cộng {len(videos)} video, chia thành {len(batches)} batch (mỗi batch {BATCH_SIZE} video)")
            for i, batch in enumerate(batches, start=1):
                log.info(f"🚀 Batch {i}/{len(batches)}: {len(batch)} video")

                # 1. Update status
                ids = [v["video_id"] for v in batch]
                await VideoModel.find(In(VideoModel.video_id, ids)).update_many({
                    "$set": {"status": "processing"}
                })

                await _crawl_batch_async_video(batch, i, len(batches))  # ✅ xử lý tuần tự từng batch
                await async_delay(10,15)
            return {"message": "Đã xử lý toàn bộ batch", "total_videos": len(videos)}
        except Exception as e:
            log.error(f"❌ Lỗi khi cào dữ liệu: {e}")
            await mongo_connection.disconnect()
        await mongo_connection.disconnect()
    return asyncio.run(do_crawl())


async def _crawl_batch_async_video(videos: list[dict], batch_index: int, total_batches: int):
    log.info(f"🔧 Bắt đầu xử lý batch {batch_index}/{total_batches} với {len(videos)} video")
    data_list_classified = []
    data_list_unclassified = []
    for index, video in enumerate(videos):
        log.info(f"🕐 [{index + 1}/{len(videos)}] Video ID: {video['video_id']}")
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


async def crawl_tiktok_post_list_direct_classified(videos: list[dict]):
    try:
        log.info(f"📦 Tổng số video: {len(videos)} classified")
        urls = [item["video_url"] for item in videos]
        posts_data = []
        data = await scrape_posts(urls)
        processed_ids = []
        for video in videos:
            for post in data:
                if post.get("id") == video["video_id"]:
                    flatten = flatten_post_data(post, video)
                    posts_data.append(flatten)
                    processed_ids.append(video["video_id"])  # Lưu lại _id cần đánh dấu
        if processed_ids:
            await VideoModel.find(In(VideoModel.video_id, processed_ids)).update_many(
                {"$set": {"status": "done"}}
            )
        await async_delay(2, 4)  # Đảm bảo browser session trước shutdown
        return posts_data

    except Exception as e:
        log.error(e)

async def crawl_tiktok_post_list_direct_unclassified(videos: list[dict]):
    try:
        log.info(f"📦 Tổng số channel: {len(videos)} unclassified")
        urls = [item["video_url"] for item in videos]
        posts_data = []
        data = await scrape_posts(urls)
        processed_ids = []
        for video in videos:
            for post in data:
                if post.get("id") == video["video_id"]:
                    flatten = flatten_post_data_unclassified(post, video)
                    posts_data.append(flatten)
                    processed_ids.append(video["video_id"])  # Lưu lại _id cần đánh dấu
        if processed_ids:
            await VideoModel.find(In(VideoModel.video_id, processed_ids)).update_many(
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


# def flatten_post_data(raw: dict) -> dict:
#     return {
#         "id": raw.get("id", None),
#         "doc_type": 1,  # POST = 1, COMMENT = 2
#         "crawl_source": 2,
#         "crawl_source_code": "tt",#channel.get("source_channel", None),
#         "pub_time": Int64(int(raw.get("createTime", 0))),
#         "crawl_time": int(datetime.now(VN_TZ).timestamp()),
#         "org_id": 2,#channel.get("org_id", None),
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
#         "web_tags": "[]",#json.dumps(raw.get("diversificationLabels", [])),
#         "web_keywords": "[]",# json.dumps(raw.get("suggestedWords", [])),
#         "auth_id": raw.get("author", {}).get("id", ""),
#         "auth_name": raw.get("author", {}).get("nickname", ""),
#         "auth_type": 1,
#         "auth_url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
#         "source_id": raw.get("id", None),
#         "source_type": 5,#channel.get("source_type", None),
#         "source_name": "Đỗ Mỹ Linh",#channel.get("source_name", None),
#         "source_url": "https://www.tiktok.com/@mylinhdo",#channel.get("source_url", None),
#         "reply_to": None,
#         "level": None,
#         "sentiment": 0,
#         "isPriority": True,
#         "crawl_bot": "tiktok_post",
#     }

# def flatten_post_list(raw_list: list[dict]) -> list[dict]:
#     return [flatten_post_data(item) for item in raw_list]