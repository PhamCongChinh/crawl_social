# import asyncio
# from datetime import datetime
# import json
# import time
# from zoneinfo import ZoneInfo

# from bson import Int64
# import requests
# from app.modules.elastic_search.service import postToES
# from app.modules.scheduler.models.jobs_log import JobLog
# # from app.modules.tiktok_scraper.api.channel import crawl_channels, get_all_sources
# # from app.modules.tiktok_scraper.api.post import crawl_posts
# # from app.modules.tiktok_scraper.api.channel import crawl_channels
# from app.modules.tiktok_scraper.models.channel import ChannelModel
# # from app.modules.tiktok_scraper.models.source import SourceModel
# # from app.modules.tiktok_scraper.scrapers.channel import scrape_channel
# # from app.modules.tiktok_scraper.scrapers.post import scrape_posts
# # from app.modules.tiktok_scraper.services.channel import ChannelService
# # from app.modules.tiktok_scraper.services.post import PostService
# # from app.modules.tiktok_scraper.services.source import SourceService

# from app.modules.tiktok_scraper.scrapers.channel import scrape_channel
# from app.modules.tiktok_scraper.scrapers.post import scrape_posts
# from app.modules.tiktok_scraper.services.channel import ChannelService
# from app.utils.safe_scrape import safe_scrape
# from app.utils.delay import async_delay
# from app.worker import celery_app

# import logging
# log = logging.getLogger(__name__)

# from app.modules.tiktok_scraper.models.source import SourceModel
# from app.config import mongo_connection

# # @celery_app.task(
# #     name="app.tasks.crawl_tiktok_channels",
# #     autoretry_for=(Exception,),
# #     retry_backoff=True,
# #     retry_kwargs={"max_retries": 3, "countdown": 10}
# # )
# # def crawl_tiktok_channels(source: dict):
# #     async def do_crawl():
# #         try:
# #             await mongo_connection.connect()

# #             source_model = SourceModel(**source)
# #             log.warning(f"🚀 Crawling source {source_model.source_url}")

# #             data = await safe_scrape(source_model.source_url)
# #             if not data:
# #                 log.warning(f"⚠️ Không lấy được dữ liệu từ {source_model.source_url}")
# #                 return {
# #                     "status": "error",
# #                     "url": source_model.source_url,
# #                     "message": "Không lấy được dữ liệu (data=None)",
# #                     "type": "NoData"
# #                 }

# #             await async_delay(1, 3)
# #             log.info(f"Đang upsert {len(data)} channels vào cơ sở dữ liệu")

# #             result = await ChannelService.upsert_channels_bulk(data, source=source_model)
# #             log.info(
# #                 f"Bulk upsert xong: matched={result.matched_count}, "
# #                 f"inserted={result.upserted_count}, modified={result.modified_count}"
# #             )

# #             return {
# #                 "status": "success",
# #                 "url": source_model.source_url,
# #                 "matched": result.matched_count,
# #                 "inserted": result.upserted_count,
# #                 "modified": result.modified_count,
# #                 "total": len(data),
# #             }

# #         except Exception as e:
# #             log.error(f"❌ Lỗi khi crawl {source.get('source_url')}: {e}")
# #             return {
# #                 "status": "error",
# #                 "url": source.get("source_url", "unknown"),
# #                 "message": str(e),
# #                 "type": type(e).__name__,
# #             }

# #     return asyncio.run(do_crawl())

# # @celery_app.task(
# #     name="app.tasks.crawl_tiktok_posts",
# #     autoretry_for=(Exception,),
# #     retry_backoff=True,
# #     retry_kwargs={'max_retries': 3}
# # )
# # def crawl_tiktok_posts(channel: dict):
# #     async def do_crawl():
# #         try:
# #             await mongo_connection.connect()
# #             channel_model = ChannelModel(**channel)
# #             log.warning(f"🚀 Crawling source {channel_model.id}")
# #             # data = await safe_scrape(channel_model.source_url)
# #             data = await scrape_posts(urls=[channel_model.id])

# #             # Nếu scrape_posts trả về dict lỗi
# #             if isinstance(data, dict) and data.get("status") == "error":
# #                 log.error(f"❌ Scrap lỗi: {data}")
# #                 return data
            
# #             if not data or len(data) == 0:
# #                 log.error(f"❌ Không có dữ liệu từ channel {channel_model.id}")
# #                 return {
# #                     "status": "error",
# #                     "url": channel_model.source_url,
# #                     "message": "Không có dữ liệu",
# #                     "type": "NoData",
# #                 }

# #             # Xử lý post đầu tiên
# #             post = flatten_post_data(data[0], channel=channel_model)
# #             log.info(f"✅ Thêm vào flatten: {post.get('id')}")
            
# #             await postToES([post])
# #             await ChannelService.channel_crawled(channel_model.id)
# #             return {"status": "success", "id": post.get("id")}
# #         except Exception as e:
# #             log.exception(f"❌ Lỗi khi crawl {channel.get('id')}")
# #             return {
# #                 "status": "error",
# #                 "url": channel.get("source_url", "unknown"),
# #                 "message": str(e),
# #                 "type": type(e).__name__,
# #             }
# #     return asyncio.run(do_crawl())










# @celery_app.task(name="app.tasks.crawl_tiktok", bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
# def crawl_tiktok(self, job_id: str, channel_id: str):
#     async def do_crawl():
#         try:
#             await mongo_connection.connect()
#             print(f"[{job_id}] Crawling TikTok for channel {channel_id}")
#             # await crawl_channels()
#             # await crawl_posts()
#             await JobLog(job_id=job_id, status="success", message="Crawl thành công").insert()
#             return {"message":"Thành công"}
#         except Exception as e:
#             await JobLog(job_id=job_id, status="error", message=str(e)).insert()
#             raise e  # để Celery tự retry
#         finally:
#             await mongo_connection.disconnect()  # 👈 chỉ chạy nếu connect thành công
#     asyncio.run(do_crawl())  # ✅ chỉ gọi 1 lần


# VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

# def flatten_post_data(raw: dict, channel: ChannelModel) -> dict:
#     return {
#         "id": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
#         "doc_type": 1, # POST = 1, COMMENT = 2
#         "crawl_source": 2,
#         "crawl_source_code": "tt",
#         "pub_time": Int64(int(raw.get("createTime", 0))),
#         "crawl_time": int(datetime.now(VN_TZ).timestamp()),
#         "org_id": channel.org_id,
#         "subject_id": "",
#         "title": raw.get("desc", ""),
#         "description": raw.get("desc", ""),
#         "content": raw.get("desc", ""),
#         "url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
#         # "media_urls": raw.get("video", {}).get("media_urls", ""),#khong duoc rông
#         "media_urls": json.dumps(raw.get("media_urls", [])),
#         "comments": raw.get("stats", {}).get("commentCount", 0),
#         "shares": raw.get("stats", {}).get("shareCount", 0),
#         "reactions": raw.get("stats", {}).get("diggCount", 0),
#         "favors": int(raw.get("stats", {}).get("collectCount", 0) or 0),
#         "views": raw.get("stats", {}).get("playCount", 0),
#         # "web_tags": ", ".join(raw.get("diversificationLabels", [])),  #khong duoc rông
#         # "web_keywords": "",#khong duoc rông
#         "web_tags": json.dumps(raw.get("diversificationLabels", [])),
#         "web_keywords": json.dumps(raw.get("suggestedWords", [])),
#         "auth_id": raw.get("author", {}).get("id", ""),
#         "auth_name": raw.get("author", {}).get("nickname", ""),
#         "auth_type": 1,
#         "auth_url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
#         "source_id": raw.get("id", ""),
#         "source_type": 5,
#         "source_name": raw.get("author", {}).get("nickname", ""),
#         "source_url": channel.source_url,
#         "reply_to": "",
#         "level": 0 ,
#         "sentiment": 0,
#         "isPriority": True,
#         "crawl_bot": "tiktok_post",
#     }