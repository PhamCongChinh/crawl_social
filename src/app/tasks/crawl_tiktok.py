import asyncio
from datetime import datetime
import json
from zoneinfo import ZoneInfo

from bson import Int64
from app.modules.elastic_search.service import postToES
from app.modules.scheduler.models.jobs_log import JobLog
from app.modules.tiktok_scraper.api.channel import crawl_channels, get_all_sources
from app.modules.tiktok_scraper.api.post import crawl_posts
from app.modules.tiktok_scraper.models.channel import ChannelModel
from app.modules.tiktok_scraper.scrapers.channel import scrape_channel
from app.modules.tiktok_scraper.scrapers.post import scrape_posts
from app.modules.tiktok_scraper.services.post import PostService
from app.modules.tiktok_scraper.services.source import SourceService
from app.worker import celery_app
from celery import shared_task

import logging
log = logging.getLogger(__name__)

from app.config import mongo_connection

@celery_app.task(name="app.tasks.crawl_tiktok", bind=True, autoretry_for=(Exception,), retry_backoff=True, retry_kwargs={'max_retries': 3})
def crawl_tiktok(self, job_id: str, channel_id: str):
    async def do_crawl():
        try:
            await mongo_connection.connect()

            print(f"[{job_id}] Crawling TikTok for channel {channel_id}")

            
            # sources = await SourceService.get_sources()
            # for source in sources:
            #     log.info(f"Đang crawl channels cho {source.source_name} từ {source.source_url}")
            #     channels = await scrape_channel(url=source.source_url) # data, source

            #     # có source và channels
            #     flatten = []
            #     for channel in channels:
            #         data = await scrape_posts(urls=[channel.id])
            #         log.info(channel.id)
            #         if data and len(data) > 0:
            #             post = flatten_post_data(data[0], channel=channel)
            #             flatten.append(post)
            #             print(f"✅ Thêm vào flatten: {post['id']}")
            #         else:
            #             print(f"❌ Không có data từ channel {channel.id}")
            #         break
            #     await PostService.upsert_posts_bulk(flatten)
            #     result = await postToES(flatten)

            await crawl_channels()
            await crawl_posts()

            await JobLog(job_id=job_id, status="success", message="Crawl thành công").insert()
            return {"message":"Thành công"}
        except Exception as e:
            await JobLog(job_id=job_id, status="error", message=str(e)).insert()
            raise e  # để Celery tự retry

        finally:
            await mongo_connection.disconnect()  # 👈 chỉ chạy nếu connect thành công

    asyncio.run(do_crawl())  # ✅ chỉ gọi 1 lần


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