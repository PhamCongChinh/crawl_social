import asyncio
from datetime import datetime
import json
import logging
from zoneinfo import ZoneInfo

from bson import Int64

from app.modules.elastic_search.service import postToES, postToESUnclassified
from app.modules.tiktok_scraper.scrapers.search import scrape_search
log = logging.getLogger(__name__)

from app.config import mongo_connection
from app.worker import celery_app

@celery_app.task(
    name="app.tasks.tiktok.post.crawl_tiktok_search",
    bind=True
)
def crawl_tiktok_search(self, job_id: str, channel_id: str):
    print(f"Task search {job_id} - {channel_id}")
    async def do_crawl():
        try:
            await mongo_connection.connect()
            search = await scrape_search(keyword="T&T GROUP", max_search=18)
            data = flatten_post_list(search)

            # await postToESUnclassified(data)
            await postToES(data)
        except Exception as e:
            log.error(e)
    return asyncio.run(do_crawl())


VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

def flatten_post_data(raw: dict) -> dict:
    return {
        "id": raw.get("id", ""),
        "doc_type": 1, # POST = 1, COMMENT = 2
        "crawl_source": 2,
        "crawl_source_code": "tt",
        "pub_time": Int64(int(raw.get("createTime", 0))),
        "crawl_time": int(datetime.now(VN_TZ).timestamp()),
        "org_id": 2,
        "subject_id": "",
        "title": "",
        "description": raw.get("desc", ""),
        "content": raw.get("desc", ""),
        "url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
        # "media_urls": raw.get("video", {}).get("media_urls", ""),#khong duoc rông
        # "media_urls": json.dumps(raw.get("media_urls", [])),
        "media_urls": "[]",
        "comments": raw.get("stats", {}).get("commentCount", 0),
        "shares": raw.get("stats", {}).get("shareCount", 0),
        "reactions": raw.get("stats", {}).get("diggCount", 0),
        "favors": int(raw.get("stats", {}).get("collectCount", 0) or 0),
        "views": raw.get("stats", {}).get("playCount", 0),
        # "web_tags": ", ".join(raw.get("diversificationLabels", [])),  #khong duoc rông
        # "web_keywords": "",#khong duoc rông
        # "web_tags": json.dumps(raw.get("diversificationLabels", [])),
        # "web_keywords": json.dumps(raw.get("suggestedWords", [])),
        "web_tags": "[]",
        "web_keywords": "[]",
        "auth_id": raw.get("author", {}).get("id", ""),
        "auth_name": raw.get("author", {}).get("nickname", ""),
        "auth_type": 1,
        "auth_url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}",
        "source_id": raw.get("id", ""),
        "source_type": 5,
        "source_name": raw.get("author", {}).get("nickname", ""),
        "source_url": f"https://www.tiktok.com/@{raw.get('author', {}).get('uniqueId', '')}/video/{raw['id']}",
        "reply_to": "",
        "level": 0 ,
        "sentiment": 0,
        "isPriority": True,
        "crawl_bot": "tiktok_post",
    }


def flatten_post_list(raw_list: list[dict]) -> list[dict]:
    return [flatten_post_data(item) for item in raw_list]