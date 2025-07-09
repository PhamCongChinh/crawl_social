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

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

@celery_app.task(
    name="app.tasks.tiktok.post.crawl_tiktok_search_hourly",
)
def crawl_tiktok_search(job_name: str, crawl_type: str):
    print(f"Task search {job_name} - {crawl_type}")
    async def do_crawl():
        try:
            await mongo_connection.connect()
            # search = await scrape_search(keyword="Đỗ Mỹ Linh", max_search=18)
            search = await scrape_search(keyword="Đỗ Mỹ Linh", max_search=18)
            
            # print(json.dumps(data, indent=2, ensure_ascii=False))

            # await postToESUnclassified(data)
            # await postToES(data)
            if len(search) == 0:
                await mongo_connection.disconnect()
                return
            
            data = flatten_post_list(search)
            await postToES(data)

        except Exception as e:
            log.error(e)
    return asyncio.run(do_crawl())




def flatten_post_data(raw: dict) -> dict:
    return {
        "id": raw.get("id", None),
        "doc_type": 1,  # POST = 1, COMMENT = 2
        "crawl_source": 2,
        "crawl_source_code": "tt",#channel.get("source_channel", None),
        "pub_time": Int64(int(raw.get("createTime", 0))),
        "crawl_time": int(datetime.now(VN_TZ).timestamp()),
        "org_id": 2,#channel.get("org_id", None),
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
        "source_type": 5,#channel.get("source_type", None),
        "source_name": "Đỗ Mỹ Linh",#channel.get("source_name", None),
        "source_url": "https://www.tiktok.com/@mylinhdo",#channel.get("source_url", None),
        "reply_to": None,
        "level": None,
        "sentiment": 0,
        "isPriority": True,
        "crawl_bot": "tiktok_post",
    }

def flatten_post_list(raw_list: list[dict]) -> list[dict]:
    return [flatten_post_data(item) for item in raw_list]