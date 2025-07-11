import asyncio
from datetime import datetime
import json
import logging
from pathlib import Path
from zoneinfo import ZoneInfo

from bson import Int64

from app.modules.elastic_search.service import postToES, postToESUnclassified
from app.modules.tiktok_scraper.models.channel import ChannelModel
from app.modules.tiktok_scraper.scrapers.search import scrape_search
from app.modules.tiktok_scraper.services.channel import ChannelService
from app.modules.tiktok_scraper.services.search import SearchService
from app.utils.delay import async_delay
log = logging.getLogger(__name__)

from app.config import mongo_connection
from app.worker import celery_app

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

output = Path(__file__).parent / "results"
output.mkdir(exist_ok=True)

@celery_app.task(
    name="app.tasks.tiktok.post.crawl_tiktok_search_hourly",
)
def crawl_tiktok_search(job_name: str, crawl_type: str):
    print(f"Task search {job_name} - {crawl_type}")
    async def do_crawl():
        try:
            await mongo_connection.connect()
            keywords = await SearchService.get_keywords()
            if len(keywords) == 0:
                await mongo_connection.disconnect()
                return
            BATCH_SIZE = 5
            keyword_dicts = [k.model_dump() for k in keywords]
            batches = [keyword_dicts[i:i + BATCH_SIZE] for i in range(0, len(keyword_dicts), BATCH_SIZE)]
            log.info(f"📦 Tổng cộng {len(keywords)} từ khóa, chia thành {len(batches)} batch (mỗi batch {BATCH_SIZE} từ khóa)")
            for i, batch in enumerate(batches, start=1):
                log.info(f"🚀 Batch {i}/{len(batches)}: {len(batch)} từ khóa")
                await _crawl_batch_async(batch, i, len(batches))
                await async_delay(2,10)
        except Exception as e:
            log.error(e)
    return asyncio.run(do_crawl())


async def _crawl_batch_async(keywords: list[dict], batch_index: int, total_batches: int):
    log.info(f"🔧 Bắt đầu xử lý batch {batch_index}/{total_batches} với {len(keywords)} từ khóa")
    for index, keyword in enumerate(keywords):
        log.info(f"🕐 [{index + 1}/{len(keywords)}] Từ khóa: {keyword['keyword']}")
        scrape_data = await scrape_search(keyword=keyword["keyword"], max_search=18)
        with open(output.joinpath("search.json"), "w", encoding="utf-8") as file:
            json.dump(scrape_data, file, indent=2, ensure_ascii=False)
        if not scrape_data:
            log.warning(f"⚠️ Không lấy được dữ liệu từ {keyword['keyword']}")
            return
        # result = await ChannelService.upsert_channels_bulk_keyword(scrape_data, keyword)
        # log.info(
        #     f"✅ Upsert xong: matched={result.matched_count}, "
        #     f"inserted={result.upserted_count}, modified={result.modified_count}"
        # )

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