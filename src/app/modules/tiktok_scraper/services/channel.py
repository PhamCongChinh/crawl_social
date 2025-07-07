from datetime import datetime, timedelta, timezone
from typing import List
from pymongo import UpdateOne
from app.modules.tiktok_scraper.models.channel import ChannelModel
from app.modules.tiktok_scraper.models.source import SourceModel

import logging

from app.modules.tiktok_scraper.scrapers.channel import scrape_channel
from app.modules.tiktok_scraper.services.source import SourceService
from app.utils.delay import async_delay
from app.utils.timezone import now_vn

from pymongo.errors import BulkWriteError
from beanie.operators import In, And

log = logging.getLogger(__name__)

from app.config import mongo_connection, postgres_connection
now = datetime.now(timezone(timedelta(hours=7)))
two_hours_ago = now - timedelta(hours=24)

class ChannelService:

    @staticmethod
    async def get_posts_postgre(start_time: int = 1751734800, end_time: int = 1751821200) -> List[dict]:
        query = f"SELECT * FROM public.tbl_posts WHERE crawl_source_code = 'tt' AND org_id IN (2, 6) AND pub_time >= {start_time} AND pub_time <= {end_time}"
        results = await postgres_connection.fetch_all(query)
        print(f"Fetched {len(results)} sources from PostgreSQL")
        return [dict(row) for row in results]  # optional: convert Record -> dict

    @staticmethod
    async def get_channels():
        return await ChannelModel.find_all().to_list()

    @staticmethod
    async def get_channels_crawl():
        return await ChannelModel.find(ChannelModel.crawled == 0).to_list()
    
    @staticmethod
    async def get_channels_crawl_comments():
        return await ChannelModel.find(ChannelModel.crawled == 1).to_list()
    
    @staticmethod
    async def get_channel_by_id(channel_id: any):
        return await ChannelModel.find_one(ChannelModel.desc == channel_id)

    @staticmethod
    async def delele_channel(id: str):
        await ChannelModel.find_one(ChannelModel.id == id).delete()

    @staticmethod
    async def channel_crawled(id: str):
        await ChannelModel.find_one(ChannelModel.id == id).update({"$set": {"crawled": 1}})

    @staticmethod
    async def channel_crawled_comments(id: str):
        await ChannelModel.find_one(ChannelModel.id == id).update({"$set": {"crawled": 2}})



    # Bắt đầu
    @staticmethod
    async def get_videos_to_crawl(limit=10):
        return await ChannelModel.find(ChannelModel.crawled == 0).limit(limit).to_list()




    @staticmethod
    async def get_channels_comments_hourly():
        """
        Lấy danh sách các channel đã crawl trong vòng 1 giờ qua
        """
        query = f"SELECT * FROM public.tbl_posts WHERE crawl_source_code = 'tt' AND org_id IN (2, 6) AND pub_time >= EXTRACT(EPOCH FROM (NOW() - INTERVAL '2 hours')) AND pub_time <= EXTRACT(EPOCH FROM NOW());"
        results = await postgres_connection.fetch_all(query)
        log.info(f"Đã lấy được {len(results)} bài viết từ PostgreSQL 2 giờ qua")
        return [dict(row) for row in results]  # optional: convert Record -> dict
    
    @staticmethod
    async def get_channels_posts_hourly():
        vietnam_tz = timezone(timedelta(hours=7))
        now_vietnam = datetime.now(vietnam_tz)
        timestamp = int(now_vietnam.timestamp())
        two_hours_ago = timestamp - 2 * 60 * 60

        return await ChannelModel.find(
            And(
                ChannelModel.status == "pending",
                ChannelModel.createTime >= two_hours_ago
            )
        ).to_list()
    
    @staticmethod
    async def upsert_channels_bulk(channels: list[dict], source: SourceModel):
        try:
            if not channels:
                log.warning("Không có dữ liệu để upsert (bulk)")
                return
            
            # now = datetime.now(timezone.utc)
            now = now_vn()
            operations = []

            for channel in channels:
                if not channel.get("id"):
                    log.warning("Channel không có ID, bỏ qua")
                    continue

                # _id = channel["id"]
                _id = f"{source.source_url}/video/{channel['id']}"
                channel["org_id"] = source.org_id
                channel["source_name"] = source.source_name
                channel["source_type"] = source.source_type
                channel["source_url"] = source.source_url
                channel["source_channel"] = source.source_channel
                channel["updated_at"] = now

                channel.pop("crawled", None)
                channel.pop("status", None)

                update_doc = {
                    "$set": {
                        **channel,
                        "updated_at": now,
                    },
                    "$setOnInsert": {
                        "crawled": 0,  # 0: chưa crawl, 1: đã crawl, 2: đã crawl comments
                        "status": "pending",  # 0: chưa crawl, 1: đã crawl, 2: đã crawl comments
                        "created_at": now
                    }
                }
                operations.append(
                    UpdateOne(
                        {"_id": _id},
                        update_doc,
                        upsert=True
                    )
                )
            if not operations:
                log.info("Không có operation nào được tạo cho bulk upsert.")
                return
            result = await ChannelModel.get_motor_collection().bulk_write(operations)
            # log.info(f"Bulk upsert xong: inserted={result.upserted_count}, modified={result.modified_count}")
            return result
        except Exception as e:
            log.error(e)
    
    @staticmethod
    async def crawl_one_channel(source: dict):
        await mongo_connection.connect()
        source_model = SourceModel(**source)
        data = await scrape_channel(source_model.source_url)
        if not data:
            log.warning(f"⚠️ Không có dữ liệu từ {source_model.source_url}")
            return {"status": "no_data", "url": source_model.source_url}
        
        result = await ChannelService.upsert_channels_bulk(data, source=source_model)
        
        return {
            "status": "success",
            "url": source_model.source_url,
            "total": len(data),
            "matched": result.matched_count,
            "inserted": result.upserted_count,
            "modified": result.modified_count
        }

# Mới
    