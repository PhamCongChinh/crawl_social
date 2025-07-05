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

log = logging.getLogger(__name__)

from app.config import mongo_connection, postgres_connection

class ChannelService:

    @staticmethod
    async def get_posts_postgre():
        query = "SELECT * FROM public.tbl_posts WHERE crawl_source_code = 'tt' AND pub_time >= 1750698000"
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
        # await mongo_connection.connect()
        # videos = await mongo_connection.db["tiktok_channels"].find({
        #     "crawled": 0,  # Chưa được crawl
        # }).limit(limit).to_list(length=limit)
        # return videos
        return await ChannelModel.find(ChannelModel.crawled == 0).limit(limit).to_list()


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
    