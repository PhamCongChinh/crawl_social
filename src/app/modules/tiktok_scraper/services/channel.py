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

from app.config import mongo_connection

class ChannelService:

    @staticmethod
    async def get_channels():
        return await ChannelModel.find_all().to_list()

    
    @staticmethod
    async def get_channels_crawl():
        return await ChannelModel.find(ChannelModel.crawled == 0).to_list()
    
    @staticmethod
    async def delele_channel(id: str):
        await ChannelModel.find_one(ChannelModel.id == id).delete()

    @staticmethod
    async def channel_crawled(id: str):
        await ChannelModel.find_one(ChannelModel.id == id).update({"$set": {"crawled": 1}})


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

                update_doc = {
                    "$set": {
                        **channel,
                        "updated_at": now,
                    },
                    "$setOnInsert": {
                        "crawled": 0,
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

