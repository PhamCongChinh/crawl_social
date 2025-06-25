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


class ChannelService:

    @staticmethod
    async def get_all_channels():
        return await ChannelModel.find_all().to_list()

    @staticmethod
    async def get_channels():
        # return await ChannelModel.find_all().to_list()
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
    
    # @staticmethod
    # async def crawl_channels():
    #     sources = await SourceService.get_sources()
    #     for source in sources:
    #         try:
    #             log.info(f"Đang crawl channels cho {source.source_name} từ {source.source_url}")
    #             data = await scrape_channel(url=source.source_url)
    #             log.info(f"Đang upsert {len(data)} channels vào cơ sở dữ liệu")
    #             await async_delay(1,3)
    #             result = await ChannelService.upsert_channels_bulk(data, source=source)
    #             log.info(f"Bulk upsert xong: inserted={result.upserted_count}, modified={result.modified_count}")
    #         except Exception as e:
    #             log.error(f"{e}")
    #             continue
            
    #         await async_delay(1,3)
    #         break
       
    #     return {"message": "Crawl channels completed", "count": len(sources)}
    
    
    def chunked(data:List[dict], size: int):
        for i in range(0, len(data), size):
            yield data[i:i + size]
