import asyncio
from app.modules.tiktok_scraper.scrapers.channel import scrape_channel
from app.modules.tiktok_scraper.services.channel import ChannelService
from app.modules.tiktok_scraper.services.source import SourceService
from app.utils.delay import async_delay
from app.worker import celery_app


import logging

from app.utils.safe_scrape import safe_scrape
log = logging.getLogger(__name__)

from app.modules.tiktok_scraper.models.source import SourceModel
from app.config import mongo_connection

@celery_app.task(
    name="app.tasks.tiktok.channel.crawl_tiktok_channels",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3, "countdown": 10}
)
def crawl_tiktok_channels(job_id: str, channel_id: str):
    print(f"Task {job_id} - {channel_id}")
    async def do_crawl():
        try:
            await mongo_connection.connect()
            sources = await SourceService.get_sources()
            log.info(f"🚀 Đang cào {len(sources)}")
            for source in sources:
                log.info(f"🚀 Đang cào {source.source_url}")
                data = source.model_dump(by_alias=True)
                data["_id"] = str(data["_id"])
                crawl_tiktok_channel.delay(data)
        except Exception as e:
            log.error(e)
    return asyncio.run(do_crawl())

@celery_app.task(name="app.tasks.tiktok.channel.crawl_tiktok_channel")
def crawl_tiktok_channel(source: dict):
    async def do_crawl():
        try:
            await mongo_connection.connect()
            source_model = SourceModel(**source)

            # data = await safe_scrape(source_model.source_url)
            data = await scrape_channel(source_model.source_url)
            if not data:
                log.warning(f"⚠️ Không lấy được dữ liệu từ {source_model.source_url}")
                return {
                    "status": "error",
                    "url": source_model.source_url,
                    "message": "Không lấy được dữ liệu (data=None)",
                    "type": "NoData"
                }
            
            await async_delay(1, 3)

            log.info(f"Đang upsert {len(data)} channels vào cơ sở dữ liệu")

            result = await ChannelService.upsert_channels_bulk(data, source=source_model)
            log.info(
                f"Bulk upsert xong: matched={result.matched_count}, "
                f"inserted={result.upserted_count}, modified={result.modified_count}"
            )

            return {
                "status": "success",
                "url": source_model.source_url,
                "matched": result.matched_count,
                "inserted": result.upserted_count,
                "modified": result.modified_count,
                "total": len(data),
            }
        except Exception as e:
            log.error(f"❌ Lỗi khi crawl : {e}")
    return asyncio.run(do_crawl())

# def crawl_tiktok_channels(source: dict):
#     async def do_crawl():
#         try:
#             await mongo_connection.connect()

#             source_model = SourceModel(**source)
#             log.warning(f"🚀 Crawling source {source_model.source_url}")

#             # data = await safe_scrape(source_model.source_url)
#             # if not data:
#             #     log.warning(f"⚠️ Không lấy được dữ liệu từ {source_model.source_url}")
#             #     return {
#             #         "status": "error",
#             #         "url": source_model.source_url,
#             #         "message": "Không lấy được dữ liệu (data=None)",
#             #         "type": "NoData"
#             #     }

#             # await async_delay(1, 3)
#             # log.info(f"Đang upsert {len(data)} channels vào cơ sở dữ liệu")

#             # result = await ChannelService.upsert_channels_bulk(data, source=source_model)
#             # log.info(
#             #     f"Bulk upsert xong: matched={result.matched_count}, "
#             #     f"inserted={result.upserted_count}, modified={result.modified_count}"
#             # )

#             # return {
#             #     "status": "success",
#             #     "url": source_model.source_url,
#             #     "matched": result.matched_count,
#             #     "inserted": result.upserted_count,
#             #     "modified": result.modified_count,
#             #     "total": len(data),
#             # }

#         except Exception as e:
#             log.error(f"❌ Lỗi khi crawl {source.get('source_url')}: {e}")
#             return {
#                 "status": "error",
#                 "url": source.get("source_url", "unknown"),
#                 "message": str(e),
#                 "type": type(e).__name__,
#             }

#     return asyncio.run(do_crawl())