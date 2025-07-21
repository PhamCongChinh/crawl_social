import asyncio
from cmath import log
from bson import ObjectId
from pymongo import UpdateOne
from app.modules.tiktok_scraper.models.source import SourceModel
from app.config import postgres_connection
from app.utils.timezone import now_vn

class SourceService:

    @staticmethod
    async def get_sources():
        return await SourceModel.find(SourceModel.org_id != 0).to_list()

    @staticmethod
    async def get_source_by_id(source_url: any):
        return await SourceModel.find(SourceModel.source_url == source_url).to_list()

    @staticmethod
    async def upsert_source(data: dict) -> SourceModel:
        existing = await SourceModel.find_one(SourceModel.source_url == data["source_url"])
        if existing:
            await existing.set(data)  # cập nhật toàn bộ fields
            return "updated"
        else:
            await SourceModel(**data).insert()
            return "inserted"
        
    @staticmethod
    async def get_sources_classified():
        return await SourceModel.find(SourceModel.org_id != 0).to_list()
    
    @staticmethod
    async def get_sources_unclassified():
        return await SourceModel.find(SourceModel.org_id == 0).to_list()

    @staticmethod
    async def upsert_source_batch(sources: list[dict]) -> int:
        now = now_vn()
        operations = []
        for src in sources:
            source_url = src.get("source_url")
            if not source_url:
                log.warning("❌ Thiếu source_url, bỏ qua.")
                continue
            src["updated_at"] = now
            operations.append(
                UpdateOne(
                    {"source_url": source_url},  # filter theo source_url
                    {
                        "$set": src,
                        "$setOnInsert": {
                            "created_at": now,
                        }
                    },
                    upsert=True
                )
            )
        
        if not operations:
            return 0

        result = await SourceModel.get_motor_collection().bulk_write(operations)
        return result.upserted_count + result.modified_count
    
    # @staticmethod
    # async def get_sources_postgre():
    #     query = "SELECT * FROM public.tbl_posts WHERE crawl_source_code = 'tt' AND pub_time >= 1743440400"
    #     results = await postgres_connection.fetch_all(query)
    #     print(f"Fetched {len(results)} sources from PostgreSQL")
    #     return [dict(row) for row in results]  # optional: convert Record -> dict