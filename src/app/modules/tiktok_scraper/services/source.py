import asyncio
from cmath import log
from bson import ObjectId
from pymongo import UpdateOne
from app.modules.tiktok_scraper.models.source import SourceModel
from app.config import postgres_connection
from app.utils.timezone import now_vn

class SourceService:

    # V1
    @staticmethod
    async def get_sources_classified():
        return await SourceModel.find(SourceModel.org_id != 0).to_list()
    
    # V1
    @staticmethod
    async def get_sources_unclassified():
        return await SourceModel.find(SourceModel.org_id == 0).to_list()
    
    # V1
    @staticmethod
    async def get_sources():
        return await SourceModel.find_all().limit(20).to_list()

    # v1
    @staticmethod
    async def upsert_source(data: dict) -> SourceModel:
        existing = await SourceModel.find_one(SourceModel.source_url == data["source_url"])
        if existing:
            await existing.set(data)  # cập nhật toàn bộ fields
            return "updated"
        else:
            await SourceModel(**data).insert()
            return "inserted"

    # v1
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