from bson import ObjectId
from app.modules.tiktok_scraper.models.source import SourceModel
# from app.config import postgres_connection

class SourceService:
    @staticmethod
    async def get_sources():
        return await SourceModel.find_all().to_list()
        # sources = await postgres_connection.fetch_all(
        #     "SELECT * FROM dim_sources WHERE source_channel = 'tt'"
        # )
        # return sources

    async def get_source_by_id(source_id: any):
        return await SourceModel.get(ObjectId(source_id))
    

    async def upsert_source(data: dict) -> SourceModel:
        existing = await SourceModel.find_one(SourceModel.source_url == data["source_url"])
        if existing:
            await existing.set(data)  # cập nhật toàn bộ fields
            return "updated"
        else:
            await SourceModel(**data).insert()
            return "inserted"