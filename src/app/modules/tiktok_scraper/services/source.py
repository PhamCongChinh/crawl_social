from bson import ObjectId
from app.modules.tiktok_scraper.models.source import SourceModel
from app.config import postgres_connection


class SourceService:

    @staticmethod
    async def get_sources_postgre():
        query = "SELECT * FROM public.tbl_posts WHERE crawl_source_code = 'tt' AND pub_time >= 1743440400"
        results = await postgres_connection.fetch_all(query)
        print(f"Fetched {len(results)} sources from PostgreSQL")
        return [dict(row) for row in results]  # optional: convert Record -> dict

    @staticmethod
    async def get_sources():
        return await SourceModel.find().sort("-_id").to_list()

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