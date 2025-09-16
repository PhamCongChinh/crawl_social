from beanie import SortDirection
from app.modules.tiktok_scraper.models.keyword import KeywordModel
from beanie.operators import In

class SearchService():
    # V1
    @staticmethod
    async def get_keywords():
        # return await KeywordModel.find_all().to_list()
        return await KeywordModel.find(
            In(KeywordModel.org_id, [2, 675983, 412592])
        ).sort(
            ("org_id", SortDirection.DESCENDING)
        ).to_list()
    
    @staticmethod
    async def upsert_keyword(data: dict) -> KeywordModel:
        existing = await KeywordModel.find_one(KeywordModel.keyword == data["keyword"])
        if existing:
            await existing.set(data)
            return "updated"
        else:
            await KeywordModel(**data).insert()
            return "inserted"