from app.modules.tiktok_scraper.models.keyword import KeywordModel


class SearchService():
    # V1
    @staticmethod
    async def get_keywords():
        return await KeywordModel.find_all().to_list()
    
    @staticmethod
    async def upsert_keyword(data: dict) -> KeywordModel:
        existing = await KeywordModel.find_one(KeywordModel.keyword == data["keyword"])
        if existing:
            await existing.set(data)
            return "updated"
        else:
            await KeywordModel(**data).insert()
            return "inserted"