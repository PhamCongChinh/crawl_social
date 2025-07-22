import asyncio
from typing import List
from fastapi import APIRouter, HTTPException

import logging

from app.modules.tiktok_scraper.models.keyword import KeywordModel
from app.modules.tiktok_scraper.services.search import SearchService

log = logging.getLogger(__name__)

router = APIRouter()

@router.post("/search")
async def create_or_update_keyword(request: KeywordModel):
    data = request.model_dump(exclude_unset=True)
    status = await SearchService.upsert_keyword(data)
    return {"status": status}

@router.get("/search")
async def get_keywords():
    keywords = await SearchService.get_keywords()
    return {"status": "success", "data": keywords}
