import logging

from fastapi import APIRouter, Body, Query


from app.tasks.crawl_data import crawl_data
from app.tasks.crawl_tiktok import crawl_tiktok_channels
from app.tasks.test_tasks import test_task
log = logging.getLogger(__name__)

# from pymongo import MongoClient
# from app.config.settings import settings
# client = MongoClient(settings.MONGO_URI)
# db = client[settings.MONGO_DB]

router = APIRouter()

@router.get("/test/crawl")
async def crawl():
    crawl_tiktok_channels.delay()
    # documents = list(db["tiktok_sources"].find())
    # for doc in documents:
    #     print(doc)


@router.post("/test-multiple-tasks")
async def test_many_tasks():
    for i in range(5):  # gửi 5 task
        test_task.delay(i)
    return {"status": "5 tasks submitted"}