from motor.motor_asyncio import AsyncIOMotorClient
from typing import List, Dict

from pymongo import MongoClient
from app.config.settings import settings

import logging
log = logging.getLogger(__name__)

class MongoService:
    def __init__(self, uri=settings.MONGO_URI, db_name=settings.MONGO_DB):
        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def get_sources(self):
        return list(self.db["tiktok_sources"].find())