from typing import List, Type

from beanie import Document, init_beanie
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ServerSelectionTimeoutError

from app.config.settings import settings

import logging
log = logging.getLogger(__name__)

class MongoDB:
    def __init__(self, document_models: List[Type[Document]]):
        self.db = None
        self.client = None
        self.mongo_uri = settings.MONGO_URI
        self.mongo_db_name = settings.MONGO_DB
        self.document_models = document_models

    async def connect(self):
        self.client = AsyncIOMotorClient(self.mongo_uri)
        self.db = self.client[self.mongo_db_name]
        try:
            await init_beanie(
                database=self.db,
                document_models=self.document_models
            )
            await self.ping()
            log.info("MongoDB (Beanie) đã kết nối")
        except Exception as e:
            log.error(f"Lỗi khi khởi tạo Beanie: {e}")
            raise e
    
    async def ping(self) -> bool:
        if not self.client:
            return False
        try:
            await self.client.admin.command("ping")
            return True
        except ServerSelectionTimeoutError:
            return False

    async def disconnect(self):
        if self.client:
            self.client.close()
            log.info("Đã ngắt kết nối MongoDB")