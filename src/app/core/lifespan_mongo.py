from contextlib import asynccontextmanager
from app.config import mongo_connection

@asynccontextmanager
async def lifespan_mongo():
    await mongo_connection.connect()
    try:
        yield
    finally:
        await mongo_connection.disconnect()