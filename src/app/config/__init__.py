from app.config.db_mongo import MongoDB
from app.config.db_postgresql import PostgresDB

from app.modules.scheduler.model import JobModel
from app.modules.scheduler.models.jobs_log import JobLog
from app.modules.tiktok_scraper.models.source import SourceModel
from app.modules.tiktok_scraper.models.channel import ChannelModel
from app.modules.tiktok_scraper.models.post import PostModel

mongo_connection = MongoDB(
    document_models=[
        SourceModel,
        ChannelModel,
        PostModel,
        JobModel,
        JobLog
    ]
)

postgres_connection = PostgresDB()