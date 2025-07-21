from app.config.db_mongo import MongoDB
from app.config.db_postgresql import PostgresDB

from app.modules.tiktok_scraper.models.video import VideoModel
from app.modules.tiktok_scraper.models.keyword import KeywordModel
from app.scheduler.model import JobModel
from app.scheduler.models.jobs_log import JobLog
from app.modules.tiktok_scraper.models.source import SourceModel
from app.modules.tiktok_scraper.models.post import PostModel

from app.config.constants import Constant

mongo_connection = MongoDB(
    document_models=[
        SourceModel,
        KeywordModel,
        VideoModel,
        PostModel,
        JobModel,
        JobLog
    ]
)

postgres_connection = PostgresDB()
constant = Constant()