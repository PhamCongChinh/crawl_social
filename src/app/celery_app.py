from celery import Celery
from app.config.settings import settings
from celery.schedules import crontab
from kombu import Queue

celery_app = Celery(
    "worker",
    broker=settings.redis_broker_url,
    backend=settings.redis_backend_url
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Ho_Chi_Minh",
    enable_utc=False,
)

celery_app.autodiscover_tasks([
    "app.tasks.tiktok"
])

# Queue
celery_app.conf.task_queue = (
    Queue("tiktok_sources"),
    Queue("tiktok_posts"),
    Queue("tiktok_comments"),
)