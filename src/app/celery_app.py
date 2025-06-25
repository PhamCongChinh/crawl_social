from celery import Celery
from app.config.settings import settings

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