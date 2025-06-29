import logging
log = logging.getLogger(__name__)

from app.worker import celery_app

@celery_app.task(
    name="app.tasks.tiktok.post.crawl_tiktok_comments",
    bind=True
)
def crawl_tiktok_comments(self, job_id: str, channel_id: str):
    print(f"Task {job_id} - {channel_id}")