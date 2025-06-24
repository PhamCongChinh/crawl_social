from app.worker import celery_app

@celery_app.task(name="tasks.crawl_data")
def crawl_data(url):
    print(f"🕷️ Crawling {url}...")
    return f"Crawled {url}"