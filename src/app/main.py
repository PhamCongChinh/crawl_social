from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
import uvicorn

# Logs
import logging
from app.config.logger import setup_logging
setup_logging()
log = logging.getLogger(__name__)
from app.config.logger import LoggingAPI

from app.core.lifespan import lifespan

from app.scheduler.api import router as scheduler_router
from app.modules.tiktok_scraper.api import router as tiktok_router
from app.modules.thread_scraper.api import router as thread_router
from app.modules.instagram_scraper.api import router as instagram_router
from app.modules.google_scraper.api import router as google_router


app = FastAPI(title="Social Media Crawler API", version="1.0.0", lifespan=lifespan)

app.add_middleware(LoggingAPI)

app.include_router(scheduler_router, prefix="/api/v1/jobs", tags=["jobs"])
app.include_router(tiktok_router, prefix="/api/v1/tiktok", tags=["tiktok"])
app.include_router(thread_router, prefix="/api/v1/thread", tags=["thread"])
app.include_router(instagram_router, prefix="/api/v1/instagram", tags=["instagram"])  
app.include_router(google_router, prefix="/api/v1/google", tags=["google"])  

def main():
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)

if __name__ == "__main__":
    main()