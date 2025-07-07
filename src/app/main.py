from fastapi import FastAPI
import uvicorn

# Logs
import logging
from app.config.logger import setup_logging
setup_logging()
log = logging.getLogger(__name__)
from app.config.logger import LoggingAPI

from app.core.lifespan import lifespan

from app.modules.scheduler.api import router as scheduler_router
from app.modules.tiktok_scraper.api import router as tiktok_router
from app.modules.thread_scraper.api import router as thread_router
from app.modules.web_scraper.api.routes import router as web_router


app = FastAPI(title="Social Media Crawler API", version="1.0.0", lifespan=lifespan)

app.add_middleware(LoggingAPI)

app.include_router(scheduler_router, prefix="/api/v1/jobs", tags=["jobs"])
app.include_router(tiktok_router, prefix="/api/v1/tiktok", tags=["tiktok"])
app.include_router(thread_router, prefix="/api/v1/thread", tags=["thread"])  
app.include_router(web_router, prefix="/api/v1/web", tags=["web"])  

def main():
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)

if __name__ == "__main__":
    main()