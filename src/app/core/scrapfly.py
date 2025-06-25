from scrapfly import ScrapflyClient
from app.config.settings import settings

SCRAPFLY = ScrapflyClient(settings.SCRAPFLY_API_KEY)

BASE_CONFIG = {
    "asp": True,
    "country": "US",
    "proxy_pool": "public_residential_pool"
}