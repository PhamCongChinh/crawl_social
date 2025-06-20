from fastapi import APIRouter

router = APIRouter()

@router.get("/ping")
async def ping_web():
    return {"module": "crawl_web", "msg": "pong"}