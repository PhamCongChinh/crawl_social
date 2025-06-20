import httpx
import requests

ROOT_URL_TEMP = 'http://103.97.125.64:8900/api/elastic/insert-posts'

async def postToES(content: list):
    data = {
        "index": "facebook_raw_posts",
        "data": content,
        "upsert": True
    }
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(ROOT_URL_TEMP, json=data)  # URL FastAPI endpoint của bạn
            response.raise_for_status()
            res_data = response.json()
            return res_data
    except httpx.HTTPStatusError as e:
        print(f"[ERROR] Insert failed: {e.response.status_code} - {e.response.text}")
        return {"successes": 0, "errors": [{"error": str(e)}]}
    except Exception as e:
        print(f"[ERROR] Insert exception: {e}")
        return {"successes": 0, "errors": [{"error": str(e)}]}