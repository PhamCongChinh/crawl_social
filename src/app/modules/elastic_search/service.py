import httpx

URL_ETL_CLASSIFIED = 'http://103.97.125.64:8900/api/elastic/insert-posts'
URL_ETL_UNCLASSIFIED = 'http://103.97.125.64:8900/api/elastic/insert-unclassified-org-posts'
    
async def postToES(content: any) -> any:
    data = {
        "index": "facebook_raw_posts",
        "data": content,
        "upsert": True
    }
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(URL_ETL_CLASSIFIED, json=data)  # URL FastAPI endpoint của bạn
            response.raise_for_status()
            res_data = response.json()
            return res_data
    except httpx.HTTPStatusError as e:
        print(f"[ERROR] Insert failed: {e.response.status_code} - {e.response.text}")
        return {"successes": 0, "errors": [{"error": str(e)}]}
    except Exception as e:
        print(f"[ERROR] Insert exception: {e}")
        return {"successes": 0, "errors": [{"error": str(e)}]}
    
async def postToESUnclassified(content: any) -> any:
    data = {
        "index": "not_classify_org_posts",
        "data": content,
        "upsert": True
    }
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(URL_ETL_UNCLASSIFIED, json=data)  # URL FastAPI endpoint của bạn
            response.raise_for_status()
            res_data = response.json()
            return res_data
    except httpx.HTTPStatusError as e:
        print(f"[ERROR] Insert failed: {e.response.status_code} - {e.response.text}")
        return {"successes": 0, "errors": [{"error": str(e)}]}
    except Exception as e:
        print(f"[ERROR] Insert exception: {e}")
        return {"successes": 0, "errors": [{"error": str(e)}]}



# TEST
# URL_ETL_CLASSIFIED = 'http://host.docker.internal:4416/api/v1/posts/insert-posts'
# URL_ETL_UNCLASSIFIED = 'http://host.docker.internal:4416/api/v1/posts/insert-unclassified-org-posts'
# async def postToES(content: any) -> any:
#     try:
#         async with httpx.AsyncClient() as client:
#             response = await client.post(URL_ETL_CLASSIFIED, json=content)  # URL FastAPI endpoint của bạn
#             response.raise_for_status()
#             res_data = response.json()
#             print(f"✅ Status: {response.status_code}")
#             print(f"📦 Data trả về: {res_data}")
#             print(URL_ETL_CLASSIFIED)
#             return res_data
#     except httpx.HTTPStatusError as e:
#         print(f"[ERROR] Insert failed: {e.response.status_code} - {e.response.text}")
#         return {"successes": 0, "errors": [{"error": str(e)}]}
#     except Exception as e:
#         print(f"[ERROR] Insert exception: {e}")
#         return {"successes": 0, "errors": [{"error": str(e)}]}
    
# async def postToESUnclassified(content: any) -> any:
#     try:
#         async with httpx.AsyncClient() as client:
#             response = await client.post(URL_ETL_UNCLASSIFIED, json=content)  # URL FastAPI endpoint của bạn
#             response.raise_for_status()
#             res_data = response.json()
#             print(f"✅ Status: {response.status_code}")
#             print(f"📦 Data trả về: {res_data}")
#             print(URL_ETL_UNCLASSIFIED)
#             return res_data
#     except httpx.HTTPStatusError as e:
#         print(f"[ERROR] Insert failed: {e.response.status_code} - {e.response.text}")
#         return {"successes": 0, "errors": [{"error": str(e)}]}
#     except Exception as e:
#         print(f"[ERROR] Insert exception: {e}")
#         return {"successes": 0, "errors": [{"error": str(e)}]}