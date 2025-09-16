import httpx

from app.helpers.telegram import Telegram

# URL_ETL_CLASSIFIED = 'http://103.97.125.64:8900/api/elastic/insert-posts'
# URL_ETL_UNCLASSIFIED = 'http://103.97.125.64:8900/api/elastic/insert-unclassified-org-posts'

URL_KAFKA_CLASSIFIED = 'http://192.168.1.28:4416/api/v1/posts/insert-posts'
URL_KAFKA_UNCLASSIFIED = 'http://192.168.1.28:4416/api/v1/posts/insert-unclassified-org-posts'

URL_KAFKA_CLASSIFIED_TEST = 'http://192.168.1.28:4420/api/v1/posts/insert-posts'
URL_KAFKA_UNCLASSIFIED_TEST = 'http://192.168.1.28:4420/api/v1/posts/insert-unclassified-org-posts'
    
async def postToESClassified(content: any) -> any:
    Telegram.send_alert(f"[CLASSIFIED]Đã đẩy {len(content)} bài viết")
    # data = {
    #     "index": "facebook_raw_posts",
    #     "data": content,
    #     "upsert": True
    # }
    data_tandt = {
        "index": "facebook_raw_posts",
        "data": [item for item in content if item["org_id"] == 2],
        "upsert": True
    }

    data_test_hanoi = {
        "index": "facebook_raw_posts",
        "data": [item for item in content if item["org_id"] == 412592],
        "upsert": True
    }

    data_test_kol = {
        "index": "facebook_raw_posts",
        "data": [item for item in content if item["org_id"] == 675983],
        "upsert": True
    }

    try:
        # To Kafka
        async with httpx.AsyncClient() as client:
            Telegram.send_alert(f"[Kafka Classified T&T]Đã đẩy {len(content)} bài viết lên Kafka")
            response = await client.post(URL_KAFKA_CLASSIFIED, json=data_tandt)

        # To Kafka Test
        async with httpx.AsyncClient() as client:
            Telegram.send_alert(f"[Kafka Classified HaNoi]Đã đẩy {len(content)} bài viết lên Kafka")
            response = await client.post(URL_KAFKA_CLASSIFIED_TEST, json=data_test_hanoi)

        # To KOL
        async with httpx.AsyncClient() as client:
            Telegram.send_alert(f"[Kafka Classified KOL]Đã đẩy {len(content)} bài viết lên Kafka")
            response = await client.post(URL_KAFKA_CLASSIFIED_TEST, json=data_test_kol)

        # # To ELK
        # async with httpx.AsyncClient() as client:
        #     response = await client.post(URL_ETL_CLASSIFIED, json=data)  # URL FastAPI endpoint của bạn
        #     response.raise_for_status()
        #     res_data = response.json()
        #     return res_data
    except httpx.HTTPStatusError as e:
        print(f"[ERROR] Insert failed: {e.response.status_code} - {e.response.text}")
        return {"successes": 0, "errors": [{"error": str(e)}]}
    except Exception as e:
        print(f"[ERROR] Insert exception: {e}")
        return {"successes": 0, "errors": [{"error": str(e)}]}



async def postToESUnclassified(content: any) -> any:
    Telegram.send_alert(f"[UNCLASSIFIED]Đã đẩy {len(content)} bài viết")
    data = {
        "index": "not_classify_org_posts",
        "data": content,
        "upsert": True
    }
    
    try:
        # To Kafka
        async with httpx.AsyncClient() as client:
            Telegram.send_alert(f"[Kafka Unclassified]Đã đẩy {len(content)} bài viết lên Kafka")
            response = await client.post(URL_KAFKA_UNCLASSIFIED, json=data)
        
        # # To ELK
        # async with httpx.AsyncClient() as client:
        #     response = await client.post(URL_ETL_UNCLASSIFIED, json=data)  # URL FastAPI endpoint của bạn
        #     response.raise_for_status()
        #     res_data = response.json()
        #     return res_data
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