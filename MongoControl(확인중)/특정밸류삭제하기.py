from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json
from datetime import datetime
import os
from bson import ObjectId

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'),
                    connectTimeoutMS=60000,
                    socketTimeoutMS=60000)

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")
    
    # 데이터베이스 선택
    db = client['insta09_database']
    
    # 사용 가능한 컬렉션 목록
    collections = [
        '01_main_newfeed_crawl_data',
        '02_main_influencer_data',
        '03_main_following_extract_data'
    ]
    
    # 컬렉션 목록 출력
    print("\n=== 사용 가능한 컬렉션 목록 ===")
    for idx, coll_name in enumerate(collections, 1):
        print(f"{idx}. {coll_name}")
    
    # 사용자 입력 받기
    while True:
        try:
            choice = int(input("\n컬렉션 번호를 선택하세요: "))
            if 1 <= choice <= len(collections):
                selected_collection = collections[choice-1]
                break
            else:
                print(f"1부터 {len(collections)} 사이의 숫자를 입력해주세요.")
        except ValueError:
            print("올바른 숫자를 입력해주세요.")
    
    # 선택된 컬렉션 사용
    collection = db[selected_collection]
    print(f"\n선택된 컬렉션: {selected_collection}")

    # author 필드가 비어있는 문서 찾기
    query = {"author": {"$exists": True, "$eq": ""}}
    
    # 업데이트할 필드들
    update_fields = {
        "09_feed": "",
        "09_brand": "",
        "09_item": "",
        "09_item_category": "",
        "09_item_category_2": "",
        "open_date": "",
        "end_date": "",
        "processed": False
    }
    
    # 업데이트 수행
    result = collection.update_many(query, {"$set": update_fields})
    
    print(f"\n업데이트 결과:")
    print(f"찾은 문서 수: {result.matched_count}")
    print(f"수정된 문서 수: {result.modified_count}")

except Exception as e:
    print(f"오류 발생: {e}")

finally:
    # MongoDB 연결 종료
    client.close()
    print("\nMongoDB 연결이 종료되었습니다.")
