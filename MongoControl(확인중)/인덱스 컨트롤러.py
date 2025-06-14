'''
이 데이터는 2026년 7월 1일 경에 자동으로 삭제될 것

_id : 67f36f97988ee1ba33600d78
cr_at : "2025-04-07T00:30:00.000Z"
author : "mommyland_kr"
content : "#구매인증여기에..."

--25년 6월 14일에 
'''

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

    # 인덱스 정보 조회
    indexes = collection.list_indexes()
    print("\n=== 인덱스 정보 ===")
    for index in indexes:
        print(f"인덱스 이름: {index['name']}")
        print(f"키: {index['key']}")
        if 'unique' in index:
            print(f"유니크: {index['unique']}")
        print("---")

except Exception as e:
    print(f"오류 발생: {e}")

finally:
    # MongoDB 연결 종료
    client.close()
    print("\nMongoDB 연결이 종료되었습니다.")
