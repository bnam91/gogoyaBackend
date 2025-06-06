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
    
    # TTL 인덱스 정보 확인
    indexes = collection.list_indexes()
    ttl_indexes = []
    
    print("\n=== TTL 인덱스 정보 ===")
    for index in indexes:
        if 'expireAfterSeconds' in index:
            ttl_indexes.append(index)
            print(f"인덱스 이름: {index['name']}")
            print(f"TTL 필드: {index['key']}")
            print(f"만료 시간: {index['expireAfterSeconds']}초 ({index['expireAfterSeconds']/86400:.1f}일)")
            print("---")
    
    if not ttl_indexes:
        print("TTL 인덱스가 설정되어 있지 않습니다.")
    else:
        # TTL 인덱스 수정 여부 확인
        modify = input("\nTTL 인덱스를 수정하시겠습니까? (y/n): ")
        if modify.lower() == 'y':
            for index in ttl_indexes:
                new_ttl = int(input(f"\n{index['name']} 인덱스의 새로운 TTL 시간(일)을 입력하세요: "))
                new_ttl_seconds = new_ttl * 86400  # 일을 초로 변환
                
                # 기존 인덱스 삭제
                collection.drop_index(index['name'])
                
                # 새로운 TTL 인덱스 생성
                collection.create_index(
                    list(index['key'].items())[0][0],
                    expireAfterSeconds=new_ttl_seconds
                )
                print(f"TTL 인덱스가 {new_ttl}일로 수정되었습니다.")

except Exception as e:
    print(f"오류 발생: {e}")

finally:
    # MongoDB 연결 종료
    client.close()
    print("\nMongoDB 연결이 종료되었습니다.")
