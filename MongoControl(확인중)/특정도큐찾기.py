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
    
    # 검색할 키와 값 입력 받기
    search_key = input("\n검색할 키를 입력하세요: ")
    search_value = input("검색할 값을 입력하세요: ")
    
    # 검색 쿼리 생성
    query = {search_key: search_value}
    
    # 검색 결과 가져오기
    search_results = list(collection.find(query))
    
    if not search_results:
        print(f"\n'{search_key}: {search_value}'에 해당하는 문서를 찾을 수 없습니다.")
    else:
        print(f"\n=== 검색 결과 ({len(search_results)}개 문서 발견) ===")
        for i, doc in enumerate(search_results, 1):
            print(f"\n{i}번째 문서:")
            # _id 필드는 제외하고 출력
            for key, value in doc.items():
                if key != '_id':
                    # 값이 문자열이고 100자 이상인 경우 축약
                    if isinstance(value, str) and len(value) > 100:
                        print(f"{key}: {value[:100]}...")
                    else:
                        print(f"{key}: {value}")
        
        # ObjectId를 문자열로 변환
        serializable_results = []
        for doc in search_results:
            serializable_doc = {}
            for key, value in doc.items():
                if isinstance(value, ObjectId):
                    serializable_doc[key] = str(value)
                elif isinstance(value, datetime):
                    serializable_doc[key] = value.isoformat()
                else:
                    serializable_doc[key] = value
            serializable_results.append(serializable_doc)
        
        # 현재 시간을 파일명에 포함
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"search_results_{selected_collection}_{timestamp}.json"
        
        # JSON 파일로 저장
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(serializable_results, f, ensure_ascii=False, indent=2)
        
        print(f"\n검색 결과가 '{filename}' 파일로 저장되었습니다.")

except Exception as e:
    print(f"오류 발생: {e}")

finally:
    # MongoDB 연결 종료
    client.close()
    print("\nMongoDB 연결이 종료되었습니다.")
