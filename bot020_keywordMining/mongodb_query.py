from pymongo import MongoClient
from pymongo.server_api import ServerApi

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")
    
    # 데이터베이스와 컬렉션 선택
    db = client['insta09_database']
    collection = db['01_main_newfeed_crawl_data']
    
    # author가 'jiemma_official'인 가장 최근 도큐먼트 조회
    latest_doc = collection.find_one(
        {"author": "jiemma_official"},
        sort=[("_id", -1)]  # _id 기준 내림차순 정렬 (최신순)
    )
    
    if latest_doc:
        print("\n가장 최근 content:")
        print(latest_doc.get('content', 'content 필드가 없습니다.'))
    else:
        print("해당 author의 도큐먼트를 찾을 수 없습니다.")

except Exception as e:
    print(f"에러 발생: {e}")
finally:
    client.close() 