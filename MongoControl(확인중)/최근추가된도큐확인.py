from pymongo import MongoClient
from pymongo.server_api import ServerApi

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'),
                    connectTimeoutMS=60000,
                    socketTimeoutMS=60000)

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")
    
    # 데이터베이스와 컬렉션 선택
    db = client['insta09_database']
    collection = db['01_main_newfeed_crawl_data']
    
    # 가장 최근에 추가된 문서 10개 가져오기
    recent_docs = list(collection.find().sort([('_id', -1)]).limit(10))
    
    print("\n=== 최근 추가된 10개 문서 ===")
    for i, doc in enumerate(recent_docs, 1):
        print(f"\n{i}번째 문서:")
        # _id 필드는 제외하고 출력
        for key, value in doc.items():
            if key != '_id':
                # 값이 문자열이고 100자 이상인 경우 축약
                if isinstance(value, str) and len(value) > 100:
                    print(f"{key}: {value[:100]}...")
                else:
                    print(f"{key}: {value}")

except Exception as e:
    print(f"오류 발생: {e}")

finally:
    # MongoDB 연결 종료
    client.close()
    print("\nMongoDB 연결이 종료되었습니다.")
