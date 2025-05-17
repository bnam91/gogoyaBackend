from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime

# MongoDB 연결
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")
    
    # 데이터베이스 선택
    db = client['insta09_database']
    
    # 컬렉션 선택
    collection = db['gogoya_vendor_etc']
    
    # 현재 날짜 가져오기
    current_date = datetime.now().strftime("%Y-%m-%d")
    
    # 삽입할 데이터
    data = {
        "last_id": 7285,
        "last_id_date": current_date
    }
    
    # 데이터 삽입
    result = collection.insert_one(data)
    print(f"문서가 성공적으로 삽입되었습니다. ID: {result.inserted_id}")
    
except Exception as e:
    print(f"오류 발생: {e}")

finally:
    # MongoDB 연결 종료
    client.close()
    print("MongoDB 연결 종료") 