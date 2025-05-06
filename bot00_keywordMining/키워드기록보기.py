from pymongo import MongoClient
from pymongo.server_api import ServerApi
from collections import Counter

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")
    
    # 데이터베이스와 컬렉션 선택
    db = client['insta09_database']
    collection = db['02_main_influencer_data']
    
    # 모든 도큐먼트에서 tags 필드 추출
    all_tags = []
    for doc in collection.find({}, {"tags": 1}):
        if "tags" in doc and doc["tags"]:
            all_tags.extend(doc["tags"])
    
    # 태그 빈도수 계산
    tag_counter = Counter(all_tags)
    
    # 상위 30개 태그 출력
    print("\n상위 30개 태그와 빈도수:")
    for tag, count in tag_counter.most_common(50):
        print(f"{tag}: {count}회")

except Exception as e:
    print(f"에러 발생: {e}")
finally:
    client.close() 