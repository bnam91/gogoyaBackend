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
    
    # URL에서 마지막 슬래시(/) 뒤에 추가 문자가 있는 경우 찾기
    random_posts = list(collection.aggregate([
        {"$match": {"post_url": {"$regex": "/p/[A-Za-z0-9]+/[^/]+$"}}},
        {"$project": {"post_url": 1, "_id": 0}},
        {"$sample": {"size": 10}}
    ]))
    
    print("\n=== URL에 추가 파라미터가 있는 게시물 중 랜덤 10개 ===")
    for i, post in enumerate(random_posts, 1):
        print(f"{i}. {post['post_url']}")
    
    # 전체 URL 중 파라미터가 있는 URL 개수 확인
    total_with_param = collection.count_documents({"post_url": {"$regex": "/p/[A-Za-z0-9]+/[^/]+$"}})
    print(f"\n전체 URL 중 추가 파라미터가 있는 URL 개수: {total_with_param:,}개")

except Exception as e:
    print(f"오류 발생: {e}")

finally:
    # MongoDB 연결 종료
    client.close()
    print("\nMongoDB 연결이 종료되었습니다.")
