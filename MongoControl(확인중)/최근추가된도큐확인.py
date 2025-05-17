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
    
    # 사용자로부터 출력할 문서 개수 입력 받기
    while True:
        try:
            doc_count = int(input("\n출력할 문서 개수를 입력하세요: "))
            if doc_count > 0:
                break
            else:
                print("1 이상의 숫자를 입력해주세요.")
        except ValueError:
            print("올바른 숫자를 입력해주세요.")
    
    # 입력받은 개수만큼 최근 문서 가져오기
    recent_docs = list(collection.find().sort([('_id', -1)]).limit(doc_count))
    
    print(f"\n=== 최근 추가된 {doc_count}개 문서 ===")
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
