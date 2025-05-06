import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

def setup_environment():
    """환경 설정 함수"""
    # 현재 파일(config.py)의 디렉토리 경로 가져오기
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 상위 디렉토리 경로 계산
    parent_dir = os.path.dirname(current_dir)
    # 상위 디렉토리의 .env 파일 경로
    dotenv_path = os.path.join(parent_dir, '.env')
    
    # 지정된 경로의 .env 파일 로드
    load_dotenv(dotenv_path)
    
    # OpenAI API 키 설정 (.env 파일에서 가져오기)
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

    # MongoDB 연결 설정
    uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi('1'))

    # 데이터베이스 및 컬렉션 선택
    db = client['insta09_database']
    read_collection = db['08_main_brand_category_data']  # 읽기용 컬렉션
    
    # 쓰기용 컬렉션이 존재하지 않는 경우 명시적으로 생성
    if '08_main_brand_category_data' not in db.list_collection_names():
        db.create_collection('08_main_brand_category_data')
        print("새 컬렉션 '08_main_brand_category_data'를 생성했습니다.")
    
    write_collection = db['gogoya_vendor_brand_info']  # 쓰기용 컬렉션
    #gogoya_vendor_brand_info
    
    # 디버깅용 정보 출력
    print(f"읽기 컬렉션: {read_collection.name}, 문서 수: {read_collection.count_documents({})}")
    print(f"쓰기 컬렉션: {write_collection.name}, 문서 수: {write_collection.count_documents({})}")
    
    return OPENAI_API_KEY, client, db, read_collection, write_collection