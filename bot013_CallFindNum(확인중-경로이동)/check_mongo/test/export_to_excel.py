import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

def setup_environment():
    """환경 설정 함수"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    dotenv_path = os.path.join(parent_dir, '.env')
    load_dotenv(dotenv_path)

    # MongoDB 연결 설정
    uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client['insta09_database']
    collection = db['gogoya_vendor_brand_info']
    
    return collection

def export_to_excel():
    """MongoDB 데이터를 엑셀로 내보내기"""
    collection = setup_environment()
    
    # 모든 문서의 모든 필드를 가져오기
    cursor = collection.find({})
    
    # 데이터를 리스트로 변환
    data = list(cursor)
    
    # DataFrame 생성
    df = pd.DataFrame(data)
    
    # _id 필드 제거 (MongoDB의 ObjectId는 엑셀에서 처리하기 어려울 수 있음)
    if '_id' in df.columns:
        df = df.drop('_id', axis=1)
    
    # 현재 시간을 파일명에 포함
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_filename = f'brand_phone_data_{current_time}.xlsx'
    
    # 엑셀 파일로 저장
    df.to_excel(excel_filename, index=False)
    print(f"데이터가 {excel_filename}에 저장되었습니다.")
    print(f"총 {len(data)}개의 문서가 저장되었습니다.")
    print(f"저장된 필드: {', '.join(df.columns)}")

if __name__ == "__main__":
    export_to_excel() 