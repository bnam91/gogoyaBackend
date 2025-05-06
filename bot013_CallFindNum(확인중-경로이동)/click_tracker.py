"""
이 모듈은 판매자 상세정보 버튼 클릭을 추적하여 MongoDB에 저장하는 기능을 제공합니다.

주요 기능:
1. track_seller_button_click: 판매자 상세정보 버튼 클릭 시간과 URL을 MongoDB에 저장합니다.
"""

import logging
from datetime import datetime
from pymongo import MongoClient
from pymongo.server_api import ServerApi

# MongoDB 연결 정보
MONGO_URI = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

def get_db_connection():
    """MongoDB 연결 객체 반환"""
    try:
        client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
        # 연결 확인
        client.admin.command('ping')
        return client
    except Exception as e:
        logger = logging.getLogger()
        logger.error(f"MongoDB 연결 실패: {e}")
        return None

def track_seller_button_click(url):
    """판매자 상세정보 버튼 클릭 추적"""
    logger = logging.getLogger()
    
    # 현재 날짜 및 시간 가져오기
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M:%S")
    
    # 클릭 정보
    click_info = {
        "time": time_str,
        "link": url
    }
    
    try:
        # MongoDB 연결
        client = get_db_connection()
        if not client:
            logger.error("MongoDB 연결 실패로 클릭 추적을 저장할 수 없습니다.")
            return False
        
        # 데이터베이스 및 컬렉션 선택
        db = client['insta09_database']
        collection = db['seller_btClick_count']
        
        # 날짜별 문서 찾기
        query = {date_str: {"$exists": True}}
        doc = collection.find_one(query)
        
        if doc:
            # 해당 날짜 문서가 존재하면 클릭 추가
            update = {"$push": {f"{date_str}.clicks": click_info}}
            result = collection.update_one(query, update)
            if result.modified_count > 0:
                logger.info(f"판매자 상세정보 버튼 클릭 기록이 추가되었습니다. (날짜: {date_str}, 시간: {time_str}, URL: {url})")
        else:
            # 해당 날짜 문서가 없으면 새로 생성
            new_doc = {
                date_str: {
                    "clicks": [click_info]
                }
            }
            result = collection.insert_one(new_doc)
            if result.inserted_id:
                logger.info(f"판매자 상세정보 버튼 클릭 기록이 새로 생성되었습니다. (날짜: {date_str}, 시간: {time_str}, URL: {url})")
        
        # 연결 종료
        client.close()
        return True
        
    except Exception as e:
        logger.error(f"클릭 추적 저장 중 오류 발생: {e}")
        return False 