from pymongo import MongoClient
from pymongo.server_api import ServerApi
import re
import pandas as pd
from datetime import datetime
import os

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
    collection = db['02_main_influencer_data']
    
    # 조건에 맞는 인플루언서 검색
    pipeline = [
        {
            "$match": {
                "category": {"$regex": "홈/리빙\\([3-9][0-9]%|100%\\)"},  # 홈/리빙이 30% 이상인 경우
                "is_contact_excluded": {"$ne": True},  # is_contact_excluded가 True가 아닌 경우
                "reels_views(15)": {
                    "$gt": 5000,  # 5000보다 큰 경우
                    "$lte": 35000  # 3.5만 이하
                }
            }
        },
        {
            "$project": {
                "clean_name": 1,
                "username": 1,
                "reels_views(15)": 1,
                "profile_link": 1,
                "_id": 0
            }
        },
        {
            "$sort": {
                "reels_views(15)": -1  # 내림차순 정렬
            }
        }
    ]
    
    result = list(collection.aggregate(pipeline))
    
    if result:
        # DataFrame 생성
        df = pd.DataFrame(result)
        
        # 칼럼 순서 지정
        columns_order = ['profile_link', 'clean_name', 'username', 'reels_views(15)']
        df = df[columns_order]
        
        # 현재 날짜 가져오기
        today = datetime.now().strftime('%Y%m%d')
        
        # 현재 스크립트의 디렉토리 경로 가져오기
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 엑셀 파일로 저장
        excel_filename = os.path.join(current_dir, f'홈리빙_인플루언서_{today}.xlsx')
        df.to_excel(excel_filename, index=False, engine='openpyxl')
        
        print(f"\n=== 검색 결과 ===")
        print(f"총 {len(result)}명의 인플루언서를 찾았습니다.")
        print(f"결과가 '{excel_filename}' 파일로 저장되었습니다.")
        
        # 첫 번째 인플루언서 정보 출력
        influencer = result[0]
        print("\n=== 첫 번째 인플루언서 정보 ===")
        print(f"이름: {influencer['clean_name']}")
        print(f"사용자명: {influencer['username']}")
        print(f"릴스 조회수: {influencer['reels_views(15)']}")
        print(f"프로필 링크: {influencer['profile_link']}")
    else:
        print("조건에 맞는 인플루언서를 찾을 수 없습니다.")
        
        # 조건별 개수 확인
        total_count = collection.count_documents({})
        category_count = collection.count_documents({"category": {"$regex": "홈/리빙\\([3-9][0-9]%|100%\\)"}})
        views_count = collection.count_documents({
            "reels_views(15)": {
                "$gt": 5000,
                "$lte": 35000
            }
        })
        
        print("\n=== 조건별 데이터 개수 ===")
        print(f"전체 데이터 수: {total_count}")
        print(f"홈/리빙 30% 이상 데이터 수: {category_count}")
        print(f"릴스 조회수 5000 초과 3.5만 이하 데이터 수: {views_count}")

except Exception as e:
    print(f"에러 발생: {e}")

finally:
    client.close()
    print("\nMongoDB 연결 종료")
