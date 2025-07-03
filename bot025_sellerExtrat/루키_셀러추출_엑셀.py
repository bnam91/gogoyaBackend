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
    
    # 사용자 입력 받기
    print("\n=== 카테고리 선택 ===")
    print("1. 홈/리빙")
    print("2. 푸드")
    print("3. 육아")
    category_choice = input("카테고리를 선택하세요 (1-3): ")
    
    category_map = {
        "1": "홈/리빙",
        "2": "푸드",
        "3": "육아"
    }
    
    selected_category = category_map.get(category_choice)
    if not selected_category:
        raise ValueError("잘못된 카테고리 선택입니다.")
    
    print("\n카테고리 비율 범위를 입력하세요 (예: 30,100 또는 15,30)")
    print("첫 번째 숫자: 최소 비율")
    print("두 번째 숫자: 최대 비율")
    min_percentage, max_percentage = map(int, input("비율 범위를 입력하세요 (쉼표로 구분): ").split(','))
    
    if min_percentage < 0 or max_percentage > 100 or min_percentage > max_percentage:
        raise ValueError("카테고리 비율은 0에서 100 사이여야 하며, 최소값은 최대값보다 작아야 합니다.")
    
    min_views = int(input("최소 릴스 조회수를 입력하세요: "))
    
    # 데이터베이스와 컬렉션 선택
    db = client['insta09_database']
    collection = db['02_main_influencer_data']
    
    # 정규식 패턴 생성
    if max_percentage == 100:
        percentage_pattern = f"{selected_category}\\([3-9][0-9]%|100%\\)"
    else:
        percentage_pattern = f"{selected_category}\\({min_percentage}%|{max_percentage}%|"
        for i in range(min_percentage + 1, max_percentage):
            percentage_pattern += f"{i}%|"
        percentage_pattern = percentage_pattern.rstrip("|") + "\\)"
    
    # 조건에 맞는 인플루언서 검색
    pipeline = [
        {
            "$match": {
                "category": {"$regex": percentage_pattern},
                "is_contact_excluded": {"$ne": True},
                "reels_views(15)": {
                    "$gt": min_views
                },
                "09_is": "Y"  # 09is를 09_is로 수정
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
        
        # 파일명에서 사용할 수 없는 문자 제거
        safe_category = selected_category.replace('/', '_')
        
        # 엑셀 파일로 저장
        excel_filename = os.path.join(current_dir, f'{safe_category}_인플루언서_{today}.xlsx')
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
        category_count = collection.count_documents({"category": {"$regex": percentage_pattern}})
        views_count = collection.count_documents({
            "reels_views(15)": {
                "$gt": min_views
            }
        })
        
        print("\n=== 조건별 데이터 개수 ===")
        print(f"전체 데이터 수: {total_count}")
        print(f"{selected_category} {min_percentage}%~{max_percentage}% 데이터 수: {category_count}")
        print(f"릴스 조회수 {min_views} 초과 데이터 수: {views_count}")

except Exception as e:
    print(f"에러 발생: {e}")

finally:
    client.close()
    print("\nMongoDB 연결 종료")
