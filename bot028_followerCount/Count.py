from pymongo import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
import numpy as np
import re

def extract_category_percentage(category_str, target_category):
    if pd.isna(category_str) or not category_str:
        return 0
    
    # 카테고리 문자열에서 타겟 카테고리의 퍼센트 추출
    pattern = f"{target_category}\((\d+)%\)"
    match = re.search(pattern, category_str)
    if match:
        return int(match.group(1))
    return 0

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

    # 데이터 가져오기 (09_is: "Y" 조건 추가)
    data = list(collection.find(
        {"09_is": "Y"},
        {
            'posts': 1,
            'followers': 1,
            'clean_name': 1,
            'username': 1,
            'reels_views(15)': 1,
            'category': 1,
            'profile_link': 1,
            '_id': 0
        }
    ))

    # DataFrame 생성
    df = pd.DataFrame(data)

    # 문자열에서 숫자만 추출하여 변환
    df['followers'] = df['followers'].str.replace(',', '').str.extract('(\d+)').astype(float)
    df['posts'] = df['posts'].str.replace(',', '').str.extract('(\d+)').astype(float)
    
    # 릴스 조회수 처리
    def process_reels_views(x):
        if pd.isna(x) or not x:
            return 0
        # 쉼표 제거 후 숫자만 추출
        num = str(x).replace(',', '')
        if num.isdigit():
            return float(num)
        return 0
    
    df['reels_views(15)'] = df['reels_views(15)'].apply(process_reels_views)

    # 0으로 나누는 것을 방지하기 위해 posts가 0인 경우 제외
    df = df[df['posts'] > 0]

    # 포스트 수 대비 팔로워 수 비율 계산
    df['follower_post_ratio'] = df['followers'] / df['posts']

    # 고정된 카테고리 목록
    categories = [
        '뷰티',
        '패션',
        '홈/리빙',
        '푸드',
        '육아',
        '건강',
        '맛집탐방',
        '전시/공연',
        '반려동물',
        '기타'
    ]

    # 카테고리 선택 프롬프트
    print("\n=== 사용 가능한 카테고리 ===")
    for idx, category in enumerate(categories, 1):
        print(f"{idx}. {category}")

    # 사용자 입력 받기
    while True:
        try:
            choice = int(input("\n카테고리 번호를 선택하세요: "))
            if 1 <= choice <= len(categories):
                selected_category = categories[choice-1]
                break
            else:
                print("유효한 번호를 입력해주세요.")
        except ValueError:
            print("숫자를 입력해주세요.")

    # 선택된 카테고리의 퍼센트 계산
    df['category_percentage'] = df['category'].apply(
        lambda x: extract_category_percentage(x, selected_category)
    )

    # 30% 이상인 경우만 필터링
    filtered_df = df[df['category_percentage'] >= 30]

    # 결과 정렬 및 출력
    result = filtered_df.sort_values('follower_post_ratio', ascending=False)
    
    print(f"\n=== {selected_category} 카테고리 30% 이상의 포스트 수 대비 팔로워 수가 많은 상위 10명 ===")
    for idx, row in result.head(10).iterrows():
        print(f"\n이름: {row['clean_name']}")
        print(f"사용자명: {row['username']}")
        print(f"팔로워 수: {row['followers']:,.0f}")
        print(f"포스트 수: {row['posts']:,.0f}")
        print(f"팔로워/포스트 비율: {row['follower_post_ratio']:,.2f}")
        print(f"릴스 평균 조회수: {row['reels_views(15)']:,.0f}")
        print(f"카테고리: {row['category']}")
        print(f"프로필 링크: {row['profile_link']}")
        print("-" * 50)

except Exception as e:
    print(f"에러 발생: {e}")
finally:
    client.close()
