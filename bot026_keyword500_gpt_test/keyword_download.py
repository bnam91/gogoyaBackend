from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import os
import re
import json

# 제외할 키워드 그룹 정의
EXCLUDED_KEYWORDS = {
    '브랜드': [
        '나이키', '아디다스', '뉴발란스', '언더아머', '푸마', '리복', '아식스', '미즈노',
        '스케쳐스', '컨버스', '반스', '크록스', '살로몬', '노스페이스', '파타고니아',
        '아르마니', '구찌', '루이비통', '샤넬', '프라다', '버버리', '몽클레어', '니콘',
        '에르메스', '발렌시아가', '펜디', '디올', '입생로랑', '보테가베네타', '발렌티노',
        '메종마르지엘라', '톰브라운', '셀린느', '지방시', '골든구스', '메종키츠네',
        '톰포드', '몽블랑', '롤렉스', '오메가', '까르띠에', '불가리', '티파니',
        '자라', '유니클로', 'H&M', '무신사', '더블유컨셉', '29CM', '에이블리',
        '화웨이', '소니', '캐논', '파나소닉',
        '다이슨', '필립스', '일렉트로룩스', '테팔', '쿠쿠', '락앤락', '코렐',
        '네스프레소', '스타벅스', '투썸플레이스', '이디야', '메가커피',
        '맥도날드', '버거킹', '롯데리아', 'KFC', '서브웨이',
        '올리브영', '롭스', '시코르', '세포라', '랄라블라',
        '닌텐도', '플레이스테이션', '엑스박스',
        '갭', '폴로', '라코스테', '타미힐피거', '리바이스',
        '코치', '마이클코어스', '케이트스페이드', '토리버치',
        '아웃백', '빕스', '애슐리', '배스킨라빈스', '던킨',
        '다이소', '이케아', '한샘', '리바트', '시몬스',
        '코카콜라', '펩시', '농심', '오뚜기', '롯데', '해태',
        '아모레퍼시픽', '이니스프리', '에뛰드', '설화수', '후',
        '비비안웨스트우드', '폴스미스', '닥터마틴', '팀버랜드', '키엘'
    ],
    '사용자_제외': [
        '추천', '스튜디오', '마스크팩', '제작', '쉬폰',
        '카카오프렌즈', '라인프렌즈', '브라운', '코니', '샐리', '초코', '라이언', '어피치',
        '포켓몬', '피카츄', '푸린', '이브이', '잠만보', '파이리', '꼬부기', '뮤', '뮤츠',
        '디즈니', '미키마우스', '미니마우스', '구피', '도날드덕', '푸우', '티거', '피글렛',
        '산리오', '헬로키티', '마이멜로디', '시나모롤', '폼폼푸린', '구데타마'
        # 사용자가 추가하고 싶은 제외 키워드를 여기에 추가
    ]
}

def is_valid_keyword(keyword):
    # 한글이 없는 경우 제외
    if not re.search(r'[가-힣]', keyword):
        return False
    
    # 길이 체크 (2글자 이하 또는 10글자 이상)
    if len(keyword) <= 2 or len(keyword) >= 10:
        return False
    
    # 제외 키워드 그룹 체크
    for group, excluded_words in EXCLUDED_KEYWORDS.items():
        for excluded_word in excluded_words:
            if excluded_word in keyword:
                return False
    
    return True

def get_category_input(collection):
    while True:
        # 사용 가능한 1차 카테고리 목록 출력
        all_ids = collection.distinct("_id")
        primary_categories = sorted(set(id.split('_')[0] for id in all_ids))
        
        print("\n=== 카테고리 선택 ===")
        print("0. 전체 도큐먼트 선택")
        print("\n=== 사용 가능한 1차 카테고리 목록 ===")
        for idx, category in enumerate(primary_categories, 1):
            print(f"{idx}. {category}")
        print("================================")
        
        # 사용자 입력 받기
        user_input = input("\n번호를 입력하세요 (0: 전체 선택, 또는 1차 카테고리 번호/직접 입력): ").strip()
        
        # 전체 선택인 경우
        if user_input == "0":
            return "ALL", None
        
        selected_primary = None
        # 번호로 입력한 경우
        if user_input.isdigit():
            idx = int(user_input) - 1
            if 0 <= idx < len(primary_categories):
                selected_primary = primary_categories[idx]
            else:
                print("잘못된 번호입니다. 다시 입력해주세요.")
                continue
        # 직접 입력한 경우
        else:
            if user_input in primary_categories:
                selected_primary = user_input
            else:
                print("입력하신 1차 카테고리가 존재하지 않습니다. 다시 입력해주세요.")
                continue
        
        # 해당 1차 카테고리의 2차 카테고리 목록 출력
        secondary_categories = sorted([id for id in all_ids if id.startswith(selected_primary + '_')])
        
        print(f"\n=== '{selected_primary}'의 2차 카테고리 목록 ===")
        print("0. 모든 2차 카테고리 선택")
        for idx, category in enumerate(secondary_categories, 1):
            print(f"{idx}. {category}")
        print("================================")
        
        # 2차 카테고리 선택
        secondary_input = input("\n2차 카테고리 번호를 입력하세요 (0: 전체 선택): ").strip()
        
        if secondary_input == "0":
            return selected_primary, None  # 1차 카테고리만 반환
        elif secondary_input.isdigit():
            idx = int(secondary_input) - 1
            if 0 <= idx < len(secondary_categories):
                return None, secondary_categories[idx]  # 선택된 2차 카테고리 반환
            else:
                print("잘못된 번호입니다. 다시 처음부터 입력해주세요.")
                continue
        else:
            print("잘못된 입력입니다. 다시 처음부터 입력해주세요.")
            continue

# 현재 스크립트의 디렉토리 경로 가져오기
current_dir = os.path.dirname(os.path.abspath(__file__))

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri,
                    connectTimeoutMS=60000,
                    socketTimeoutMS=60000)

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")
    
    # 데이터베이스와 컬렉션 선택
    db = client['insta09_database']
    collection = db['gogoya_keyword500']
    
    # 사용자로부터 카테고리 입력 받기
    primary_category, secondary_category = get_category_input(collection)
    
    # 모든 _id 목록 가져오기
    all_ids = collection.distinct("_id")
    ids_df = pd.DataFrame(all_ids, columns=['카테고리_ID'])
    
    # 데이터프레임 생성 (필터링 적용)
    df_data = []
    filtered_count = 0
    processed_categories = []
    
    if primary_category == "ALL":  # 전체 도큐먼트 선택
        for category_id in all_ids:
            data = collection.find_one({"_id": category_id})
            if data and 'keyword_history' in data and len(data['keyword_history']) > 0:
                latest_keywords = data['keyword_history'][0]['keywords']
                for keyword_data in latest_keywords:
                    keyword = keyword_data['keyword']
                    if is_valid_keyword(keyword):
                        df_data.append({
                            '순위': keyword_data['rank'],
                            '키워드': keyword,
                            '카테고리': category_id
                        })
                    else:
                        filtered_count += 1
                processed_categories.append(category_id)
    elif secondary_category:  # 2차 카테고리가 선택된 경우
        data = collection.find_one({"_id": secondary_category})
        if data and 'keyword_history' in data and len(data['keyword_history']) > 0:
            latest_keywords = data['keyword_history'][0]['keywords']
            for keyword_data in latest_keywords:
                keyword = keyword_data['keyword']
                if is_valid_keyword(keyword):
                    df_data.append({
                        '순위': keyword_data['rank'],
                        '키워드': keyword,
                        '카테고리': secondary_category
                    })
                else:
                    filtered_count += 1
            processed_categories.append(secondary_category)
    else:  # 1차 카테고리만 선택된 경우
        matching_categories = [id for id in all_ids if id.startswith(primary_category + '_')]
        for category_id in matching_categories:
            data = collection.find_one({"_id": category_id})
            if data and 'keyword_history' in data and len(data['keyword_history']) > 0:
                latest_keywords = data['keyword_history'][0]['keywords']
                for keyword_data in latest_keywords:
                    keyword = keyword_data['keyword']
                    if is_valid_keyword(keyword):
                        df_data.append({
                            '순위': keyword_data['rank'],
                            '키워드': keyword,
                            '카테고리': category_id
                        })
                    else:
                        filtered_count += 1
                processed_categories.append(category_id)
    
    if df_data:
        # 데이터프레임 생성 시 필요한 컬럼만 포함
        df = pd.DataFrame([{
            '_id': item['카테고리'],
            '순위': item['순위'],
            '키워드': item['키워드']
        } for item in df_data])
        
        # 현재 날짜를 파일명에 포함
        current_date = datetime.now().strftime('%Y%m%d')
        if primary_category == "ALL":
            json_filename = f'전체_키워드_{current_date}.json'
        elif secondary_category:
            json_filename = f'{secondary_category}_키워드_{current_date}.json'
        else:
            json_filename = f'{primary_category}_전체_키워드_{current_date}.json'
        
        # 전체 파일 경로 생성
        json_filepath = os.path.join(current_dir, json_filename)
        
        # JSON 파일로 저장
        df.to_json(json_filepath, orient='records', force_ascii=False, indent=2)
            
        print(f"\nJSON 파일이 성공적으로 저장되었습니다: {json_filepath}")
        print(f"처리된 카테고리 수: {len(processed_categories)}개")
        print(f"필터링된 키워드 수: {filtered_count}개")
        print(f"저장된 키워드 수: {len(df_data)}개")
        print("\n처리된 카테고리 목록:")
        for category in processed_categories:
            print(f"- {category}")
    else:
        if primary_category == "ALL":
            print("데이터를 찾을 수 없거나 키워드 히스토리가 비어있습니다.")
        elif secondary_category:
            print(f"데이터를 찾을 수 없거나 키워드 히스토리가 비어있습니다. (카테고리 ID: {secondary_category})")
        else:
            print(f"데이터를 찾을 수 없거나 키워드 히스토리가 비어있습니다. (1차 카테고리: {primary_category})")

except Exception as e:
    print(f"에러 발생: {e}")

finally:
    client.close()
    print("\nMongoDB 연결 종료")
