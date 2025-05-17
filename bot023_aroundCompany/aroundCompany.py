from pymongo import MongoClient
from pymongo.server_api import ServerApi
from collections import defaultdict
import pandas as pd
from datetime import datetime
import os

# 카테고리 정의
CATEGORIES = {
    0: "📋전체 선택",
    1: "🍽주방용품&식기",
    2: "🛋생활용품&가전",
    3: "🥦식품&건강식품",
    4: "🧴뷰티&헬스",
    5: "👶유아&교육",
    6: "👗의류&잡화",
    7: "🚗기타"
}

def get_user_categories():
    print("\n=== 카테고리 목록 ===")
    for num, category in CATEGORIES.items():
        print(f"{num}. {category}")
    
    print("\n원하는 카테고리 번호를 입력해주세요 (여러 개 선택 시 쉼표로 구분)")
    print("예시: 1,3,5 (전체 선택: 0)")
    
    while True:
        try:
            selected = input("카테고리 번호 입력: ").strip()
            if not selected:
                print("카테고리가 선택되지 않았습니다. 다시 입력해주세요.")
                continue
                
            # 선택된 번호를 정수 리스트로 변환
            selected_nums = [int(num.strip()) for num in selected.split(',')]
            
            # 0(전체 선택)이 포함되어 있으면 모든 카테고리 반환
            if 0 in selected_nums:
                return list(CATEGORIES.values())[1:]  # 0번(전체 선택) 제외한 모든 카테고리
            
            # 유효한 번호인지 확인
            invalid_nums = [num for num in selected_nums if num not in CATEGORIES]
            if invalid_nums:
                print(f"잘못된 번호가 포함되어 있습니다: {invalid_nums}")
                continue
                
            # 선택된 카테고리 이름 리스트 반환
            return [CATEGORIES[num] for num in selected_nums]
            
        except ValueError:
            print("올바른 형식으로 입력해주세요 (예: 1,3,5 또는 0)")

def get_mongodb_connection():
    uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi('1'))
    
    try:
        # 연결 테스트
        client.admin.command('ping')
        print("MongoDB 연결 성공!")
        
        # 데이터베이스 선택
        db = client['insta09_database']
        
        # 컬렉션 매핑
        collections = {
            'feeds': db['01_main_newfeed_crawl_data'],
            'influencers': db['02_main_influencer_data'],
            'brands': db['08_main_brand_category_data'],
            'vendor_brands': db['gogoya_vendor_brand_info'],
            'item_today': db['04_main_item_today_data']  # 아이템 데이터 컬렉션 추가
        }
        
        return client, collections
    except Exception as e:
        print(f"MongoDB 연결 실패: {str(e)}")
        raise

def find_brands_in_area():
    client, collections = get_mongodb_connection()
    
    try:
        # 사용자로부터 검색할 지역 입력 받기
        print("\n검색할 지역을 입력해주세요 (여러 지역은 쉼표로 구분)")
        print("예시: 성남,강남,분당")
        search_areas = input("지역 입력: ").strip()
        
        if not search_areas:
            print("지역이 입력되지 않았습니다.")
            return
            
        # 카테고리 선택 받기
        selected_categories = get_user_categories()
        print(f"\n선택된 카테고리: {', '.join(selected_categories)}")
            
        # 입력된 지역들을 리스트로 변환하고 정규식 패턴 생성
        area_list = [area.strip() for area in search_areas.split(',')]
        regex_pattern = '|'.join(area_list)
        
        # 검색 쿼리 수정 (지역 검색 + is_verified 필터링)
        query = {
            '$and': [
                {
                    'business_address': {
                        '$regex': regex_pattern,
                        '$options': 'i'
                    }
                },
                {
                    'is_verified': {'$ne': 'false'}
                }
            ]
        }
        
        # 엑셀 저장을 위한 데이터 리스트
        excel_data = []
        
        # vendor_brands 컬렉션에서 브랜드 검색
        vendor_brands = collections['vendor_brands'].find(query)
        
        # 결과 출력
        print(f"\n=== {', '.join(area_list)} 지역의 인증된 브랜드 목록 ===")
        print(f"선택된 카테고리: {', '.join(selected_categories)}")
        brand_count = 0
        
        for vendor_brand in vendor_brands:
            brand_name = vendor_brand.get('brand_name', 'N/A')
            
            # 해당 브랜드의 아이템 데이터 검색
            item_query = {'brand': brand_name}
            item_projection = {
                'item_category': 1,
                'author': 1,
                'category': 1,
                '_id': 0
            }
            
            items = collections['item_today'].find(item_query, item_projection)
            
            # 선택된 카테고리에 해당하는 아이템만 필터링
            filtered_items = []
            for item in items:
                if item.get('item_category') in selected_categories:
                    filtered_items.append(item)
            
            # 필터링된 아이템이 있는 경우에만 브랜드 정보 출력
            if filtered_items:
                brand_count += 1
                print(f"\n{'='*80}")
                print(f"브랜드명: {brand_name}")
                print(f"주소: {vendor_brand.get('business_address', 'N/A')}")
                print(f"도메인 URL: {vendor_brand.get('actual_domain_url', 'N/A')}")
                print(f"인증 상태: {vendor_brand.get('is_verified', 'N/A')}")
                print(f"{'='*80}")
                
                # 카테고리별 통계를 위한 딕셔너리
                category_stats = defaultdict(int)
                author_stats = defaultdict(int)
                
                print("\n[아이템 상세 정보]")
                for idx, item in enumerate(filtered_items, 1):
                    print(f"\n아이템 #{idx}")
                    print(f"아이템 카테고리: {item.get('item_category', 'N/A')}")
                    print(f"작성자: {item.get('author', 'N/A')}")
                    print(f"카테고리: {item.get('category', 'N/A')}")
                    
                    # 엑셀 데이터 추가
                    excel_data.append({
                        '브랜드명': brand_name,
                        '주소': vendor_brand.get('business_address', 'N/A'),
                        '도메인 URL': vendor_brand.get('actual_domain_url', 'N/A'),
                        '인증 상태': vendor_brand.get('is_verified', 'N/A'),
                        '아이템 카테고리': item.get('item_category', 'N/A'),
                        '작성자': item.get('author', 'N/A'),
                        '카테고리': item.get('category', 'N/A')
                    })
                    
                    # 통계 데이터 수집
                    if item.get('item_category'):
                        category_stats[item['item_category']] += 1
                    if item.get('author'):
                        author_stats[item['author']] += 1
                
                print(f"\n[브랜드 '{brand_name}' 통계]")
                print(f"선택된 카테고리의 총 아이템 수: {len(filtered_items)}")
                
                if category_stats:
                    print("\n아이템 카테고리별 통계:")
                    for category, count in category_stats.items():
                        print(f"- {category}: {count}개")
                
                if author_stats:
                    print("\n작성자별 통계:")
                    for author, count in author_stats.items():
                        print(f"- {author}: {count}개")
                
                print(f"\n{'-'*80}")
        
        print(f"\n총 {brand_count}개의 브랜드가 검색되었습니다.")
        
        # 엑셀 파일 저장
        if excel_data:
            # 현재 시간을 파일명에 포함
            current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"brand_search_result_{current_time}.xlsx"
            
            # DataFrame 생성 및 엑셀 저장
            df = pd.DataFrame(excel_data)
            df.to_excel(filename, index=False, engine='openpyxl')
            print(f"\n검색 결과가 '{filename}' 파일로 저장되었습니다.")
            
    except Exception as e:
        print(f"검색 중 오류 발생: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    while True:
        find_brands_in_area()
        
        # 계속 검색할지 물어보기
        retry = input("\n다시 검색하시겠습니까? (y/n): ").strip().lower()
        if retry != 'y':
            print("검색을 종료합니다.")
            break
