"""
1-3_item_today.py - 인플루언서 공구 아이템 데이터 처리 및 업데이트

[입력 데이터]
1. 01_main_newfeed_crawl_data (MongoDB)
   - 크롤링된 새로운 피드 데이터
   - 공구 아이템 정보 포함
   - 주요 필드: author, post_url, crawl_date, 09_brand, 09_item, 09_item_category, processed

2. 08_main_brand_category_data (MongoDB)
   - 브랜드 정보 및 카테고리 매핑 데이터
   - 브랜드 별칭(aliases) 정보 포함
   - 주요 필드: name, category, level, aliases

3. 02_main_influencer_data (MongoDB)
   - 인플루언서 기본 정보
   - 등급 및 카테고리 정보
   - 주요 필드: username, clean_name, grade, category

4. 04_main_item_today_data (MongoDB)
   - 기존 처리된 아이템 데이터
   - NEW 표시 상태 정보
   - 주요 필드: NEW, crawl_date, brand_level, brand_category, brand, item, 
             item_category, author, clean_name, grade, category, item_feed_link

[출력 데이터]
1. 04_main_item_today_data (MongoDB) 업데이트
   - 새로운 아이템 추가
   - NEW 표시 상태 갱신 (3일 이내 데이터)
   - 중복 데이터 제거

2. 01_main_newfeed_crawl_data (MongoDB) 업데이트
   - 처리 완료된 피드 데이터 상태 변경 (processed: true)

[처리 프로세스]
1. 미처리 공구 아이템 식별 및 추출
   - processed가 true인 데이터 추출
   - 09_brand 필드가 있는 데이터만 선별

2. 브랜드 정보 매핑 및 카테고리 연동
   - 브랜드명 정규화 및 매핑
   - 브랜드의 level, category 정보 매핑
   - aliases를 통한 브랜드명 표기 통일

3. 인플루언서 정보 매핑
   - username 기준 인플루언서 정보 매핑
   - clean_name, grade, category 정보 연동

4. 중복 데이터 필터링
   - 동일 작성자/브랜드 기준 중복 체크
   - 20일 이내 중복 게시물 필터링
   - 기존 데이터와 비교 검증

5. NEW 표시 상태 관리
   - 3일 이내 데이터 'NEW' 표시
   - 기존 데이터 NEW 상태 갱신

6. 데이터베이스 동기화
   - 새로운 아이템 추가
   - 처리 상태 업데이트
   - NEW 표시 상태 갱신
"""

from datetime import datetime
import json
from pymongo import MongoClient
from pymongo.server_api import ServerApi



def get_base_author(author_str):
    """작성자 문자열에서 기본 닉네임 추출"""
    return author_str.split('(')[0].strip()

def should_combine_dates(date1, date2):
    """두 날짜가 20일 이내인지 확인
    날짜가 없는 경우 오늘 날짜 기준으로 계산"""
    try:
        # 날짜가 없는 경우 오늘 날짜 사용
        if not date1:
            date1 = datetime.now().strftime('%Y-%m-%d')
        if not date2:
            date2 = datetime.now().strftime('%Y-%m-%d')
            
        d1 = datetime.strptime(date1, '%Y-%m-%d')
        d2 = datetime.strptime(date2, '%Y-%m-%d')
        return abs((d1 - d2).days) < 20
    except:
        return True
def ensure_indexes(db):
    """필요한 컬렉션에 인덱스가 있는지 확인하고 없으면 생성합니다"""
    print("인덱스 확인 및 생성 중...")
    collections_to_index = {
        '04_main_item_today_data': [
            ('author', 1),
            ('brand', 1), 
            ('crawl_date', 1),
            [('author', 1), ('brand', 1)]  # 복합 인덱스
        ],
        '01_main_newfeed_crawl_data': [
            ('author', 1),
            ('09_brand', 1),
            ('processed', 1)
        ],
        '02_main_influencer_data': [
            ('username', 1)
        ]
    }
    
    for collection_name, indexes in collections_to_index.items():
        existing_indexes = db[collection_name].index_information()
        for index in indexes:
            # 단일 필드 인덱스
            if isinstance(index, tuple):
                field, direction = index
                index_name = f"{field}_{direction}"
                if index_name not in existing_indexes:
                    print(f"컬렉션 '{collection_name}'에 '{field}' 인덱스 생성 중...")
                    db[collection_name].create_index([(field, direction)])
            # 복합 인덱스
            elif isinstance(index, list):
                # 복합 인덱스 이름 생성 (예: author_1_brand_1)
                index_name = "_".join([f"{field}_{direction}" for field, direction in index])
                if index_name not in existing_indexes:
                    print(f"컬렉션 '{collection_name}'에 복합 인덱스 {index} 생성 중...")
                    db[collection_name].create_index(index)
    
    print("인덱스 확인 및 생성 완료")


def update_data():
    try:
        # MongoDB 연결
        uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        client = MongoClient(uri, server_api=ServerApi('1'))
        db = client['insta09_database']
        
        ensure_indexes(db)

        print("데이터 업데이트를 시작합니다...")
        today_date = datetime.now()
        
        # 기존 데이터 읽기 및 NEW 표시 업데이트
        try:
            existing_data = list(db['04_main_item_today_data'].find())
            print(f"- 기존 데이터 수: {len(existing_data)}개")
            
            # 기존 데이터의 NEW 표시 업데이트
            for item in existing_data:
                crawl_date = item.get('crawl_date', '')
                if isinstance(crawl_date, str):
                    # 문자열인 경우 datetime으로 변환
                    try:
                        collection_date = datetime.strptime(crawl_date, '%Y-%m-%d')
                    except:
                        collection_date = today_date
                elif isinstance(crawl_date, datetime):
                    # 이미 datetime 객체인 경우 그대로 사용
                    collection_date = crawl_date
                else:
                    # 다른 경우 (None 등) 오늘 날짜 사용
                    collection_date = today_date
                
                days_difference = (today_date - collection_date).days
                item['NEW'] = 'NEW' if days_difference <= 3 else ''
        except Exception as e:
            existing_data = []
            print(f"- 기존 데이터 읽기 오류: {str(e)}")

        # 새로운 피드 데이터 읽기
        try:
            newfeed_data = list(db['01_main_newfeed_crawl_data'].find())
            if not newfeed_data:  # 데이터가 비어있는 경우
                print("- 새로운 피드 데이터가 없습니다.")
                newfeed_data = []
            else:
                print(f"- 새로운 피드 데이터 수: {len(newfeed_data)}개")
        except Exception as e:
            print(f"새로운 피드 데이터 읽기 오류: {str(e)}")
            newfeed_data = []

        # brand_category.json 파일 읽기 대신 MongoDB에서 브랜드 카테고리 데이터 읽기
        try:
            brand_category_data = list(db['08_main_brand_category_data'].find())
            print(f"- 브랜드 카테고리 데이터 수: {len(brand_category_data)}개")
        except Exception as e:
            print(f"브랜드 카테고리 데이터 읽기 오류: {str(e)}")
            brand_category_data = []

        # 인플루언서 데이터 읽기
        try:
            influencer_data = list(db['02_main_influencer_data'].find())
            # username으로 빠르게 검색하기 위한 딕셔너리 생성
            influencer_dict = {inf['username']: inf for inf in influencer_data}
        except Exception as e:
            print(f"인플루언서 데이터 읽기 오류: {str(e)}")
            influencer_dict = {}

        # 브랜드 매핑 딕셔너리 생성 수정
        brand_mapping = {}
        for brand_info in brand_category_data:
            brand_name = brand_info.get('name', '')
            brand_mapping[brand_name] = {
                'name': brand_name,
                'category': brand_info.get('category', ''),
                'level': brand_info.get('level', '')
            }
            if brand_info.get('aliases'):
                for alias in brand_info['aliases']:
                    brand_mapping[alias] = {
                        'name': brand_name,
                        'category': brand_info.get('category', ''),
                        'level': brand_info.get('level', '')
                    }

        # 새로운 피드 데이터 처리
        new_items = []
        processed_count = 0

        for item in newfeed_data:
            print(f"\n처리 시작: {item.get('author')} - {item.get('09_brand')}")
            
            # processed가 있고 True인 경우, 09_brand가 있는 경우 처리
            if (('processed' in item and item['processed'] == True) and item.get('09_brand')):
                brand = item['09_brand'].strip()
                brand_info = brand_mapping.get(brand, {'name': brand, 'category': '', 'level': ''})
                
                # 인플루언서 정보 가져오기
                influencer = influencer_dict.get(item['author'], {})
                
                processed_item = {
                    'NEW': 'NEW',
                    'crawl_date': item['crawl_date'],
                    'brand_level': brand_info['level'],                    
                    'brand': brand_info['name'],
                    'item_category': item.get('09_item_category', ''),
                    'item': item.get('09_item', ''),            
                    'brand_category': brand_info['category'],
                    'author': item['author'],
                    'clean_name': influencer.get('clean_name', ''),
                    'grade': influencer.get('grade', ''),
                    'category': influencer.get('category', ''),
                    'item_feed_link': item['post_url']
                }

                # 중복 체크
                is_duplicate = False
                for existing_item in existing_data + new_items:
                    same_author = get_base_author(existing_item['author']) == get_base_author(processed_item['author'])
                    same_brand = existing_item['brand'] == processed_item['brand']
                    dates_within_range = should_combine_dates(existing_item['crawl_date'], processed_item['crawl_date'])
                    
                    if same_author and same_brand and dates_within_range:
                        is_duplicate = True
                        print(f"중복 발견: {processed_item['author']} - {processed_item['brand']}")
                        break

                if not is_duplicate:
                    new_items.append(processed_item)
                    # MongoDB update
                    db['01_main_newfeed_crawl_data'].update_one(
                        {'_id': item['_id']},
                        {'$set': {'processed': True}}
                    )
                    processed_count += 1
                    print(f"처리 완료: {processed_item['author']} - {processed_item['brand']}")

        # 최종 데이터 MongoDB에 저장
        try:
            if new_items:
                # 새로운 아이템들 추가
                try:
                    before_count = db['04_main_item_today_data'].count_documents({})
                    print(f"삽입 전 문서 수: {before_count}")
                    
                    result = db['04_main_item_today_data'].insert_many(new_items)
                    
                    after_count = db['04_main_item_today_data'].count_documents({})
                    print(f"- 새로운 데이터 {len(new_items)}개 중 {after_count - before_count}개가 성공적으로 저장되었습니다.")
                    print(f"- 저장된 문서 ID들: {result.inserted_ids}")
                    
                    # 실제 저장 확인
                    saved_docs = list(db['04_main_item_today_data'].find({'_id': {'$in': result.inserted_ids}}))
                    print(f"- 실제 저장된 문서 수: {len(saved_docs)}개")
                    
                except Exception as e:
                    print(f"새로운 데이터 삽입 중 오류 발생: {str(e)}")
            
            # 기존 데이터 NEW 표시 업데이트
            for item in existing_data:
                if '_id' in item:  # _id가 있는 경우에만 업데이트
                    try:
                        result = db['04_main_item_today_data'].update_one(
                            {'_id': item['_id']},
                            {'$set': {'NEW': item['NEW']}}
                        )
                        if result.modified_count > 0:
                            print(f"- 문서 {item['_id']} NEW 표시 업데이트 완료")
                    except Exception as e:
                        print(f"문서 {item['_id']} 업데이트 중 오류 발생: {str(e)}")
            
            print("\n최종 데이터베이스 상태:")
            final_count = db['04_main_item_today_data'].count_documents({})
            print(f"- 전체 데이터 수: {final_count}개")

        except Exception as e:
            print(f"데이터 저장 중 오류 발생: {str(e)}")
            print("오류 발생 위치:", e.__class__.__name__)
            print("상세 스택트레이스:")
            import traceback
            print(traceback.format_exc())

        print(f"\n작업이 완료되었습니다!")
        print(f"- 최종 데이터 수: {len(existing_data) + len(new_items)}개")
        print(f"- 새로 추가된 데이터 수: {len(new_items)}개")
        print(f"- 처리된 아이템 수: {processed_count}개")

    except Exception as e:
        print(f"\n오류 발생: {str(e)}")
    finally:
        client.close()

def format_date(date_str):
    """오픈예정일 포맷 변환 (2025-02-10 -> 250210)"""
    if not date_str or date_str.strip() == '':
        return '-'
    try:
        date_obj = datetime.strptime(date_str.strip(), '%Y-%m-%d')
        return date_obj.strftime('%y%m%d')
    except:
        return '-'

if __name__ == "__main__":
    update_data()
