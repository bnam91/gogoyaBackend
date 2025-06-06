from pymongo import MongoClient
from datetime import datetime
import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri,
                    connectTimeoutMS=60000,  # 연결 타임아웃을 60초로 증가
                    socketTimeoutMS=60000)   # 소켓 타임아웃도 60초로 증가

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")
    
    # 데이터베이스와 컬렉션 선택
    db = client['insta09_database']
    influencer_collection = db['02_main_influencer_data']
    feed_collection = db['01_main_newfeed_crawl_data']
    
except Exception as e:
    print(f"MongoDB 연결 실패: {e}")
    exit(1)

def find_influencer_username(search_name):
    """
    인플루언서의 username 또는 clean_name으로 username을 찾는 함수
    
    Args:
        search_name (str): 검색할 username 또는 clean_name
    
    Returns:
        str: 찾은 username 또는 None
    """
    # username으로 검색
    influencer = influencer_collection.find_one({'username': search_name})
    if influencer:
        return influencer['username']
    
    # clean_name으로 검색
    influencer = influencer_collection.find_one({'clean_name': search_name})
    if influencer:
        return influencer['username']
    
    return None

def highlight_keyword(text, keyword):
    """
    텍스트에서 키워드를 찾아 하이라이트 처리하는 함수
    
    Args:
        text (str): 원본 텍스트
        keyword (str): 하이라이트할 키워드
    
    Returns:
        str: 키워드가 하이라이트된 텍스트
    """
    if not text or not keyword:
        return text
    
    # 키워드의 위치 찾기
    start_idx = text.lower().find(keyword.lower())
    if start_idx == -1:
        return text
    
    # 키워드 앞뒤로 50자씩 추출
    context_start = max(0, start_idx - 50)
    context_end = min(len(text), start_idx + len(keyword) + 50)
    
    # 컨텍스트 추출
    context = text[context_start:context_end]
    
    # 키워드 하이라이트
    highlighted = context.replace(
        text[start_idx:start_idx + len(keyword)],
        f"【{text[start_idx:start_idx + len(keyword)]}】"
    )
    
    # 앞뒤에 ... 추가
    if context_start > 0:
        highlighted = "..." + highlighted
    if context_end < len(text):
        highlighted = highlighted + "..."
    
    return highlighted

def search_content_by_author_and_keyword(search_name, search_keyword):
    """
    특정 작성자의 컨텐츠에서 검색어가 포함된 게시물을 찾는 함수
    
    Args:
        search_name (str): 검색할 인플루언서의 username 또는 clean_name
        search_keyword (str): 검색할 키워드
    
    Returns:
        list: 검색 결과 리스트 (post_url과 cr_at 포함)
    """
    try:
        # 인플루언서 username 찾기
        username = find_influencer_username(search_name)
        if not username:
            print(f"\n입력하신 '{search_name}'에 해당하는 인플루언서를 찾을 수 없습니다.")
            return []
        
        print(f"\n인플루언서 username: {username}")
        
        # 해당 작성자의 모든 게시물 수 확인
        total_posts = feed_collection.count_documents({'author': username})
        print(f"해당 작성자의 전체 게시물 수: {total_posts}개")
        
        # 실제 검색 쿼리 실행
        query = {
            'author': username,
            'content': {'$regex': search_keyword, '$options': 'i'}  # 대소문자 구분 없이 검색
        }
        
        projection = {
            'post_url': 1,    # 게시물 URL
            'cr_at': 1,       # 작성 시간
            'content': 1,     # 게시물 내용
            '09_brand': 1,    # 브랜드 정보
            '09_item': 1,     # 아이템 정보
            '_id': 0          # _id는 제외
        }
        
        results = list(feed_collection.find(query, projection))
        return results
    except Exception as e:
        print(f"검색 중 오류 발생: {e}")
        return []

if __name__ == "__main__":
    # 사용자 입력 받기
    print("\n=== 인스타그램 게시물 검색 ===")
    search_name = input("검색할 인플루언서의 username 또는 clean_name을 입력하세요: ").strip()
    keyword = input("검색할 키워드를 입력하세요: ").strip()
    
    if not search_name or not keyword:
        print("인플루언서 이름과 검색 키워드를 모두 입력해주세요.")
        exit(1)
    
    print(f"\n검색 중... (인플루언서: {search_name}, 키워드: {keyword})")
    search_results = search_content_by_author_and_keyword(search_name, keyword)
    
    # 결과 출력
    if search_results:
        print(f"\n검색 결과: {len(search_results)}개의 게시물을 찾았습니다.")
        for result in search_results:
            print(f"게시물 URL: {result['post_url']}")
            print(f"작성 시간: {result['cr_at']}")
            print(f"브랜드: {result.get('09_brand', '정보 없음')}")
            print(f"아이템: {result.get('09_item', '정보 없음')}")
            print(f"내용: {highlight_keyword(result.get('content', '내용 없음'), keyword)}")
            print("-" * 50)
    else:
        print("검색 결과가 없습니다.")
