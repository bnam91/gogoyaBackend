from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import os
import shutil
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import datetime, timezone, timedelta
import json
import random
from googleapiclient.discovery import build
import sys
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from urllib.parse import urlparse, urlunsplit

# auth.py 파일 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from auth import get_credentials
from module.reels_analyzer import ReelsAnalyzer

def clean_url(url):
    """URL에서 쿼리 파라미터를 제거하는 함수"""
    parsed = urlparse(url)
    clean = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, '', ''))
    return clean

def load_processed_posts(collection):
    """MongoDB에서 게시물 URL들을 로드 (최적화된 버전)"""
    processed_posts = set()
    
    try:
        # MongoDB에서 URL만 선택적으로 로드 (프로젝션 사용)
        mongo_posts = collection.find(
            {}, 
            {"post_url": 1, "_id": 0},
            batch_size=1000  # 배치 크기 설정
        )
        
        # URL만 추출하여 set에 추가
        processed_posts = {post["post_url"] for post in mongo_posts if "post_url" in post}
        print(f"🚩MongoDB에서 {len(processed_posts)}개의 게시물 URL을 로드했습니다.")
    except Exception as e:
        print(f"MongoDB 데이터 로드 중 오류 발생: {str(e)}")
        # 오류가 발생해도 빈 set을 반환하여 크롤링을 계속할 수 있도록 함
        return set()
    
    return processed_posts

# MongoDB 연결 설정
def connect_mongodb():
    uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi('1'),
                        connectTimeoutMS=60000,  # 연결 타임아웃을 60초로 증가
                        socketTimeoutMS=60000)   # 소켓 타임아웃도 60초로 증가

    try:
        # 연결 확인
        client.admin.command('ping')
        print("MongoDB 연결 성공!")
        
        # 데이터베이스와 컬렉션 선택
        db = client['insta09_database']
        collection_feed = db['01_main_newfeed_crawl_data']
        collection_influencer = db['02_main_influencer_data']
        
        # post_url에 Unique Index 생성
        collection_feed.create_index("post_url", unique=True)
        print("post_url에 Unique Index가 생성되었습니다.")

        return collection_feed, collection_influencer
    except Exception as e:
        print(f"MongoDB 연결 또는 인덱스 생성 실패: {e}")
        return None, None

def update_mongodb_data(values, collection):
    """MongoDB에 데이터 저장"""
    try:
        post_url = clean_url(values[3])  # URL 파라미터 제거
        
        # 중복 체크
        existing_post = collection.find_one({"post_url": post_url})
        if existing_post:
            print(f"\n이미 존재하는 게시물입니다: {post_url}")
            return True

        # MongoDB 데이터 구성
        post_data = {
            "cr_at": values[0],
            "author": values[1],
            "content": values[2],
            "post_url": post_url,
            "crawl_date": datetime.now(timezone(timedelta(hours=9))),  # KST 기준
            "09_feed": "",
            "09_brand": "",
            "09_item": "",
            "09_item_category": "",
            "09_item_category_2": "",
            "open_date": "",
            "end_date": "",
            "processed": False
        }
        
        # MongoDB에 데이터 저장
        collection.insert_one(post_data)
        print(f"\n새로운 게시물이 MongoDB에 저장되었습니다: {post_url}")
        return True

    except Exception as e:
        print(f"MongoDB 저장 중 오류 발생: {str(e)}")
        return False

def clear_chrome_data(user_data_dir, keep_login=True):
    default_dir = os.path.join(user_data_dir, 'Default')
    if not os.path.exists(default_dir):
        print("Default 디렉토리가 존재하지 않습니다.")
        return

    dirs_to_clear = ['Cache', 'Code Cache', 'GPUCache']
    files_to_clear = ['History', 'Visited Links', 'Web Data']
    
    for dir_name in dirs_to_clear:
        dir_path = os.path.join(default_dir, dir_name)
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
            print(f"{dir_name} 디렉토리를 삭제했습니다.")

    if not keep_login:
        files_to_clear.extend(['Cookies', 'Login Data'])

    for file_name in files_to_clear:
        file_path = os.path.join(default_dir, file_name)
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"{file_name} 파일을 삭제했습니다.")

def is_within_period(date_str, weeks):
    try:
        post_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        period_ago = datetime.now(timezone.utc) - timedelta(weeks=weeks)
        return post_date >= period_ago
    except Exception as e:
        print(f"날짜 변환 중 오류 발생: {str(e)}")
        return False

def crawl_instagram_posts(driver, post_url, weeks, collection):
    try:
        # 게시물 카운터 초기화 (기간 내 모든 게시물 카운트)
        total_posts_in_period = 0
        
        # 첫 번째 피드 게시물이 로드될 때까지 대기
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div._aagv"))
        )
        
        # 잠시 대기
        time.sleep(3)
        
        # 첫 번째 게시물 찾기
        first_post = driver.find_element(By.CSS_SELECTOR, "div._aagv")
        
        # 부모 요소로 이동하여 링크 찾기
        parent = first_post.find_element(By.XPATH, "./ancestor::a")
        post_link = parent.get_attribute("href")
        
        # JavaScript로 첫 번째 게시물 클릭
        print(f"\n첫 번째 게시물({post_link})을 클릭합니다...")
        driver.execute_script("arguments[0].click();", parent)
        
        # 페이지 로딩 대기
        time.sleep(3)
        
        try:
            # 게시물 정보 추출
            post_data = {}
            
            # 작성자 ID 추출
            author_element = driver.find_element(By.CSS_SELECTOR, "a[role='link'][tabindex='0']")
            post_data['author'] = author_element.text
            
            # 본문 내용 추출
            content_element = driver.find_element(By.CSS_SELECTOR, "h1._ap3a._aaco._aacu._aacx._aad7._aade")
            post_data['content'] = content_element.text  # 전체 내용 저장
            
            # 게시 날짜 추출
            time_element = driver.find_element(By.CSS_SELECTOR, "time._a9ze._a9zf")
            post_date = time_element.get_attribute('datetime')
            
            # 처음 3개의 게시물은 핀고정 가능성 때문에 무조건 확인
            if total_posts_in_period < 3:
                total_posts_in_period += 1
                print(f"핀고정 가능성 있는 게시물 {total_posts_in_period}/3 확인 중...")
            else:
                # 4번째 게시물부터 기간 체크
                if not is_within_period(post_date, weeks):
                    print(f"\n{weeks}주 이전 게시물을 발견했습니다. 크롤링을 종료합니다.")
                    return total_posts_in_period
                total_posts_in_period += 1  # 기간 내 게시물 카운트 증가
            
            print(f"기간 내 총 게시물 수: {total_posts_in_period}")
            
            post_data['cr_at'] = post_date
            
            # 게시물 URL 저장 (파라미터 제거)
            post_url = clean_url(driver.current_url)
            post_data['post_url'] = post_url
            
            # 현재 시간 추가
            post_data['crawl_date'] = datetime.now(timezone.utc).isoformat()

            # 새로운 필드 추가
            post_data['09_feed'] = ""
            post_data['09_brand'] = ""
            post_data['09_item'] = ""
            post_data['09_item_category'] = ""
            post_data['09_item_category_2'] = ""
            post_data['open_date'] = ""
            post_data['end_date'] = ""
            post_data['processed'] = False
            
            # MongoDB에 데이터 저장
            if collection is not None:
                try:
                    # post_url로 중복 체크
                    existing_post = collection.find_one({"post_url": post_url})
                    if existing_post:
                        print(f"\n이미 존재하는 게시물입니다: {post_url}")
                    else:
                        collection.insert_one(post_data)
                        print(f"\n첫 번째 피드 정보가 MongoDB에 저장되었습니다.")
                except Exception as e:
                    print(f"MongoDB 저장 중 오류 발생: {str(e)}")
            
            # 다음 피드로 이동 (1주일 이내의 모든 피드)
            i = 1
            while True:  # 무한 루프로 변경
                # 게시물 100개 제한 체크
                if total_posts_in_period >= 100:
                    print("\n100개의 게시물을 확인했습니다. 다음 계정으로 넘어갑니다.")
                    return total_posts_in_period
                    
                try:
                    # 현재 URL 저장
                    current_url = driver.current_url
                    
                    # 다음 버튼 찾기
                    next_button = None
                    selector = "//span[contains(@style, 'rotate(90deg)')]/.."  # 90도 회전된 화살표(다음 버튼)의 부모 요소
                    
                    print("\n다음 버튼 찾는 중...")
                    try:
                        next_button = driver.find_element(By.XPATH, selector)
                        if next_button.is_displayed():
                            print("다음 버튼을 찾았습니다.")
                    except Exception as e:
                        print(f"다음 버튼을 찾을 수 없습니다: {str(e)}")
                        break
                    
                    if next_button is None:
                        print(f"{i+1}번째 피드로 이동할 수 없습니다. 다음 버튼을 찾을 수 없습니다.")
                        break
                    
                    print(f"\n{i+1}번째 피드로 이동합니다...")
                    driver.execute_script("arguments[0].click();", next_button)
                    
                    # 실제 사람처럼 랜덤한 시간 대기 (정규 분포 사용)
                    wait_time = abs(random.gauss(3.5, 2))  # 평균 6초, 표준편차 4초
                    # 최소 0.5초, 최대 50초로 제한
                    wait_time = max(0.5, min(wait_time, 30.0))
                    print(f"다음 피드 로딩 대기 중... ({wait_time:.1f}초)")
                    time.sleep(wait_time)
                    
                    # URL이 변경되었는지 확인
                    if driver.current_url == current_url:
                        print(f"{i+1}번째 피드로 이동하지 못했습니다. URL이 변경되지 않았습니다.")
                        print("현재 URL:", driver.current_url)
                        print("이전 URL:", current_url)
                        break
                    
                    # 다음 피드 정보 추출
                    next_post_data = {}
                    
                    # 작성자 ID 추출
                    author_element = driver.find_element(By.CSS_SELECTOR, "a[role='link'][tabindex='0']")
                    next_post_data['author'] = author_element.text
                    
                    # 본문 내용 추출
                    content_element = driver.find_element(By.CSS_SELECTOR, "h1._ap3a._aaco._aacu._aacx._aad7._aade")
                    next_post_data['content'] = content_element.text  # 전체 내용 저장
                    
                    # 게시 날짜 추출
                    time_element = driver.find_element(By.CSS_SELECTOR, "time._a9ze._a9zf")
                    post_date = time_element.get_attribute('datetime')
                    
                    # 처음 3개의 게시물은 핀고정 가능성 때문에 무조건 다음으로 넘어감
                    if total_posts_in_period < 3:
                        total_posts_in_period += 1
                        print(f"핀고정 가능성 있는 게시물 {total_posts_in_period}/3 확인 중...")
                        # 기간 내 게시물인 경우에만 MongoDB에 저장
                        if is_within_period(post_date, weeks):
                            next_post_data['cr_at'] = post_date
                            next_post_data['post_url'] = clean_url(driver.current_url)
                            next_post_data['crawl_date'] = datetime.now(timezone.utc).isoformat()
                            next_post_data['09_feed'] = ""
                            next_post_data['09_brand'] = ""
                            next_post_data['09_item'] = ""
                            next_post_data['09_item_category'] = ""
                            next_post_data['09_item_category_2'] = ""
                            next_post_data['open_date'] = ""
                            next_post_data['end_date'] = ""
                            next_post_data['processed'] = False
                            
                            try:
                                # MongoDB에 저장 시도
                                if collection is not None:
                                    existing_post = collection.find_one({"post_url": next_post_data['post_url']})
                                    if existing_post:
                                        print(f"\n이미 존재하는 게시물입니다: {next_post_data['post_url']}")
                                    else:
                                        collection.insert_one(next_post_data)
                                        print(f"기간 내 게시물이라 MongoDB에 저장했습니다: {next_post_data['post_url']}")
                            except Exception as e:
                                print(f"MongoDB 저장 중 오류 발생: {str(e)}")
                        else:
                            print(f"기간 외 게시물이라 MongoDB에 저장하지 않습니다.")
                    else:
                        # 4번째 게시물부터 기간 체크
                        if not is_within_period(post_date, weeks):
                            print(f"\n{weeks}주 이전 게시물을 발견했습니다. 크롤링을 종료합니다.")
                            return total_posts_in_period
                        total_posts_in_period += 1  # 기간 내 게시물 카운트 증가
                        
                        # MongoDB에 저장할 데이터 준비
                        next_post_data['cr_at'] = post_date
                        next_post_data['post_url'] = clean_url(driver.current_url)
                        next_post_data['crawl_date'] = datetime.now(timezone.utc).isoformat()
                        next_post_data['09_feed'] = ""
                        next_post_data['09_brand'] = ""
                        next_post_data['09_item'] = ""
                        next_post_data['09_item_category'] = ""
                        next_post_data['09_item_category_2'] = ""
                        next_post_data['open_date'] = ""
                        next_post_data['end_date'] = ""
                        next_post_data['processed'] = False
                        
                        try:
                            # MongoDB에 저장 시도
                            if collection is not None:
                                existing_post = collection.find_one({"post_url": next_post_data['post_url']})
                                if existing_post:
                                    print(f"\n이미 존재하는 게시물입니다: {next_post_data['post_url']}")
                                else:
                                    collection.insert_one(next_post_data)
                                    print(f"MongoDB에 저장했습니다: {next_post_data['post_url']}")
                        except Exception as e:
                            print(f"MongoDB 저장 중 오류 발생: {str(e)}")
                    
                    print(f"기간 내 총 게시물 수: {total_posts_in_period}")
                    
                    i += 1  # 카운터 증가
                    
                except Exception as e:
                    print(f"{i+1}번째 피드로 이동하는 중 오류 발생: {str(e)}")
                    break
            
        except Exception as e:
            print(f"게시물 정보를 추출하는 중 오류 발생: {str(e)}")
            print("현재 페이지 소스:")
            print(driver.page_source[:500])  # 페이지 소스의 일부를 출력하여 디버깅

    except Exception as e:
        print(f"게시물을 클릭하는 중 오류 발생: {str(e)}")
        print("현재 페이지 소스:")
        print(driver.page_source[:500])  # 페이지 소스의 일부를 출력하여 디버깅

    return total_posts_in_period

def update_crawl_date(service, spreadsheet_id, username, post_count, error_log=None):
    """Google Sheets의 Date 칼럼과 Count 칼럼, Log 칼럼에 크롤링 날짜와 게시물 수, 에러 로그 업데이트"""
    try:
        # 전체 데이터 가져오기
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range='A:G'  # A열: Username, B열: Category, E열: Date, F열: Count, G열: Log
        ).execute()
        values = result.get('values', [])

        # username이 있는 행 찾기
        row_number = None
        for i, row in enumerate(values):
            if row and row[0] == username:
                row_number = i + 1  # 1-based index
                break

        if row_number:
            # 현재 날짜 포맷팅 (KST)
            kst = timezone(timedelta(hours=9))
            current_date = datetime.now(kst).strftime('%Y-%m-%d')

            # Date 칼럼(E열), Count 칼럼(F열), Log 칼럼(G열) 업데이트
            range_name = f'E{row_number}:G{row_number}'
            body = {
                'values': [[current_date, post_count, error_log if error_log else '']]
            }
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
            print(f"\n{username}의 크롤링 날짜가 {current_date}로, 게시물 수가 {post_count}개로 업데이트되었습니다.")
            if error_log:
                print(f"에러 로그가 기록되었습니다: {error_log}")
        else:
            print(f"\n{username}을 스프레드시트에서 찾을 수 없습니다.")

    except Exception as e:
        print(f"\n크롤링 날짜와 게시물 수 업데이트 중 오류 발생: {str(e)}")

def check_already_crawled(service, spreadsheet_id, username):
    """해당 username의 Date 칼럼 확인"""
    try:
        # 전체 데이터 가져오기
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range='A:E'  # A열: Username, B열: Category, E열: Date
        ).execute()
        values = result.get('values', [])

        # username이 있는 행 찾기
        for row in values:
            if row and row[0] == username:
                # Date 칼럼에 값이 있는지 확인 (E열)
                if len(row) > 4 and row[4].strip():  # E열(인덱스 4)에 값이 있는지 확인
                    category = row[1] if len(row) > 1 else "카테고리 정보 없음"
                    print(f"\n{username} 계정은 이미 {category}에 크롤링되었습니다. 건너뜁니다.")
                    return True, row[4]  # 이미 크롤링됨, 날짜 반환
                return False, None  # 크롤링 안됨
    except Exception as e:
        print(f"\nDate 칼럼 확인 중 오류 발생: {str(e)}")
        return False, None

    return False, None

def update_reels_views(collection_influencer, username, average_views):
    """인플루언서 컬렉션의 reels_views(15) 필드를 업데이트합니다."""
    try:
        # 정수로 변환
        views_int = int(average_views)
        
        # username으로 도큐먼트 찾아서 업데이트
        result = collection_influencer.update_one(
            {"username": username},
            {"$set": {"reels_views(15)": views_int}}
        )
        
        if result.matched_count > 0:
            print(f"\n{username}의 reels_views(15) 필드가 {views_int:,}회로 업데이트되었습니다.")
        else:
            print(f"\n{username}을 인플루언서 컬렉션에서 찾을 수 없습니다.")
            
    except Exception as e:
        print(f"\nreels_views 업데이트 중 오류 발생: {str(e)}")

def take_break(username_count):
    """
    크롤링 중 휴식 시간을 관리하는 함수
    
    Args:
        username_count (int): 현재까지 처리한 username의 수
    """
    def show_countdown(seconds, break_type):
        """카운트다운을 보여주는 내부 함수"""
        start_time = time.time()
        while True:
            elapsed_time = int(time.time() - start_time)
            remaining = seconds - elapsed_time
            if remaining <= 0:
                break
            
            if break_type == "중간":
                mins, secs = divmod(remaining, 60)
                countdown = f"\r{break_type} 휴식 중: {mins}분 {secs}초 남음...     "
            else:  # "대규모"
                hours, remainder = divmod(remaining, 3600)
                mins, secs = divmod(remainder, 60)
                countdown = f"\r{break_type} 휴식 중: {hours}시간 {mins}분 {secs}초 남음...     "
            
            print(countdown, end='', flush=True)
            time.sleep(1)
        print("\r휴식 완료!            ")  # 카운트다운 종료 후 줄 정리

    # 중간 휴식 (15-25개 username마다)
    if username_count % random.randint(15, 25) == 0:
        break_time = random.randint(60, 720)  # 1-12분
        print(f"\n중간 휴식 시작 (총 {break_time//60}분 {break_time%60}초)...")
        show_countdown(break_time, "중간")

    # 대규모 휴식 (80-100개 username마다)
    if username_count % random.randint(80, 100) == 0:
        break_time = random.randint(1800, 7200)  # 30분-2시간
        hours = break_time // 3600
        minutes = (break_time % 3600) // 60
        print(f"\n대규모 휴식 시작 (총 {hours}시간 {minutes}분)...")
        show_countdown(break_time, "대규모")

# 메인 실행 코드
def main():
    # Google Sheets API 설정
    SPREADSHEET_ID = '1RdnS9IsC1TbTi356J5W-Pb66oaJ7xUhVZr-pTlJTwxQ'
    RANGE_NAME = 'A:A'  # Username 필드가 있는 열 (Sheet 이름 제거)

    # Google Sheets API 인증 및 서비스 객체 생성
    creds = get_credentials()
    service = build('sheets', 'v4', credentials=creds)

    # 스프레드시트에서 데이터 가져오기
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    values = result.get('values', [])

    if not values:
        print('스프레드시트에서 데이터를 찾을 수 없습니다.')
        return

    # 헤더 제거 (첫 번째 행 건너뛰기)
    usernames = [row[0] for row in values[1:] if row]  # 빈 행 제외

    # 크롤링 기간 입력 받기
    while True:
        try:
            weeks = int(input("\n크롤링할 기간을 주 단위로 입력하세요 (예: 9주(63일) = 9): "))
            if weeks > 0:
                break
            print("1 이상의 숫자를 입력해주세요.")
        except ValueError:
            print("올바른 숫자를 입력해주세요.")

    # 로그인 정보 파일 경로 설정 (상대 경로 사용)
    login_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "0_insta_login.txt")
    with open(login_file_path, 'r', encoding='utf-8') as f:
        profile_name = f.read().strip()

    # 사용자 데이터 디렉토리 설정 (상대 경로 사용)
    user_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "user_data", profile_name)

    try:
        # MongoDB 연결
        collection_feed, collection_influencer = connect_mongodb()
        if collection_feed is None and collection_influencer is None:
            print("MongoDB 연결 실패. 프로그램을 종료합니다.")
            return

        # 이미 처리된 게시물 URL 로드
        processed_posts = load_processed_posts(collection_feed)
        print("이미 처리된 게시물 URL 로드 완료")

        # 각 사용자명에 대해 크롤링 수행
        for username in usernames:
            try:
                # 이미 크롤링된 계정인지 확인
                already_crawled, crawl_date = check_already_crawled(service, SPREADSHEET_ID, username)
                if already_crawled:
                    print(f"\n{username} 계정은 이미 {crawl_date}에 크롤링되었습니다. 건너뜁니다.")
                    continue

                # Chrome 옵션 설정
                options = Options()
                options.add_argument("--start-maximized")
                options.add_experimental_option("detach", True)
                options.add_argument("disable-blink-features=AutomationControlled")
                options.add_experimental_option("excludeSwitches", ["enable-logging"])
                options.add_argument(f"user-data-dir={user_data_dir}")
                options.add_argument("--disable-application-cache")
                options.add_argument("--disable-cache")

                # 캐시와 임시 파일 정리 (로그인 정보 유지)
                clear_chrome_data(user_data_dir)

                # 새로운 Chrome 드라이버 시작
                driver = webdriver.Chrome(options=options)

                print(f"\n{username} 계정 크롤링을 시작합니다...")
                profile_url = f"https://www.instagram.com/{username}/"
                print(f"\n프로필 URL({profile_url})로 이동합니다...")
                driver.get(profile_url)
                
                # 프로필 페이지의 주요 요소가 로드될 때까지 대기
                try:
                    # 프로필 이미지나 게시물 그리드가 로드될 때까지 대기
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "div._aagv"))
                    )
                    print("프로필 페이지가 성공적으로 로드되었습니다.")
                except Exception as e:
                    print(f"프로필 페이지 로딩 중 오류 발생: {str(e)}")
                    raise

                # 크롤링 실행 및 게시물 수 받기
                post_count = crawl_instagram_posts(driver, username, weeks, collection_feed)
                
                # 릴스 분석 수행
                print(f"\n{username} 계정의 릴스 분석을 시작합니다...")
                reels_analyzer = ReelsAnalyzer(driver)  # 기존 드라이버 재사용
                reels_result = reels_analyzer.analyze_reels_views(f"https://www.instagram.com/{username}/")
                
                print(f"\n[릴스 분석 결과]")
                print(f"평균 조회수: {int(reels_result['average_views']):,}회")
                print(f"전체 릴스 수: {reels_result['total_reels']}개")
                print(f"계산 방식: {reels_result['calculation_method']}")
                
                # 릴스 조회수 업데이트
                update_reels_views(collection_influencer, username, reels_result['average_views'])
                
                # 크롤링 날짜와 게시물 수 업데이트
                update_crawl_date(service, SPREADSHEET_ID, username, post_count)
                
                # 브라우저 종료
                print(f"\n{username} 계정 크롤링 완료. 브라우저를 종료합니다.")
                driver.quit()
                
                # 계정 간 크롤링 딜레이
                wait_time = random.uniform(10, 20)
                print(f"\n다음 계정으로 이동하기 전 {wait_time:.1f}초 대기...")
                time.sleep(wait_time)

            except Exception as e:
                error_message = f"{datetime.now(timezone(timedelta(hours=9))).strftime('%Y-%m-%d %H:%M:%S')} - {str(e)}"
                print(f"{username} 계정 크롤링 중 오류 발생: {error_message}")
                # 에러 발생 시 로그 기록
                update_crawl_date(service, SPREADSHEET_ID, username, 0, error_message)
                if 'driver' in locals():
                    driver.quit()
                continue

            # 휴식 시간 관리
            take_break(usernames.index(username) + 1)

    except Exception as e:
        print(f"크롤링 중 오류 발생: {str(e)}")
    finally:
        print("\n모든 계정의 크롤링이 완료되었습니다.")
        input("프로그램을 종료하려면 엔터를 누르세요...")

if __name__ == "__main__":
    main()
