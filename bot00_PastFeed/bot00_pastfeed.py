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
import random  # 상단에 추가

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

def crawl_instagram_posts(driver, post_url, weeks):
    try:
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
            
            # 입력받은 기간 이내의 게시물인지 확인
            if not is_within_period(post_date, weeks):
                print(f"\n{weeks}주 이전 게시물을 발견했습니다. 크롤링을 종료합니다.")
                return
            
            post_data['cr_at'] = post_date
            
            # 게시물 URL 저장
            post_data['post_url'] = driver.current_url
            
            # 현재 시간 추가
            post_data['crawl_date'] = datetime.now(timezone.utc).isoformat()
            
            # JSON 파일명 설정 (계정명 포함)
            filename = f"post_data_{post_url}.json"
            
            # 기존 데이터 읽기
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    all_posts = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                all_posts = []
            
            # 첫 번째 피드 정보 추가
            all_posts.append(post_data)
            
            # 전체 데이터 저장
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(all_posts, f, ensure_ascii=False, indent=2)
            print(f"\n첫 번째 게시물 정보가 {filename}에 추가되었습니다.")
            
            # 다음 피드로 이동 (1주일 이내의 모든 피드)
            i = 1
            while True:  # 무한 루프로 변경
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
                    
                    # 실제 사람처럼 랜덤한 시간 대기
                    wait_time = random.uniform(0.5, 8.0)  # 2~4초 사이 랜덤 대기
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
                    
                    # 입력받은 기간 이내의 게시물인지 확인
                    if not is_within_period(post_date, weeks):
                        print(f"\n{weeks}주 이전 게시물을 발견했습니다. 크롤링을 종료합니다.")
                        return
                    
                    next_post_data['cr_at'] = post_date
                    
                    # 게시물 URL 저장
                    next_post_data['post_url'] = driver.current_url
                    
                    # 현재 시간 추가
                    next_post_data['crawl_date'] = datetime.now(timezone.utc).isoformat()
                    
                    # 기존 데이터에 다음 피드 정보 추가
                    all_posts.append(next_post_data)
                    
                    # 전체 데이터 저장
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(all_posts, f, ensure_ascii=False, indent=2)
                    print(f"{i+1}번째 피드 정보가 {filename}에 추가되었습니다.")
                    
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

# 메인 실행 코드
# Chrome 옵션 설정
options = Options()
options.add_argument("--start-maximized")
options.add_experimental_option("detach", True)
options.add_argument("disable-blink-features=AutomationControlled")
options.add_experimental_option("excludeSwitches", ["enable-logging"])

# 크롤링 기간 입력 받기
while True:
    try:
        weeks = int(input("\n크롤링할 기간을 주 단위로 입력하세요 (예: 4주 = 4): "))
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
options.add_argument(f"user-data-dir={user_data_dir}")

# 캐시와 임시 파일 정리 (로그인 정보 유지)
clear_chrome_data(user_data_dir)

# 추가 옵션 설정
options.add_argument("--disable-application-cache")
options.add_argument("--disable-cache")

# Chrome 드라이버 실행
driver = webdriver.Chrome(options=options)

# 인스타그램 프로필 URL 설정
post_url = "thanks_kim"  # 인스타그램 아이디 변수
profile_url = f"https://www.instagram.com/{post_url}/"
print(f"\n프로필 URL({profile_url})로 이동합니다...")
driver.get(profile_url)
time.sleep(5)  # 페이지 로딩 대기 시간

# 크롤링 실행
crawl_instagram_posts(driver, post_url, weeks)

# 브라우저 창 유지
input("프로그램을 종료하려면 엔터를 누르세요...")
driver.quit()
