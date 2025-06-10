#https://docs.google.com/spreadsheets/d/1RdnS9IsC1TbTi356J5W-Pb66oaJ7xUhVZr-pTlJTwxQ/edit?gid=0#gid=0

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
import os
import shutil
import numpy as np
from datetime import datetime
import sys

# auth.py 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from auth import get_credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

class ReelsAnalyzer:
    def __init__(self, driver=None):
        """
        릴스 분석기 초기화
        
        Args:
            driver: 기존 Selenium WebDriver 인스턴스 (선택사항)
        """
        self.driver = driver
        
    def setup_driver(self):
        """자체 WebDriver 설정 (driver가 제공되지 않은 경우)"""
        if not self.driver:
            options = Options()
            options.add_argument("--start-maximized")
            options.add_experimental_option("detach", True)
            options.add_argument("disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-logging"])
            self.driver = webdriver.Chrome(options=options)
            return True
        return False

    def analyze_reels_views(self, profile_url, scroll_attempts=5, wait_time=20):
        """
        특정 인스타그램 프로필의 릴스 조회수를 분석합니다.
        
        Args:
            profile_url (str): 인스타그램 프로필 URL
            scroll_attempts (int): 스크롤 시도 횟수
            wait_time (int): 페이지 로딩 대기 시간(초)
            
        Returns:
            dict: {
                'average_views': float,  # 평균 조회수
                'total_reels': int,      # 전체 릴스 수
                'views_list': list,      # 개별 릴스 조회수 목록
                'calculation_method': str # 계산 방식 설명
            }
        """
        driver_created = self.setup_driver()
        
        try:
            # 릴스 페이지로 이동
            reels_url = profile_url.rstrip('/') + '/reels/'
            self.driver.get(reels_url)
            
            views_list = []
            wait = WebDriverWait(self.driver, wait_time)
            
            try:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "._aajy span.html-span")))
            except:
                print("릴스가 없거나 페이지 로딩에 실패했습니다.")
                return {
                    'average_views': 0,
                    'total_reels': 0,
                    'views_list': [],
                    'calculation_method': '릴스 없음'
                }

            # 스크롤 수행
            for _ in range(scroll_attempts):
                view_elements = self.driver.find_elements(By.CSS_SELECTOR, "._aajy span.html-span")
                if not view_elements:
                    break
                    
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(5)
            
            # 릴스 조회수 수집
            view_elements = self.driver.find_elements(By.CSS_SELECTOR, "._aajy span.html-span")
            
            for element in view_elements:
                try:
                    text = element.text
                    if '만' in text:
                        number = float(text.replace('만', '')) * 10000
                        views_list.append(int(number))
                    else:
                        number = int(text.replace(',', ''))
                        views_list.append(number)
                except Exception as e:
                    continue
            
            # 조회수 처리 로직
            if len(views_list) == 0:
                return {
                    'average_views': 0,
                    'total_reels': 0,
                    'views_list': [],
                    'calculation_method': '릴스 없음'
                }
            elif len(views_list) <= 5:
                # 전체 평균 계산
                avg_views = sum(views_list) / len(views_list)
                calculation_method = f'전체 {len(views_list)}개 릴스의 단순 평균'
            else:
                # 상위 30%와 하위 20%를 제외한 평균 계산
                sorted_views = sorted(views_list)
                top_cut = int(len(sorted_views) * 0.3)  # 상위 30%
                bottom_cut = int(len(sorted_views) * 0.2)  # 하위 20%
                
                # 상위 30%와 하위 20% 제외
                trimmed_views = sorted_views[bottom_cut:-top_cut] if top_cut > 0 and bottom_cut > 0 else sorted_views
                avg_views = sum(trimmed_views) / len(trimmed_views)
                calculation_method = f'전체 {len(views_list)}개 중 상위 {top_cut}개(30%)와 하위 {bottom_cut}개(20%)를 제외한 {len(trimmed_views)}개의 평균'
            
            return {
                'average_views': avg_views,
                'total_reels': len(views_list),
                'views_list': views_list,
                'calculation_method': calculation_method
            }
            
        finally:
            if driver_created:
                self.driver.quit()

    def close(self):
        """WebDriver 종료"""
        if self.driver:
            self.driver.quit()

def clear_chrome_data(user_data_dir, keep_login=True):
    """Chrome 캐시 및 임시 파일 정리"""
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

def get_or_create_sheet(service, username):
    """username으로 시트를 찾거나 생성"""
    try:
        spreadsheet_id = '1dV5of0RVmG7pzyJHu68kWcxdTjDz4R-uO9t0vdFVhbg'
        
        # 현재 스프레드시트의 모든 시트 정보 가져오기
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = spreadsheet.get('sheets', [])
        
        # username으로 된 시트가 있는지 확인
        sheet_exists = False
        for sheet in sheets:
            if sheet['properties']['title'] == username:
                sheet_exists = True
                break
        
        # 시트가 없으면 생성
        if not sheet_exists:
            request = {
                'addSheet': {
                    'properties': {
                        'title': username
                    }
                }
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': [request]}
            ).execute()
            
            # 헤더 추가
            range_name = f'{username}!A1:C1'
            values = [['조회수', '링크', '수집일시']]
            body = {
                'values': values
            }
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
        
        return spreadsheet_id
        
    except HttpError as error:
        print(f'시트 확인/생성 중 오류 발생: {error}')
        return None

def calculate_average_views(reels_data):
    """상위 10% 하위 15%를 제외한 평균 조회수 계산"""
    if not reels_data:
        return 0
    
    # 조회수만 추출하여 정렬
    views = sorted([reel['views'] for reel in reels_data])
    
    # 상위 10%와 하위 15% 제외
    top_cut = int(len(views) * 0.1)  # 상위 10%
    bottom_cut = int(len(views) * 0.15)  # 하위 15%
    
    # 상위 10%와 하위 15% 제외한 조회수
    trimmed_views = views[bottom_cut:-top_cut] if top_cut > 0 and bottom_cut > 0 else views
    
    # 평균 계산
    avg_views = sum(trimmed_views) / len(trimmed_views)
    return int(avg_views)

def save_to_sheet(service, username, reels_data):
    """데이터를 시트에 저장"""
    try:
        # 평균 조회수 계산
        avg_views = calculate_average_views(reels_data)
        print(f"\n평균 조회수: {avg_views:,}")
        
        # 데이터 준비
        values = []
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for reel in reels_data:
            values.append([
                reel['views'],
                reel['link'],
                current_time
            ])
        
        # 데이터 저장
        spreadsheet_id = '1dV5of0RVmG7pzyJHu68kWcxdTjDz4R-uO9t0vdFVhbg'
        range_name = f'{username}!A2:C'
        body = {
            'values': values
        }
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        # 평균 이상인 행에 배경색 적용
        requests = []
        for i, reel in enumerate(reels_data, start=2):  # 2부터 시작 (헤더 다음 행)
            if reel['views'] > avg_views:
                requests.append({
                    'repeatCell': {
                        'range': {
                            'sheetId': get_sheet_id(service, spreadsheet_id, username),
                            'startRowIndex': i-1,
                            'endRowIndex': i,
                            'startColumnIndex': 0,
                            'endColumnIndex': 3
                        },
                        'cell': {
                            'userEnteredFormat': {
                                'backgroundColor': {
                                    'red': 0.9,
                                    'green': 0.9,
                                    'blue': 0.9
                                }
                            }
                        },
                        'fields': 'userEnteredFormat.backgroundColor'
                    }
                })
        
        if requests:
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            ).execute()
        
        print(f"\n데이터가 성공적으로 저장되었습니다.")
        print(f"스프레드시트 ID: {spreadsheet_id}")
        print(f"시트 이름: {username}")
        print(f"평균 조회수({avg_views:,}) 이상인 게시글은 배경색이 표시됩니다.")
        
    except HttpError as error:
        print(f'데이터 저장 중 오류 발생: {error}')

def get_sheet_id(service, spreadsheet_id, sheet_name):
    """시트 이름으로 시트 ID 가져오기"""
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        for sheet in spreadsheet.get('sheets', []):
            if sheet['properties']['title'] == sheet_name:
                return sheet['properties']['sheetId']
        return None
    except HttpError as error:
        print(f'시트 ID 조회 중 오류 발생: {error}')
        return None

def main():
    # username 입력 받기
    username = input("\n크롤링할 인스타그램 username을 입력하세요: ").strip()

    # Google Sheets API 설정
    creds = get_credentials()
    service = build('sheets', 'v4', credentials=creds)
    
    # username으로 시트 확인/생성
    spreadsheet_id = get_or_create_sheet(service, username)
    if not spreadsheet_id:
        print("시트 확인/생성에 실패했습니다.")
        return

    # 로그인 정보 파일 경로 설정
    login_file_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "0_insta_login.txt")
    with open(login_file_path, 'r', encoding='utf-8') as f:
        profile_name = f.read().strip()

    # 사용자 데이터 디렉토리 설정
    user_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "user_data", profile_name)

    try:
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

        # Chrome 드라이버 시작
        driver = webdriver.Chrome(options=options)

        print(f"\n{username} 계정의 릴스 분석을 시작합니다...")
        profile_url = f"https://www.instagram.com/{username}/"
        print(f"\n프로필 URL({profile_url})로 이동합니다...")
        driver.get(profile_url)
        
        # 프로필 페이지 로딩 대기
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div._aagv"))
            )
            print("프로필 페이지가 성공적으로 로드되었습니다.")
        except Exception as e:
            print(f"프로필 페이지 로딩 중 오류 발생: {str(e)}")
            raise

        # 릴스 탭으로 이동
        try:
            reels_tab = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//span[text()='릴스']"))
            )
            reels_tab.click()
            print("릴스 탭으로 이동했습니다.")
            time.sleep(3)
        except Exception as e:
            print(f"릴스 탭 이동 중 오류 발생: {str(e)}")
            raise

        # 릴스 정보 수집
        reels_data = []
        last_height = driver.execute_script("return document.body.scrollHeight")
        processed_links = set()
        
        print("\n[실시간 크롤링 시작]")
        print("=" * 50)
        print("상위 100개의 릴스만 크롤링합니다.")
        print("=" * 50)
        
        while len(reels_data) < 100:  # 100개로 수정
            # 릴스 요소들 찾기
            reels = driver.find_elements(By.CSS_SELECTOR, "div._aajy")
            
            for reel in reels:
                if len(reels_data) >= 100:  # 100개로 수정
                    break
                    
                try:
                    # 조회수 추출
                    views = None
                    views_element = reel.find_element(By.CSS_SELECTOR, "span.html-span")
                    views_text = views_element.text
                    
                    if '만' in views_text:
                        views = int(float(views_text.replace('만', '')) * 10000)
                    elif '회' in views_text or 'views' in views_text.lower():
                        views = int(views_text.replace('회', '').replace('views', '').replace(',', '').strip())
                    elif views_text.isdigit():
                        views = int(views_text)
                    
                    if not views:
                        print("\n[스킵] 조회수를 찾을 수 없습니다.")
                        continue
                    
                    # 링크 추출
                    link_element = reel.find_element(By.XPATH, "./ancestor::a")
                    current_url = link_element.get_attribute('href')
                    
                    if current_url in processed_links:
                        continue
                    
                    processed_links.add(current_url)
                    
                    # 데이터 저장 및 출력
                    reel_info = {
                        'views': int(views),
                        'link': current_url
                    }
                    
                    reels_data.append(reel_info)
                    
                    print("\n" + "=" * 50)
                    print(f"[{len(reels_data)}번째 릴스]")
                    print(f"조회수: {int(views):,}")
                    print(f"링크: {current_url}")
                    print("=" * 50)

                except Exception as e:
                    print(f"\n[오류] 릴스 정보 추출 중 오류 발생: {str(e)}")
                    continue
            
            if len(reels_data) >= 100:  # 100개 수집 완료시 종료
                break
            
            # 스크롤 다운
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        print("\n" + "=" * 50)
        print(f"[크롤링 완료] 총 {len(reels_data)}개의 릴스 정보를 수집했습니다.")
        print("=" * 50)
        
        # 데이터를 시트에 저장
        save_to_sheet(service, username, reels_data)
        
        print(f"\n{username} 계정 릴스 분석이 완료되었습니다.")
        print("크롬 창은 자동으로 종료되지 않습니다. 직접 닫아주세요.")

    except Exception as e:
        print(f"릴스 분석 중 오류 발생: {str(e)}")
    finally:
        print("\n프로그램을 종료하려면 엔터를 누르세요...")
        input()

if __name__ == "__main__":
    main()
