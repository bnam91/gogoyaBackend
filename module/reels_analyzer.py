from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time
import numpy as np

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

# 사용 예시
if __name__ == "__main__":
    # 단독 실행 예시
    analyzer = ReelsAnalyzer()
    result = analyzer.analyze_reels_views("https://www.instagram.com/example_user/")
    print(f"평균 조회수: {int(result['average_views']):,}회")
    print(f"전체 릴스 수: {result['total_reels']}개")
    print(f"계산 방식: {result['calculation_method']}")
    analyzer.close() 