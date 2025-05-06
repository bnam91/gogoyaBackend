"""
브라우저 자동화 유틸리티 모듈

이 모듈은 셀레니움을 사용한 웹 브라우저 자동화 기능을 제공합니다.
주요 기능:
- 크롬 브라우저 캐시 데이터 정리
- 브라우저 환경 설정 및 초기화
- 웹페이지 자동 스크롤 기능
- 스마트스토어 상품 목록에서 특정 브랜드명 포함 여부 확인

사용 기술:
- Selenium: 웹 브라우저 자동화
- BeautifulSoup: HTML 파싱
- 파일 시스템 조작: 브라우저 프로필 관리

작성자: 미상
버전: 1.0
"""

import os
import shutil
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
import logging
import re
from datetime import datetime
import base64  # base64 인코딩을 위해 추가
import sys  # 경로 추가를 위해 필요

# 모듈 디렉토리를 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# imgbb 모듈 가져오기
from module.imgbb import upload_image_to_imgbb

def clear_chrome_data(user_data_dir):
    """Chrome 사용자 데이터 디렉토리 캐시 정리"""
    if not os.path.exists(user_data_dir):
        print(f"사용자 데이터 디렉토리가 존재하지 않습니다: {user_data_dir}")
        return
    
    try:
        # 캐시, 쿠키 폴더 등 삭제
        subdirs_to_clear = [
            'Cache', 'Cookies', 'GPUCache', 
            'Service Worker', 'Storage', 'Code Cache'
        ]
        
        for subdir in subdirs_to_clear:
            full_path = os.path.join(user_data_dir, subdir)
            if os.path.exists(full_path):
                try:
                    shutil.rmtree(full_path)
                    print(f"{full_path} 디렉토리 삭제 완료")
                except Exception as e:
                    print(f"{full_path} 삭제 실패: {e}")
                    
    except Exception as e:
        print(f"Chrome 데이터 정리 중 오류 발생: {e}")

def setup_browser(options):
    """브라우저 설정"""
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    
    # 기본 옵션 설정
    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("detach", True)
    
    # 사용자 지정 옵션 추가
    for option in options:
        chrome_options.add_argument(option)
    
    # 브라우저 생성
    browser = webdriver.Chrome(options=chrome_options)
    
    return browser

def scroll_to_bottom(browser):
    """페이지 끝까지 스크롤"""
    logger = logging.getLogger()
    logger.info("페이지를 끝까지 스크롤합니다...")
    
    # 처음 문서 높이 가져오기
    last_height = browser.execute_script("return document.body.scrollHeight")
    
    while True:
        # 스크롤 다운
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # 페이지 로딩 대기
        time.sleep(2)
        
        # 새 문서 높이 가져와서 이전과 비교
        new_height = browser.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    
    logger.info("스크롤 완료")

def take_screenshot(browser, brand_name=None):
    """현재 페이지의 스크린샷을 찍고 ImgBB에 업로드한 후 URL을 반환합니다"""
    logger = logging.getLogger()
    
    try:
        # 브랜드명으로 로깅용 식별자 생성
        brand_identifier = f"'{brand_name}'" if brand_name else "페이지"
        logger.info(f"{brand_identifier} 스크린샷을 ImgBB에 업로드 중...")
        
        # 스크린샷을 임시 파일 대신 메모리에 바로 저장
        # PNG 형식으로 바이너리 데이터 가져오기
        screenshot_binary = browser.get_screenshot_as_png()
        
        # 바이너리 데이터를 base64로 인코딩
        img_base64 = base64.b64encode(screenshot_binary).decode('ascii')
        
        # ImgBB에 업로드
        try:
            image_url = upload_image_to_imgbb(img_base64)
            logger.info(f"스크린샷이 ImgBB에 업로드되었습니다: {image_url}")
            return image_url
        except Exception as e:
            logger.error(f"ImgBB 업로드 실패: {e}")
            
            # 실패 시 로컬에 백업으로 저장
            script_dir = os.path.dirname(os.path.abspath(__file__))
            screenshots_dir = os.path.join(script_dir, 'screenshots')
            os.makedirs(screenshots_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if brand_name:
                brand_name = re.sub(r'[\\/*?:"<>|]', "", brand_name)
                filename = os.path.join(screenshots_dir, f"{brand_name}_{timestamp}.png")
            else:
                filename = os.path.join(screenshots_dir, f"screenshot_{timestamp}.png")
            
            with open(filename, 'wb') as f:
                f.write(screenshot_binary)
            logger.info(f"ImgBB 업로드 실패로 인해 로컬에 저장: {filename}")
            return filename
    
    except Exception as e:
        logger.error(f"스크린샷 처리 중 오류 발생: {e}")
        return None

def check_product_brand_match(browser, brand_name):
    """상품 페이지에서 브랜드명 확인"""
    logger = logging.getLogger()
    logger.info(f"상품명에 브랜드명 '{brand_name}' 포함 여부 확인 중...")
    
    # 페이지 소스 가져오기
    page_source = browser.page_source
    soup = BeautifulSoup(page_source, 'html.parser')
    
    # 상품 목록 찾기 (클래스 이름은 사이트마다 다를 수 있음)
    product_elements = soup.select('.product_item, ._product_info, .item, .product-item')
    
    # 상품명 추출
    product_names = []
    for element in product_elements:
        # 다양한 선택자 시도
        for selector in ['.product_name', '.product-name', '.title', 'strong', 'a']:
            product_name_element = element.select_one(selector)
            if product_name_element and product_name_element.text.strip():
                product_names.append(product_name_element.text.strip())
                break
    
    # 브랜드명 포함 상품 개수 계산
    brand_name_lower = brand_name.lower()
    products_with_brand = [
        name for name in product_names 
        if brand_name_lower in name.lower() or 
           any(word.lower() in name.lower() for word in brand_name_lower.split())
    ]
    
    # 결과 분석
    total_products = len(product_names)
    brand_products = len(products_with_brand)
    
    if total_products > 0:
        brand_ratio = brand_products / total_products * 100
        logger.info("\n분석 결과:")
        logger.info(f"- 총 상품 수: {total_products}개")
        logger.info(f"- '{brand_name}' 포함 상품 수: {brand_products}개")
        logger.info(f"- 브랜드명 포함 비율: {brand_ratio:.1f}%")
        
        # 50% 이상이면 공식 브랜드로 간주
        is_authentic = brand_ratio >= 50
        logger.info(f"- 공식 브랜드 판정: {'예' if is_authentic else '아니오'} (기준: 50% 이상)")
        
        return is_authentic, {
            'total_products': total_products,
            'brand_products': brand_products,
            'brand_ratio': brand_ratio
        }
    else:
        logger.info("상품을 찾을 수 없습니다.")
        return False, {'total_products': 0, 'brand_products': 0, 'brand_ratio': 0}