import os
import time
from bs4 import BeautifulSoup
import json
import re
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import threading
import sys
import logging
from log_utils import setup_logger
from datetime import datetime
from urllib.parse import urlparse

# 분리된 모듈 임포트
from config import setup_environment
from browser_utils import clear_chrome_data, setup_browser, scroll_to_bottom, check_product_brand_match, take_screenshot
from link_analyzer import extract_official_links
from gpt_utils import analyze_links_with_gpt4omini, analyze_phone_with_gpt4omini
from data_utils import get_brand_names_from_mongodb, save_contact_info_to_mongodb, mark_brand_as_processed
from captcha_processor import handle_captcha
from url_utils import (get_search_url, extract_url_from_gpt_response, 
                      format_url_for_site_type, detect_site_type, handle_direct_link)
from site_handlers import process_brand_store, process_smartstore, extract_seller_info

def process_brand(brand_name, browser, api_key, read_collection, write_collection):
    """브랜드 처리 함수"""
    # 브랜드별 로거 설정
    logger = setup_logger(brand_name)
    
    extracted_info = {"브랜드명": brand_name}
    
    logger.info(f"===== 브랜드: {brand_name} 처리 시작 =====")
    
    # 네이버 검색 URL 생성 및 이동
    search_url = get_search_url(brand_name)
    browser.get(search_url)
    time.sleep(3)
    
    # 검색 URL 저장
    extracted_info["검색 URL"] = search_url
    
    # 직접 링크 썸네일 확인
    direct_link_elements = browser.find_elements("css selector", ".direct_link_thumb")
    best_url = None

    # 직접 링크 처리
    if direct_link_elements and len(direct_link_elements) > 0:
        try:
            logger.info("공식 홈페이지 바로가기 요소 발견, 클릭 시도 중...")
            best_url = handle_direct_link(browser, direct_link_elements[0])
            
            # 기본 정보 설정
            extracted_info["공식 홈페이지 URL"] = best_url
            logger.info(f"직접 이동한 공식 홈페이지 URL: {best_url}")
            
            # 사이트 유형 확인
            site_type = detect_site_type(best_url)
            if site_type == 'coupang':
                logger.info("쿠팡링크입니다. 확인해주세요")
                extracted_info["결과"] = "쿠팡링크"
                return extracted_info
            elif site_type == 'naver-brandstore':
                extracted_info["도메인유형"] = "네이버-브랜드스토어"
            elif site_type == 'naver-smartstore':
                extracted_info["도메인유형"] = "네이버-스마트스토어"
            elif site_type == 'naver':
                extracted_info["도메인유형"] = "네이버"
            else:
                extracted_info["도메인유형"] = "자사몰"
                
        except Exception as e:
            logger.error(f"바로가기 요소 클릭 중 오류 발생: {e}")
            logger.info("대체 방법으로 링크 분석을 진행합니다...")
            best_url = None
    else:
        logger.info("공식 홈페이지 바로가기 요소가 없습니다. 스크롤 후 링크 추출을 시작합니다...")

    # 직접 링크가 없는 경우 링크 분석
    if not best_url:
        scroll_to_bottom(browser)
        soup = BeautifulSoup(browser.page_source, 'html.parser')
        official_links = extract_official_links(soup, brand_name)
        
        if official_links:
            logger.info("\n공식 홈페이지 후보 링크:")
            for i, link in enumerate(official_links[:10]):
                logger.info(f"{i+1}. 텍스트: {link['text']}")
                logger.info(f"   URL: {link['href']}")
                logger.info(f"   점수: {link['score']}")
            
            # GPT 분석
            gpt_mini_response = analyze_links_with_gpt4omini(official_links[:10], brand_name, api_key)
            
            if "choices" in gpt_mini_response:
                logger.info("\nGPT-4o mini 분석 결과:")
                gpt_analysis = gpt_mini_response["choices"][0]["message"]["content"]
                logger.info(gpt_analysis)
                
                # URL 추출
                best_url = extract_url_from_gpt_response(gpt_analysis, official_links)
                logger.info(f"\n선택된 공식 홈페이지 URL: {best_url}")
            else:
                logger.error("API 응답 오류:", gpt_mini_response)
                best_url = official_links[0]['href'] if official_links else None
        else:
            logger.info("공식 홈페이지 링크를 찾을 수 없습니다.")

    # 가장 적절한 URL로 이동하여 정보 찾기
    if best_url:
        # 사이트 유형 재확인
        site_type = detect_site_type(best_url)
        if site_type == 'coupang':
            logger.info("쿠팡링크입니다. 확인해주세요")
            extracted_info["결과"] = "쿠팡링크"
            return extracted_info
        
        try:
            # 이미 이동하지 않은 경우에만 URL로 이동
            if browser.current_url != best_url:
                best_url = format_url_for_site_type(best_url)
                browser.get(best_url)
                time.sleep(5)
            
            extracted_info["공식 홈페이지 URL"] = best_url
            
            # 실제 도메인 주소 저장
            current_url = browser.current_url
            extracted_info["실제 도메인 주소"] = current_url
            
            # 특정 도메인 목록
            skip_domains = [
                'lfmall.co.kr', 'kurly.com', 'lotteon.com', 'danawa.com', 
                'ssg.com', 'coupang.com', 'ssfshop.com', 'lghellovision.net', 
                'eqlstore.com', 'sivillage.com', 'tmon.co.kr', 'auction.co.kr', 'interpark.com',
                'gmarket.co.kr', 'search.shopping.naver.com', 'ainmall.com', '11st.co.kr',
                'wemakeprice.com', 'hfashionmall.com', 'thehyundai.com', 'gsshop.com',
                'hmall.com', 'facebook.com', 'google.com', 'tiktok.com', 'map.naver.com', 'search.naver.com', 'cafe.naver.com', 'nid.naver.com',
                'jobkorea.co.kr', 'tistory.com', 'terms.naver.com','www.e-himart.co.kr/app/','www.wadiz.kr/web/','mcst.go.kr', 'pay.naver.com',
                'daejeon.go.kr','lotteimall.com/display','samdae500.com', '.go.kr', 'aadome.com', 'adcr.naver.com/adcr',
                'tv.naver.com', 'ader.naver.com', 'shopping.naver.com/ns/home', 
                'costco.co.kr',
                '29cm.co.kr',
                'shopping.naver.com/ns/home',
                'dict.naver.com',
                'github.com',
                'cafe24.com',
                'hankookilbo.com',
                'store.kakao.com',
                'imginn.com',
                '.ac.kr',
                'ko.dict.naver.com',
                'modoo.at',
                'accounts.kakao.com',
                'sapyoung.com',
                'poom.co.kr',
                'incruit.com',
                'travel.naver.com',
                'elandmall.com',
                'tving.com',
                'health.kr',
                
            ]
            
            # 수정된 도메인 체크 코드
            if any(domain in current_url for domain in skip_domains):
                logger.info(f"특정 도메인({current_url})이 감지되어 처리를 건너뜁니다.")
                extracted_info["결과"] = "특정 도메인으로 리다이렉트됨"
                return extracted_info
            
            # 사이트 유형 다시 설정 (URL이 변경되었을 수 있음)
            site_type = detect_site_type(current_url)
            if site_type == 'naver-brandstore':
                extracted_info["도메인유형"] = "네이버-브랜드스토어"
            elif site_type == 'naver-smartstore':
                extracted_info["도메인유형"] = "네이버-스마트스토어"
            elif site_type == 'naver':
                extracted_info["도메인유형"] = "네이버"
            else:
                extracted_info["도메인유형"] = "자사몰"
            
            # 공식 URL 페이지 로딩 후 스크린샷 찍기
            logger.info("페이지 스크롤 전 스크린샷을 찍습니다...")
            logger.info(f"공식 홈페이지 URL: {best_url}")
            logger.info(f"실제 도메인 주소: {current_url}")
            
            # 네이버 메인 페이지인 경우 처리 건너뛰기
            if current_url == "https://www.naver.com/":
                logger.info("실제 도메인 주소가 네이버 메인 페이지입니다. 처리를 건너뜁니다.")
                extracted_info["결과"] = "네이버 메인 페이지로 리다이렉트됨"
                return extracted_info
                
            screenshot_path = take_screenshot(browser, brand_name)
            if screenshot_path:
                extracted_info["페이지 스크린샷"] = screenshot_path
            else:
                extracted_info["페이지 스크린샷"] = ""  # 스크린샷이 없는 경우 빈 문자열 저장
            
            # 사이트 유형별 처리
            scroll_to_bottom(browser)
            time.sleep(3)
            
            if site_type == 'naver-brandstore' or site_type == 'brand_store':
                extracted_info = process_brand_store(browser, brand_name, best_url, api_key, extracted_info)
            elif site_type == 'naver-smartstore' or site_type == 'smartstore':
                is_authentic, brand_match_info = check_product_brand_match(browser, brand_name)
                extracted_info = process_smartstore(browser, brand_name, best_url, api_key, extracted_info, is_authentic)
                
                # 공식 브랜드로 판정된 경우 도메인유형을 "네이버-스마트스토어(전문몰)"로 설정
                if is_authentic:
                    logger.info("공식 브랜드로 판정되어 도메인유형을 '네이버-스마트스토어(전문몰)'로 설정합니다.")
                    extracted_info["도메인유형"] = "네이버-스마트스토어(전문몰)"
                else:
                    extracted_info["도메인유형"] = "네이버-스마트스토어"
            
            # 모든 경우에 공통으로 실행되는 정보 추출 처리
            soup = BeautifulSoup(browser.page_source, 'html.parser')
            extracted_info = extract_seller_info(soup, brand_name, best_url, api_key, extracted_info)
            
        except Exception as e:
            logger.error(f"브랜드 '{brand_name}' 처리 중 오류 발생: {e}")
            # 에러 발생 시에도 기본 정보는 유지
            extracted_info["결과"] = f"오류: {str(e)}"
            # 나머지 필드들은 빈 값으로 설정
            extracted_info["도메인유형"] = ""
            extracted_info["상호명"] = ""
            extracted_info["고객센터 번호"] = ""
            extracted_info["사업장소재지"] = ""
            extracted_info["이메일주소"] = ""
            extracted_info["페이지 스크린샷"] = ""
        
        # 추출된 모든 정보를 로그에 출력
        logger.info("\n======= 추출된 정보 =======")
        for key, value in extracted_info.items():
            logger.info(f"{key}: {value}")
        logger.info("===========================")
        
    logger.info(f"\n===== 브랜드: {brand_name} 처리 완료 =====\n")
    return extracted_info

def input_with_timeout(prompt, timeout=5):
    """타임아웃이 있는 input 함수"""
    print(prompt, end='', flush=True)
    
    # 사용자 입력을 저장할 변수
    user_input = [None]
    
    # 입력을 받는 스레드
    def get_input():
        user_input[0] = sys.stdin.readline().strip()
    
    # 입력 스레드 시작
    input_thread = threading.Thread(target=get_input)
    input_thread.daemon = True
    input_thread.start()
    
    # 카운트다운 시작
    for i in range(timeout, 0, -1):
        if not input_thread.is_alive():
            break  # 입력이 들어오면 카운트다운 중지
        print(f"\r{prompt}({i}초 후 자동 선택)  ", end='', flush=True)
        time.sleep(1)
    
    # 타임아웃 여부 확인
    if input_thread.is_alive():
        print(f"\r{prompt}시간이 초과되었습니다. 기본값을 사용합니다.       ")
        return ""
    
    # 줄바꿈
    print()
    return user_input[0]

def validate_seller_info(seller_info):
    """판매자 정보 유효성 검사"""
    logger = logging.getLogger()
    
    if not seller_info:
        return False
    
    # 네이버 공통 고객센터 번호
    naver_common_numbers = ['1588-3819']
    
    # 고객센터 번호 검사 - 정확히 일치하는 경우만 필터링
    customer_center = seller_info.get('고객센터', '')
    if customer_center in naver_common_numbers:
        logger.warning(f"⚠️ 주의: 네이버 공통 고객센터 번호({customer_center})가 감지되었습니다. 정확한 브랜드 연락처가 아닙니다.")
        return False
    
    # 판매자 정보 유효성 검사 - 필수 필드 확인
    required_fields = ['상호명', '고객센터', '사업장 소재지', 'e-mail']
    for field in required_fields:
        if not seller_info.get(field):
            logger.warning(f"필수 필드 '{field}'가 비어 있습니다.")
            return False
    
    return True

def main():
    """메인 함수"""
    # 스크립트 경로를 기준으로 로그 디렉토리 설정
    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_dir, 'log')
    os.makedirs(log_dir, exist_ok=True)

    # 로그 파일 설정
    log_file = os.path.join(log_dir, f'app_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

    # 로깅 핸들러 설정
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    console_handler = logging.StreamHandler()

    # 로그 형식 설정
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 로거 설정
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 환경 설정
    logger.info("환경 설정 중...")
    api_key, client, db, read_collection, write_collection = setup_environment()
    
    # MongoDB에서 처리되지 않은 브랜드명만 가져오기
    brand_names = get_brand_names_from_mongodb(read_collection, only_unprocessed=True)
    
    # 시작 인덱스 설정
    start_index = 0
    
    if start_index < len(brand_names):
        # main.py가 있는 디렉토리를 기준으로 경로 설정
        main_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 사용 가능한 프로필 확인
        naver_user_data_dir = os.path.join(main_dir, "naver_user_data")
        os.makedirs(naver_user_data_dir, exist_ok=True)
        
        # 기존 프로필 목록 가져오기
        existing_profiles = []
        try:
            existing_profiles = [d for d in os.listdir(naver_user_data_dir) 
                              if os.path.isdir(os.path.join(naver_user_data_dir, d))]
        except Exception as e:
            print(f"프로필 디렉토리 읽기 실패: {e}")
        
        # 프로필 선택 안내 메시지
        if existing_profiles:
            print("\n사용 가능한 프로필:")
            for i, profile in enumerate(existing_profiles):
                print(f"  {i+1}. {profile}")
            print("  0. 'new' - 새 프로필 생성하기")
            print("\n기존 프로필을 사용하려면 프로필명을 입력하고, 새 프로필을 만들려면 'new'를 입력하세요.")
        else:
            print("\n사용 가능한 프로필이 없습니다. 새 프로필을 생성합니다.")
        
        # 프로필명 입력 받기 (타임아웃 적용)
        profile_input = input_with_timeout("어떤 profile_name으로 로그인 할까요? (숫자/new/프로필명): ", timeout=10)
        
        # 입력이 없거나 공백인 경우 기본 프로필 사용
        if not profile_input:
            if existing_profiles:
                profile_name = existing_profiles[0]  # 첫 번째 프로필 사용
                print(f"기본 프로필 '{profile_name}'을 선택합니다.")
            else:
                profile_name = "default_profile"
                print(f"기본 프로필명 '{profile_name}'을 사용합니다.")
            
            # 기존 프로필 자동 로그인 스킵
            login_required = 'n'
            print(f"기존 프로필 '{profile_name}'의 로그인 정보를 유지합니다.")
        # 숫자 입력 처리
        elif profile_input.isdigit():
            index = int(profile_input)
            if 1 <= index <= len(existing_profiles):
                # 입력한 번호에 해당하는 프로필 선택
                profile_name = existing_profiles[index-1]
                print(f"프로필 {index}번 '{profile_name}'을 선택했습니다.")
                
                # 기존 프로필 자동 로그인 스킵
                login_required = 'n'
                print(f"기존 프로필 '{profile_name}'의 로그인 정보를 유지합니다.")
            else:
                print(f"유효하지 않은 번호입니다. 1~{len(existing_profiles)} 사이의 번호를 입력하세요.")
                print("기본 프로필을 사용합니다.")
                profile_name = existing_profiles[0] if existing_profiles else "default_profile"
                
                # 기존 프로필 자동 로그인 스킵
                login_required = 'n'
                print(f"기존 프로필 '{profile_name}'의 로그인 정보를 유지합니다.")
        elif profile_input.lower() == 'new':
            # 새 프로필명 입력 받기
            new_profile_name = input("새 프로필 폴더명을 입력하세요: ").strip()
            if not new_profile_name:
                new_profile_name = "default_profile"
                print(f"입력이 없어 기본값 '{new_profile_name}'을 사용합니다.")
            
            profile_name = new_profile_name
            login_required = 'y'  # 새 프로필은 반드시 로그인 필요
            print(f"새 프로필 '{profile_name}'을 생성합니다. 로그인이 필요합니다.")
        else:
            # 프로필명 직접 입력 처리
            profile_name = profile_input
            
            # 기존 프로필 존재 여부 확인
            profile_exists = profile_name in existing_profiles
            if profile_exists:
                print(f"기존 프로필 '{profile_name}'을 사용합니다.")
                # 기존 프로필은 자동으로 로그인 건너뛰기
                login_required = 'n'
                print(f"기존 프로필 '{profile_name}'의 로그인 정보를 유지합니다.")
            else:
                print(f"프로필 '{profile_name}'이 존재하지 않습니다. 새로 생성하고 로그인합니다.")
                login_required = 'y'  # 존재하지 않는 프로필은 로그인 필요
                os.makedirs(os.path.join(naver_user_data_dir, profile_name), exist_ok=True)
        
        # 프로필 디렉토리 설정
        user_data_dir = os.path.join(main_dir, "naver_user_data", profile_name)
        os.makedirs(user_data_dir, exist_ok=True)
        
        print(f"사용자 데이터 디렉토리: {user_data_dir}")
        print(f"디렉토리 존재 여부: {os.path.exists(user_data_dir)}")
        
        # 캐시 정리 부분 - 로그인시에는 캐시 정리하지 않음
        if login_required == 'y':
            print("로그인 정보 유지를 위해 캐시 정리를 건너뜁니다.")
        else:
            # 로그인이 필요 없다면 캐시 정리
            clear_chrome_data(user_data_dir)
            print("캐시 정리 완료.")
        
        # 브라우저 설정 - 예전 코드의 방식으로 직접 옵션 설정
        chrome_options = Options()
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("detach", True)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        
        # 절대 경로로 변환
        abs_user_data_dir = os.path.abspath(user_data_dir)
        print(f"사용할 크롬 프로필 절대 경로: {abs_user_data_dir}")
        chrome_options.add_argument(f"user-data-dir={abs_user_data_dir}")
        
        chrome_options.add_argument("--disable-application-cache")
        chrome_options.add_argument("--disable-cache")
        chrome_options.headless = False
        
        browser = webdriver.Chrome(options=chrome_options)
        
        # 로그인 과정이 필요한 경우
        if login_required == 'y':
            # 네이버 로그인 페이지로 직접 이동
            print(f"\n네이버 로그인 페이지로 이동합니다. 프로필 '{profile_name}'에 로그인해 주세요.")
            browser.get("https://nid.naver.com/nidlogin.login")
            
            # 15초 대기 - 사용자가 로그인할 수 있는 시간 제공
            print("15초 동안 대기합니다. 이 시간 동안 로그인해 주세요...")
            for i in range(15, 0, -1):
                print(f"\r로그인 대기 시간: {i}초 남음...", end="")
                time.sleep(1)
            print("\n대기 시간이 끝났습니다. 작업을 계속합니다.")
        else:
            print(f"로그인 과정을 건너뜁니다. 프로필 '{profile_name}'을 사용합니다.")
        
        # 로그인 상태 확인 - 네이버 메인으로 이동하여 확인
        browser.get("https://www.naver.com")
        time.sleep(2)
        
        print("브라우저 상태 확인 중...")
        print(f"현재 URL: {browser.current_url}")
        
        # 로그인 상태 확인 (네이버 로그인 버튼 또는 프로필 요소 확인)
        try:
            login_elements = browser.find_elements("css selector", ".sc_login")
            if login_elements and len(login_elements) > 0:
                print("⚠️ 주의: 네이버에 로그인되어 있지 않은 것 같습니다!")
                proceed_anyway = input("로그인 없이 계속 진행하시겠습니까? (y/n): ").strip().lower()
                if proceed_anyway != 'y':
                    print("작업을 중단합니다.")
                    browser.quit()
                    return
            else:
                print("✅ 네이버에 로그인되어 있는 것 같습니다.")
        except Exception as e:
            print(f"로그인 상태 확인 중 오류: {e}")
        
        # 브랜드 분석 작업 자동 시작
        print("브랜드 분석 작업을 자동으로 시작합니다...")

        # 결과를 저장할 리스트
        results = []

        # 건너뛸 브랜드명 리스트
        skip_brands = ['Unknown', 'Unkonwn', '확인필요', 'Unspecified Brand']

        # 모든 브랜드 처리
        for idx, brand_name in enumerate(brand_names[start_index:]):
            # 건너뛸 브랜드명인지 확인
            if brand_name in skip_brands or brand_name.strip() == '':
                print(f"\n===== 브랜드: {brand_name} - 처리 건너뜀 =====\n")
                results.append({"브랜드명": brand_name, "결과": "처리 건너뜀"})
                # 처리를 건너뛰어도 처리 완료로 표시
                mark_brand_as_processed(read_collection, brand_name)
                continue
                
            try:
                # 브랜드 처리 함수에 읽기 및 쓰기 컬렉션 전달
                brand_info = process_brand(brand_name, browser, api_key, read_collection, write_collection)
                results.append(brand_info)  # 결과 저장
                
                # MongoDB에 결과 저장 (쓰기용 컬렉션 사용)
                save_contact_info_to_mongodb(brand_name, brand_info, write_collection, read_collection)
                
                # 브랜드를 처리 완료로 표시
                mark_brand_as_processed(read_collection, brand_name)
                
                # 매 5개 브랜드마다 중간 진행 상황 출력
                if (idx + 1) % 5 == 0:
                    print(f"\n중간 진행 상황: {idx + 1}개 브랜드 처리됨\n")
            
            except Exception as e:
                print(f"브랜드 '{brand_name}' 처리 중 오류 발생: {e}")
                results.append({"브랜드명": brand_name, "결과": f"오류: {str(e)}"})

        print("모든 브랜드 처리 완료")
        
        # 브라우저 종료
        try:
            browser.quit()
        except:
            pass
            
        input("종료하려면 Enter 키를 누르세요...")
    else:
        print("처리할 브랜드가 없습니다.")


if __name__ == "__main__":
    main() 
