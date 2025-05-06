"""
이 모듈은 다양한 유형의 웹사이트(네이버 브랜드 스토어, 스마트스토어 등)에서 
브랜드 및 판매자 정보를 추출하기 위한 특화된 처리 기능을 제공합니다.

주요 기능:
1. 네이버 브랜드 스토어 페이지 처리 및 정보 추출
2. 스마트스토어 페이지 처리 및 정보 추출
3. 판매자 정보 페이지에서 상호명, 고객센터 번호, 주소, 이메일 등 추출
4. 캡챠 및 새 창 처리 로직

이 모듈은 셀레니움 웹드라이버를 사용하여 웹 페이지를 탐색하고,
BeautifulSoup 및 GPT를 활용하여 필요한 연락처 정보를 추출합니다.
"""

"""사이트 유형별 처리 모듈"""
import time
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from captcha_processor import handle_captcha
from gpt_utils import analyze_phone_with_gpt4omini
import json
import re
import logging
from click_tracker import track_seller_button_click

def process_brand_store(browser, brand_name, url, api_key, extracted_info):
    """네이버 브랜드 스토어 처리 함수"""
    logger = logging.getLogger()
    logger.info("\n네이버 브랜드 스토어가 감지되었습니다!")
    
    # 최대 재시도 횟수 설정
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info(f"판매자 상세정보 버튼 찾기 시도 중... (시도 {retry_count + 1}/{max_retries})")
            
            # 페이지 로드를 위해 스크롤 다운
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)  # 스크롤 후 로딩 대기
            
            # 판매자 상세정보 버튼 찾기
            seller_info_button = browser.find_element("css selector", "button._8Z05k1oEsY._nlog_click[data-shp-area-id='sellerinfo']")
            
            # 버튼이 보이도록 스크롤
            browser.execute_script("arguments[0].scrollIntoView(true);", seller_info_button)
            time.sleep(1)
            
            # 버튼 클릭
            seller_info_button.click()
            logger.info("판매자 상세정보 버튼을 클릭했습니다.")
            
            # 클릭 추적 - 현재 URL 기록
            track_seller_button_click(browser.current_url)
            
            # 새 창으로 전환
            time.sleep(2)
            window_handles = browser.window_handles
            
            if len(window_handles) > 1:
                browser.switch_to.window(window_handles[-1])
                logger.info("새 창으로 전환했습니다.")
                
                # 캡챠 처리 및 판매자 정보 추출
                captcha_handled = handle_captcha(browser, api_key)
                
                if captcha_handled:
                    soup = BeautifulSoup(browser.page_source, 'html.parser')
                    extracted_info = extract_seller_info(soup, brand_name, url, api_key, extracted_info)
                
                # 다시 원래 창으로 돌아가기
                browser.switch_to.window(window_handles[0])
            
            # 성공적으로 처리되었으면 루프 종료
            break
            
        except Exception as e:
            retry_count += 1
            logger.warning(f"판매자 상세정보 버튼 찾기 실패 (시도 {retry_count}/{max_retries}): {e}")
            
            if retry_count < max_retries:
                wait_time = 3  # 일정한 대기 시간 사용
                logger.info(f"새로고침 후 {wait_time}초 대기 후 재시도합니다...")
                
                # 항상 새로고침 시도 (첫 번째 시도 실패 후부터)
                browser.refresh()
                time.sleep(wait_time)  # 새로고침 후 로딩 시간 제공
            else:
                logger.error(f"최대 재시도 횟수({max_retries})에 도달했습니다.")
                logger.info("⚠️ 일반적인 방법으로 페이지 분석을 진행합니다.")
    
    return extracted_info

def process_smartstore(browser, brand_name, url, api_key, extracted_info, is_authentic=False):
    """스마트스토어 처리 함수"""
    logger = logging.getLogger()
    logger.info("\n스마트스토어 URL이 감지되었습니다!")
    
    # 공식 브랜드로 판정된 경우 도메인유형을 "네이버-스마트스토어(전문몰)"로 설정
    if is_authentic:
        logger.info("공식 브랜드로 판정되어 도메인유형을 '네이버-스마트스토어(전문몰)'로 설정합니다.")
        extracted_info["도메인유형"] = "네이버-스마트스토어(전문몰)"
    else:
        extracted_info["도메인유형"] = "네이버-스마트스토어"
    
    # 최대 재시도 횟수 설정
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logger.info(f"판매자 상세정보 버튼 찾기 시도 중... (시도 {retry_count + 1}/{max_retries})")
            
            # 페이지 로드를 위해 스크롤 다운
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)  # 스크롤 후 로딩 대기
            
            # 판매자 상세정보 버튼 찾기
            seller_info_button = browser.find_element("css selector", "button._8Z05k1oEsY._nlog_click[data-shp-area-id='sellerinfo']")
            
            browser.execute_script("arguments[0].scrollIntoView(true);", seller_info_button)
            time.sleep(1)
            
            seller_info_button.click()
            logger.info("판매자 상세정보 버튼을 클릭했습니다.")
            
            # 클릭 추적 - 현재 URL 기록
            track_seller_button_click(browser.current_url)
            
            time.sleep(2)
            window_handles = browser.window_handles
            
            if len(window_handles) > 1:
                browser.switch_to.window(window_handles[-1])
                logger.info("새 창으로 전환했습니다.")
                
                # 캡챠 처리
                captcha_handled = handle_captcha(browser, api_key)
                
                if captcha_handled:
                    soup = BeautifulSoup(browser.page_source, 'html.parser')
                    extracted_info = extract_seller_info(soup, brand_name, url, api_key, extracted_info)
                    
                    # 공식 브랜드가 아닌 경우 경고 표시
                    if not is_authentic:
                        logger.warning("\n⚠️ 경고: 판매자 정보가 정확한 브랜드 공식 정보인지 확인이 필요합니다! ⚠️")
                
                # 다시 원래 창으로 돌아가기
                browser.switch_to.window(window_handles[0])
            
            # 성공적으로 처리되었으면 루프 종료
            break
            
        except Exception as e:
            retry_count += 1
            logger.warning(f"판매자 상세정보 버튼 찾기 실패 (시도 {retry_count}/{max_retries}): {e}")
            
            if retry_count < max_retries:
                wait_time = 3  # 일정한 대기 시간 사용
                logger.info(f"새로고침 후 {wait_time}초 대기 후 재시도합니다...")
                
                # 항상 새로고침 시도 (첫 번째 시도 실패 후부터)
                browser.refresh()
                time.sleep(wait_time)  # 새로고침 후 로딩 시간 제공
            else:
                logger.error(f"최대 재시도 횟수({max_retries})에 도달했습니다.")
                logger.info("⚠️ 일반적인 방법으로 페이지 분석을 진행합니다.")
    
    return extracted_info

def extract_seller_info(soup, brand_name, url, api_key, extracted_info):
    """판매자 정보 추출 함수"""
    logger = logging.getLogger()
    logger.info("판매자 정보 HTML에서 연락처 정보 추출 중...")
    
    # 이미 유효한 판매자 정보가 extracted_info에 있다면 우선 사용
    if (extracted_info.get("상호명") and 
        extracted_info.get("고객센터 번호") and 
        extracted_info.get("사업장소재지") and 
        extracted_info.get("이메일주소")):
        logger.info("이미 유효한 판매자 정보가 있습니다. 기존 정보를 유지합니다.")
        return extracted_info
    
    # 캡챠 프로세서에서 추출된 정보가 있는지 확인
    # 일단 GPT 분석 진행
    gpt_response = analyze_phone_with_gpt4omini(soup, brand_name, url, api_key)
    
    if "choices" in gpt_response:
        phone_analysis = gpt_response["choices"][0]["message"]["content"]
        clean_json_text = re.sub(r'```json\s*|\s*```', '', phone_analysis)
        
        try:
            info_json = json.loads(clean_json_text)
            
            # '1588-3819' 번호 확인 - GPT가 네이버 공통 번호를 반환하면 무시하고 직접 추출한 정보 사용
            if "customer_service_number" in info_json and info_json["customer_service_number"] == "1588-3819":
                logger.warning("⚠️ 주의: GPT가 네이버 공통 고객센터 번호(1588-3819)를 감지했습니다. 실제 판매자 페이지에서 추출된 정보를 확인합니다.")
                # 기존 정보를 유지하고 GPT 분석 결과는 무시
                logger.info("\n======= 판매자 정보 추출 완료 =======")
                return extracted_info
            
            # 정상적인 경우 정보 추가 진행
            if "company_name" in info_json and info_json["company_name"] != "정보 없음":
                extracted_info["상호명"] = info_json["company_name"]
            if "customer_service_number" in info_json and info_json["customer_service_number"] != "정보 없음":
                extracted_info["고객센터 번호"] = info_json["customer_service_number"]
            if "business_address" in info_json and info_json["business_address"] != "정보 없음":
                extracted_info["사업장소재지"] = info_json["business_address"]
            if "email" in info_json and info_json["email"] != "정보 없음":
                extracted_info["이메일주소"] = info_json["email"]
            
            logger.info("\n======= 판매자 정보 추출 완료 =======")
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON 파싱 실패: {e}")
            logger.info("원본 텍스트:")
            logger.info(phone_analysis)
    
    return extracted_info 