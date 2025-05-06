import base64
import os
import time
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from gpt_utils import get_captcha_answer

def extract_seller_info(browser):
    """판매자 정보 HTML에서 필요한 정보를 추출하는 함수"""
    logger = logging.getLogger()
    
    try:
        logger.info("판매자 정보 HTML에서 연락처 정보 추출 중...")
        
        # 판매자 정보 컨테이너 찾기
        seller_container = browser.find_element(By.CSS_SELECTOR, '._30Kfe1c22L')
        
        # 정보 초기화
        seller_info = {
            '상호명': '',
            '대표자': '',
            '고객센터': '',
            '사업자등록번호': '',
            '사업장 소재지': '',
            '통신판매업번호': '',
            'e-mail': ''
        }
        
        # 각 정보 항목 찾기
        info_items = seller_container.find_elements(By.CSS_SELECTOR, '.aAVvlAZ43w')
        
        for item in info_items:
            try:
                # 항목 제목과 내용 추출
                title = item.find_element(By.CSS_SELECTOR, '._1nqckXI-BW').text.strip()
                value = item.find_element(By.CSS_SELECTOR, '.EdE67hDR6I').text.strip()
                
                # 고객센터 번호의 경우 추가 텍스트가, 인증, 신고 버튼 등이 있으므로 전화번호만 추출
                if title == '고객센터':
                    value = value.split(' ')[0]  # 첫 번째 부분만 가져옴 (전화번호)
                
                # 정보 저장
                if title in seller_info:
                    seller_info[title] = value
            except Exception as e:
                logger.error(f"항목 '{title}' 추출 중 오류: {e}")
        
        # 한 번만 로깅
        logger.info("\n======= 추출된 판매자 정보 =======")
        for key, value in seller_info.items():
            logger.info(f"{key}: {value}")
        logger.info("===============================")
        
        return seller_info
        
    except Exception as e:
        logger.error(f"판매자 정보 추출 중 오류 발생: {e}")
        return None

def process_seller_info(browser, api_key=None):
    """판매자 정보 처리 함수 - CAPTCHA 처리 및 정보 추출"""
    logger = logging.getLogger()
    
    # 성공 여부 확인을 위한 선택자들
    seller_info_selectors = [
        '._30Kfe1c22L',     # 판매자 정보 메인 컨테이너
        '._3BlyWp6LJv',     # 판매자 정보 상세 리스트
        '._18Mr7W8HRL'      # "판매자 정보 확인" 제목
    ]
    
    try:
        # 먼저 판매자 정보 페이지가 이미 표시되었는지 확인
        for selector in seller_info_selectors:
            elements = browser.find_elements(By.CSS_SELECTOR, selector)
            if elements and any(elem.is_displayed() for elem in elements):
                logger.info(f"판매자 정보 화면이 이미 표시되어 있습니다. ({selector})")
                # 판매자 정보 추출 바로 실행
                return extract_seller_info(browser)
        
        # 판매자 정보가 표시되지 않았으면 CAPTCHA 확인
        captcha_exists = len(browser.find_elements(By.ID, 'captchaimg')) > 0
        
        if not captcha_exists:
            logger.info("이 페이지에는 CAPTCHA가 없고 판매자 정보도 표시되지 않았습니다.")
            return True  # 원래 코드와 동일하게 True 반환
        
        # CAPTCHA가 있으면 처리 시작
        logger.info("CAPTCHA 이미지를 발견했습니다. CAPTCHA 처리를 시작합니다.")
        
        # 최대 CAPTCHA 시도 횟수
        max_captcha_attempts = 5  # 최초 1회 + 실패시 4회 추가
        captcha_attempt = 0
        
        while captcha_attempt < max_captcha_attempts:
            captcha_attempt += 1
            logger.info(f"CAPTCHA 처리 시도 {captcha_attempt}/{max_captcha_attempts}")
            
            # 첫 번째 시도 전에도 페이지 새로고침
            if captcha_attempt == 1:
                logger.info("첫 번째 시도 전에 페이지를 새로고침합니다.")
                browser.refresh()
                time.sleep(3)  # 새로고침 후 로딩 시간 제공
            
            # 이미지 다운로드
            img_element = browser.find_element(By.ID, 'captchaimg')
            img_src = img_element.get_attribute('src')

            # 이미지가 base64로 인코딩되어 있는 경우
            if img_src.startswith('data:image'):
                # base64 데이터 추출
                img_data = img_src.split(',')[1]
                img_data = base64.b64decode(img_data)

                # 디렉토리 생성
                os.makedirs('agent_capture', exist_ok=True)

                # 이미지 저장
                with open('agent_capture/captcha.png', 'wb') as f:
                    f.write(img_data)

            # 캡차 질문 텍스트 추출
            question_element = browser.find_element(By.ID, 'captcha_info')
            question_text = question_element.text
            logger.info(question_text)

            # 캡차 답변 획득
            captcha_answer = get_captcha_answer('agent_capture/captcha.png', question_text, api_key)
            logger.info(f"GPT가 제공한 캡차 답변: {captcha_answer}")
            
            try:
                # 캡챠 입력 필드에 값 설정
                browser.execute_script(f"document.getElementById('captcha').value = '{captcha_answer}';")
                logger.info("JavaScript로 캡차 값을 설정했습니다.")
                logger.info(f"캡차 답변 '{captcha_answer}'이(가) 입력되었습니다. 자동으로 제출합니다.")
                
                # 제출 버튼 클릭
                browser.execute_script("document.querySelector('button[type=\"submit\"]').click();")
                logger.info("JavaScript로 제출 버튼을 클릭했습니다.")
                
                # 제출 후 잠시 대기
                time.sleep(3)
                
                # 성공 여부 확인 (3번 확인)
                max_check_attempts = 3
                check_attempt = 0
                success = False
                
                while check_attempt < max_check_attempts and not success:
                    # 오류 메시지 확인
                    error_elements = browser.find_elements(By.CSS_SELECTOR, '.error-message, .captcha-error, #captcha_layer .error')
                    if error_elements and any(elem.is_displayed() for elem in error_elements):
                        logger.warning("CAPTCHA 오류 메시지 발견: 잘못된 입력입니다.")
                        break
                    
                    # 판매자 정보 요소 확인
                    for selector in seller_info_selectors:
                        elements = browser.find_elements(By.CSS_SELECTOR, selector)
                        if elements and any(elem.is_displayed() for elem in elements):
                            logger.info(f"성공! 판매자 정보 요소({selector})가 발견되었습니다.")
                            success = True
                            # CAPTCHA 성공 후 판매자 정보 추출
                            return extract_seller_info(browser)
                    
                    # 더 기다려보기
                    check_attempt += 1
                    if not success and check_attempt < max_check_attempts:
                        logger.info(f"판매자 정보를 찾을 수 없습니다. {max_check_attempts - check_attempt}번 더 확인합니다...")
                        time.sleep(2)
                
                # 성공 여부에 따른 처리
                if success:
                    return extract_seller_info(browser)
                
                # 실패한 경우 다음 시도로 진행
                if captcha_attempt < max_captcha_attempts:
                    logger.warning("CAPTCHA 인증에 실패했습니다. 페이지를 새로고침하고 다시 시도합니다.")
                    
                    # 새로고침 버튼이 있으면 클릭, 없으면 F5로 새로고침
                    refresh_buttons = browser.find_elements(By.CSS_SELECTOR, '#captcha_refresh, .captcha-refresh')
                    if refresh_buttons and any(btn.is_displayed() for btn in refresh_buttons):
                        logger.info("CAPTCHA 새로고침 버튼을 클릭합니다.")
                        for btn in refresh_buttons:
                            if btn.is_displayed():
                                btn.click()
                                break
                    else:
                        logger.info("F5 키를 사용하여 페이지를 새로고침합니다.")
                        browser.refresh()
                    
                    time.sleep(3)  # 새로고침 후 잠시 대기
                    continue
                
            except Exception as e:
                logger.error(f"JavaScript 캡차 처리 실패: {e}")
                
                # 실패한 경우 다음 시도로 진행
                if captcha_attempt < max_captcha_attempts:
                    logger.warning("CAPTCHA 인증에 실패했습니다. 페이지를 새로고침하고 다시 시도합니다.")
                    browser.refresh()
                    time.sleep(3)
                    continue
                
                return False
        
        # 모든 시도 실패
        logger.error(f"최대 시도 횟수({max_captcha_attempts})에 도달했지만 CAPTCHA 인증에 실패했습니다.")
        return False
        
    except Exception as e:
        logger.error(f"CAPTCHA 처리 중 오류 발생: {e}")
        logger.info("CAPTCHA 처리를 건너뛰고 계속 진행합니다.")
        return False

# 기존 코드와의 호환성을 위해 handle_captcha 함수 유지
def handle_captcha(browser, api_key):
    """CAPTCHA 처리 함수"""
    logger = logging.getLogger()
    
    # 성공 여부 확인을 위한 선택자들 - 함수의 시작 부분에 정의
    success_selectors = [
        '._30Kfe1c22L',     # 판매자 정보 메인 컨테이너
        '._3BlyWp6LJv',     # 판매자 정보 상세 리스트
        '._18Mr7W8HRL',     # "판매자 정보 확인" 제목
        '.aAVvlAZ43w',      # 판매자 정보 항목 컨테이너
        '._1nqckXI-BW',     # 항목 제목 요소
        '.EdE67hDR6I'       # 항목 내용 요소
    ]
    
    try:
        # 최대 CAPTCHA 시도 횟수
        max_captcha_attempts = 5  # 최초 1회 + 실패시 4회 추가
        captcha_attempt = 0
        
        while captcha_attempt < max_captcha_attempts:
            # 현재 시도 횟수 로깅
            captcha_attempt += 1
            logger.info(f"CAPTCHA 처리 시도 {captcha_attempt}/{max_captcha_attempts}")
            
            # CAPTCHA 이미지 요소가 존재하는지 확인
            captcha_exists = len(browser.find_elements(By.ID, 'captchaimg')) > 0
            
            if not captcha_exists:
                logger.info("이 페이지에는 CAPTCHA가 없습니다. CAPTCHA 처리를 건너뜁니다.")
                return True  # CAPTCHA가 없으면 성공으로 간주
                
            logger.info("CAPTCHA 이미지를 발견했습니다. CAPTCHA 처리를 시작합니다.")
            
            # 이미지 다운로드
            img_element = browser.find_element(By.ID, 'captchaimg')
            img_src = img_element.get_attribute('src')

            # 이미지가 base64로 인코딩되어 있는 경우
            if img_src.startswith('data:image'):
                # base64 데이터 추출
                img_data = img_src.split(',')[1]
                img_data = base64.b64decode(img_data)

                # 디렉토리 생성
                os.makedirs('agent_capture', exist_ok=True)

                # 이미지 저장
                with open('agent_capture/captcha.png', 'wb') as f:
                    f.write(img_data)

            # 터미널에 텍스트 출력
            question_element = browser.find_element(By.ID, 'captcha_info')
            question_text = question_element.text
            logger.info(question_text)

            # 캡차 답변 획득
            captcha_answer = get_captcha_answer('agent_capture/captcha.png', question_text, api_key)
            logger.info(f"GPT가 제공한 캡차 답변: {captcha_answer}")
            
            try:
                # 캡챠 입력 필드에 값 설정
                browser.execute_script(f"document.getElementById('captcha').value = '{captcha_answer}';")
                logger.info("JavaScript로 캡차 값을 설정했습니다.")
                logger.info(f"캡차 답변 '{captcha_answer}'이(가) 입력되었습니다. 자동으로 제출합니다.")
                
                # 제출 버튼 클릭
                browser.execute_script("document.querySelector('button[type=\"submit\"]').click();")
                logger.info("JavaScript로 제출 버튼을 클릭했습니다.")
                
                # 제출 후 잠시 대기
                time.sleep(3)
                
                # 성공 여부 확인 (3번 확인)
                max_check_attempts = 3
                check_attempt = 0
                success = False
                
                while check_attempt < max_check_attempts and not success:
                    # 오류 메시지 확인
                    error_elements = browser.find_elements(By.CSS_SELECTOR, '.error-message, .captcha-error, #captcha_layer .error')
                    if error_elements and any(elem.is_displayed() for elem in error_elements):
                        logger.warning("CAPTCHA 오류 메시지 발견: 잘못된 입력입니다.")
                        break
                    
                    # 판매자 정보 요소 확인
                    for selector in success_selectors:
                        elements = browser.find_elements(By.CSS_SELECTOR, selector)
                        if elements and any(elem.is_displayed() for elem in elements):
                            logger.info(f"성공! 판매자 정보 요소({selector})가 발견되었습니다.")
                            success = True
                            
                            # 판매자 정보 추출 추가 (선택 사항)
                            if '._30Kfe1c22L' in selector or selector == '._30Kfe1c22L':
                                seller_info = extract_seller_info(browser)
                            
                            return True  # 성공적으로 CAPTCHA 처리 완료
                    
                    # 더 기다려보기
                    check_attempt += 1
                    if not success and check_attempt < max_check_attempts:
                        logger.info(f"판매자 정보를 찾을 수 없습니다. {max_check_attempts - check_attempt}번 더 확인합니다...")
                        time.sleep(2)  # 추가 대기
                
                # 성공 여부에 따른 처리
                if success:
                    return True
                
                # 실패한 경우 다음 시도로 진행
                if captcha_attempt < max_captcha_attempts:
                    logger.warning("CAPTCHA 인증에 실패했습니다. 페이지를 새로고침하고 다시 시도합니다.")
                    
                    # 새로고침 버튼이 있으면 클릭, 없으면 F5로 새로고침
                    refresh_buttons = browser.find_elements(By.CSS_SELECTOR, '#captcha_refresh, .captcha-refresh')
                    if refresh_buttons and any(btn.is_displayed() for btn in refresh_buttons):
                        logger.info("CAPTCHA 새로고침 버튼을 클릭합니다.")
                        for btn in refresh_buttons:
                            if btn.is_displayed():
                                btn.click()
                                break
                    else:
                        logger.info("F5 키를 사용하여 페이지를 새로고침합니다.")
                        browser.refresh()
                    
                    time.sleep(3)  # 새로고침 후 잠시 대기
                    continue  # 다음 시도로 진행
                
            except Exception as e:
                logger.error(f"JavaScript 캡차 처리 실패: {e}")
                
                # 실패한 경우 다음 시도로 진행
                if captcha_attempt < max_captcha_attempts:
                    logger.warning("CAPTCHA 인증에 실패했습니다. 페이지를 새로고침하고 다시 시도합니다.")
                    browser.refresh()
                    time.sleep(3)  # 새로고침 후 잠시 대기
                    continue  # 다음 시도로 진행
                
                return False
        
        # 모든 시도 실패
        logger.error(f"최대 시도 횟수({max_captcha_attempts})에 도달했지만 CAPTCHA 인증에 실패했습니다.")
        return False
        
    except Exception as e:
        logger.error(f"CAPTCHA 처리 중 오류 발생: {e}")
        logger.info("CAPTCHA 처리를 건너뛰고 계속 진행합니다.")
        return False 