from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

def convert_to_number(text):
    """인스타그램 숫자 형식을 순수 숫자로 변환"""
    text = text.replace(',', '')  # 쉼표 제거
    
    if '만' in text:
        number = float(text.replace('만', '')) * 10000
        return str(int(number))
    elif '천' in text:
        number = float(text.replace('천', '')) * 1000
        return str(int(number))
    else:
        return text

def extract_following_count(driver):
    """
    인스타그램 프로필에서 팔로우 수를 추출하는 함수
    
    Args:
        driver: Selenium WebDriver 인스턴스
        
    Returns:
        tuple: (팔로우 수, 성공 여부)
            - 팔로우 수: 추출된 팔로우 수 (문자열)
            - 성공 여부: True/False
    """
    try:
        following = convert_to_number(driver.find_element(By.XPATH, "//li[contains(., '팔로우')]/div/a/span/span").text)
        return following, True
    except NoSuchElementException:
        print("팔로우 수를 찾을 수 없습니다.")
        return '-', False
    except Exception as e:
        print(f"팔로우 수 추출 중 오류 발생: {str(e)}")
        return '-', False 