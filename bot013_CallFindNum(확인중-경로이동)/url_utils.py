"""
이 모듈은 브랜드 관련 URL을 생성, 분석, 처리하기 위한 유틸리티 함수들을 제공합니다.

주요 기능:
1. 브랜드명을 기반으로 검색 URL 생성
2. GPT 응답에서 브랜드 관련 URL 추출 및 정제
3. 사이트 유형(쿠팡, 네이버 브랜드스토어, 스마트스토어 등) 감지
4. 사이트 유형에 따른 URL 형식 최적화
5. 웹 브라우저에서 직접 링크 클릭 처리

이 모듈은 브랜드의 공식 웹사이트 및 스토어 URL을 효과적으로 처리하여
브랜드 연락처 정보 수집을 위한 기반을 제공합니다.
"""

"""URL 처리 관련 유틸리티 함수"""
import re
import time
from bs4 import BeautifulSoup
import urllib.parse
import logging
from urllib.parse import urlparse, urlunparse

# 모듈 레벨에서 로거 가져오기
logger = logging.getLogger()

def clean_url(url):
    """
    URL에서 불필요한 경로와 파라미터를 제거하여 기본 도메인만 반환합니다.
    단, 다음 도메인들은 정제 대상에서 제외합니다:
    - brand.naver.com
    - smartstore.naver.com
    - stores.auction.co.kr
    - pf.kakao.com (카카오 플러스친구)
    - contents.ohou.se (오늘의 집)
    - www.wadiz.kr (와디즈)
    - www.11st.co.kr (11번가)
    
    Args:
        url (str): 정제할 URL
        
    Returns:
        str: 정제된 URL
    """
    try:
        # 정제 제외 대상 도메인들
        excluded_domains = [
            'brand.naver.com',
            'smartstore.naver.com',
            'stores.auction.co.kr',
            'pf.kakao.com',
            'contents.ohou.se',
            'www.wadiz.kr',
            'www.11st.co.kr'
        ]
        
        # 제외 대상 도메인이 포함된 URL은 그대로 반환
        if any(domain in url for domain in excluded_domains):
            return url
            
        # URL 파싱
        parsed = urlparse(url)
        
        # 도메인과 포트만 남기고 나머지 부분 제거
        cleaned = urlunparse((
            parsed.scheme,
            parsed.netloc,
            '',  # path 제거
            '',  # params 제거
            '',  # query 제거
            ''   # fragment 제거
        ))
        
        # 마지막 슬래시 제거
        cleaned = cleaned.rstrip('/')
        
        return cleaned
    except Exception as e:
        logger.error(f"URL 정제 중 오류 발생: {e}")
        return url

def get_search_url(brand_name):
    """브랜드명으로 검색 URL 생성"""
    encoded_query = urllib.parse.quote(brand_name)
    return f"https://search.naver.com/search.naver?where=nexearch&sm=top_hty&fbm=0&ie=utf8&query={encoded_query}"

def extract_url_from_gpt_response(gpt_analysis, official_links=None):
    """GPT 응답에서 URL 추출"""
    best_url = None
    
    # 1. 마크다운 링크 패턴 찾기 [text](url) 형식
    markdown_link_pattern = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')
    markdown_matches = markdown_link_pattern.findall(gpt_analysis)

    if markdown_matches:
        best_url = markdown_matches[0][1]
        logger.info(f"GPT 결과에서 마크다운 링크에서 추출한 URL: {best_url}")
    else:
        # 2. 일반 URL 패턴 찾기
        url_pattern = re.compile(r'https?://[^\s)\'"*]+')
        found_urls = url_pattern.findall(gpt_analysis)
        
        if found_urls:
            best_url = found_urls[0].rstrip('*.')
            logger.info(f"GPT 결과에서 직접 추출한 URL: {best_url}")
            
            # URL에 마크다운 문자가 있는지 확인
            if '**' in best_url:
                best_url = best_url.replace('**', '')
                logger.info(f"정제된 URL: {best_url}")

    # 3. 위 방법으로 찾지 못한 경우
    if not best_url and official_links:
        best_url = official_links[0]['href']
        logger.info(f"GPT 결과에서 URL을 찾지 못해 첫 번째 링크 사용: {best_url}")
    
    # URL에서 한국어 및 기타 비ASCII 문자 제거
    if best_url:
        # URL에서 한국어 및 기타 특수 문자 제거
        clean_url_pattern = re.compile(r'https?://[a-zA-Z0-9-_.]+(?:\.[a-zA-Z0-9-_.]+)+(?:/[a-zA-Z0-9-_.~%]*)?\/?')
        clean_url_match = clean_url_pattern.search(best_url)
        if clean_url_match:
            best_url = clean_url_match.group(0)
            logger.info(f"한국어 및 특수 문자가 제거된 URL: {best_url}")
        
        # URL 정제 적용
        best_url = clean_url(best_url)
        logger.info(f"최종 정제된 URL: {best_url}")
    
    return best_url

def format_url_for_site_type(url):
    """사이트 유형에 따라 URL 형식 조정"""
    # 로거 객체 가져오기
    logger = logging.getLogger()
    
    # 스마트스토어 URL인 경우 /category/ALL?cp=1 추가
    if 'smartstore.naver.com' in url:
        if not url.endswith('/'):
            url += '/'
        url += 'category/ALL?cp=1'
        logger.info(f"스마트스토어 카테고리 페이지로 수정된 URL: {url}")
    
    return url

def detect_site_type(url):
    """URL 기반으로 사이트 유형 감지"""
    url = url.lower()
    
    if 'coupang.com' in url:
        return 'coupang'
    elif 'brand.naver.com' in url:
        return 'naver-brandstore'
    elif 'smartstore.naver.com' in url:
        return 'naver-smartstore'
    elif 'naver.com' in url or 'pay.naver.com' in url:
        return 'naver'
    else:
        return 'external'

def handle_direct_link(browser, direct_link_element):
    """직접 링크 클릭 처리"""
    # 요소가 보이도록 스크롤
    browser.execute_script("arguments[0].scrollIntoView(true);", direct_link_element)
    time.sleep(1)  # 스크롤 후 잠시 대기
    
    # JavaScript로 클릭 시도
    browser.execute_script("arguments[0].click();", direct_link_element)
    time.sleep(3)  # 페이지 로딩 대기
    
    # 새 탭으로 전환
    browser.switch_to.window(browser.window_handles[-1])
    
    return browser.current_url 