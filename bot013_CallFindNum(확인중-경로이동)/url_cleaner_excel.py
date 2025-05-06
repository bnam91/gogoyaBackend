from urllib.parse import urlparse, urlunparse
import re
import pandas as pd

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
        print(f"URL 정제 중 오류 발생: {e}")
        return url

# 테스트 코드
if __name__ == "__main__":
    # 엑셀 파일 읽기
    try:
        df = pd.read_excel('brand_phone_data_20250407_192028_01.xlsx')
        urls = df.iloc[:, 2].tolist()  # C열 (인덱스 2)의 모든 URL 가져오기
        
        print(f"총 {len(urls)}개의 URL을 처리합니다.\n")
        
        for url in urls:
            if pd.isna(url):  # NaN 값 건너뛰기
                continue
            cleaned = clean_url(url)
            print(f"원본: {url}")
            print(f"정제: {cleaned}\n")
            
    except Exception as e:
        print(f"엑셀 파일 처리 중 오류 발생: {e}") 