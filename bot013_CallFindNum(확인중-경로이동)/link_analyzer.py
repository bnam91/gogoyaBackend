"""
이 모듈은 웹페이지에서 브랜드 관련 공식 홈페이지 링크를 추출하는 기능을 제공합니다.

주요 기능:
1. HTML 웹페이지에서 브랜드 관련 공식 링크 추출
2. 관련성이 높은 링크에 점수 부여 및 우선순위 정렬
3. 소셜 미디어 등 불필요한 링크 필터링

BeautifulSoup 라이브러리를 사용하여 웹페이지를 파싱하고, 
브랜드명과 관련 키워드를 기반으로 관련성이 높은 공식 홈페이지 링크를 찾아냅니다.
"""

import requests
import re
from bs4 import BeautifulSoup

def extract_official_links(soup, brand_name):
    """공식 홈페이지 링크 추출 함수"""
    potential_links = []
    keywords = ['공식', '홈페이지', '스토어', '몰', '사이트', 'official', brand_name]
    
    # 제외할 URL 패턴 목록
    exclude_patterns = [
        'blog.naver.com',
        'talk.naver.com', 
        'in.naver.com',       # 추가: 네이버 인플루언서/인사이트 콘텐츠
        'shoppinglive.naver.com', 
        'namu.wiki',
        '/ns/home',
        'instagram',
        'youtube.com',
        'facebook.com',
        'twitter.com',
        'musinsa',
        'mkt.naver.com',      # 추가: 네이버 마케팅 플랫폼
        'enuri.com',          # 추가: 에누리
        'ssg.com',            # 추가: SSG
        'cr3.shopping.naver.com',  # 추가: 네이버 쇼핑 검색 관련
        'news',
        'wikipedia',
        'tiktok',
        'instagram.com'
        'jobkorea',        
        'terms.naver.com',
        'go.kr',
        'aadome.com',
        'mcst.go.kr',
        'pay.naver.com',
        'www.e-himart.co.kr/app/',
        'www.wadiz.kr/web/',
        'lotteimall.com/display',
        'samdae500.com',
        'nid.naver.com',
        'ssfshop.com',
        'moneypin.biz',
        'cafe.naver.com',
        'search.naver.com',
        'inflow.pay.naver.com',
        'map.naver.com',
        'terms.naver.com',
        'ogqmarket.naver.com',
        'ader.naver.com',
        'adcr.naver.com',
        '.ac.kr',
        '.org',
        '.or.kr',
        'ko.dict.naver.com',
        'www.costco.co.kr',
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
    
    # 모든 링크 검사
    for a_tag in soup.find_all('a', href=True):
        link_text = a_tag.text.lower()
        href = a_tag['href']
        
        # 제외 패턴이 URL에 포함되어 있는지 확인
        should_exclude = any(pattern in href for pattern in exclude_patterns)
        
        # 제외 패턴이 없고, 키워드가 텍스트나 href에 포함된 링크 찾기
        if not should_exclude and (any(keyword.lower() in link_text for keyword in keywords) or \
           any(keyword.lower() in href for keyword in keywords)):
            potential_links.append({
                'text': a_tag.text,
                'href': href,
                'score': sum(1 for keyword in keywords if keyword.lower() in link_text.lower() or keyword.lower() in href.lower())
            })
    
    # 점수에 따라 정렬
    potential_links.sort(key=lambda x: x['score'], reverse=True)
    return potential_links 