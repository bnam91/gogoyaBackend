import requests
import re
import json
import base64
import os
from bs4 import BeautifulSoup
import logging
import traceback

def analyze_links_with_gpt4omini(links, brand_name, api_key):
    """GPT-4-mini를 사용하여 링크 분석"""
    logger = logging.getLogger()
    
    # 분석 요청 메시지 구성
    messages = [
        {"role": "system", "content": f"당신은 {brand_name} 브랜드의 공식 홈페이지를 찾는 것을 도와주는 AI 비서입니다. 사용자가 제공한 링크 목록에서 최적의 공식 웹사이트를 선택해주세요."},
        {"role": "user", "content": f"다음은 '{brand_name}' 브랜드와 관련된 링크 목록입니다. 이 중에서 '{brand_name}'의 공식 홈페이지로 가장 적합한 URL을 선택해 주세요. 광고 링크(adcr.naver.com)는 제외하고, 브랜드의 공식 도메인을 우선적으로 선택해야 합니다.\n\n" + 
                                    "\n".join([f"{i+1}. 텍스트: {link['text']}\n   URL: {link['href']}" for i, link in enumerate(links[:10])]) +
                                    "\n\n가장 적합한 공식 홈페이지 URL을 선택하고, 왜 그것이 최선의 선택인지 간략히 설명해 주세요. 응답 형식: '공식 홈페이지는 URL입니다. 이유: 설명'"}
    ]
    
    try:
        # API 설정
        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        # 요청 데이터 구성
        data = {
            "model": "gpt-4o-mini",
            "messages": messages,
            "temperature": 0.2,
            "max_tokens": 500
        }
        
        # API 요청
        response = requests.post(url, headers=headers, json=data)
        
        # 응답 확인
        if response.status_code == 200:
            return response.json()
        else:
            # 오류 상세 내용 로깅
            logger.error(f"API 응답 오류 (상태 코드: {response.status_code}):")
            logger.error(f"응답 내용: {response.text}")
            
            # 오류 내용 반환
            return {"error": response.text, "status_code": response.status_code}
            
    except requests.exceptions.RequestException as e:
        # 요청 예외 처리 - 상세 오류 메시지 로깅
        logger.error(f"API 요청 중 오류 발생: {str(e)}")
        logger.error(f"오류 세부 정보: {traceback.format_exc()}")
        return {"error": str(e)}
    except Exception as e:
        # 기타 예외 처리 - 상세 오류 메시지 로깅
        logger.error(f"예상치 못한 오류 발생: {str(e)}")
        logger.error(f"오류 세부 정보: {traceback.format_exc()}")
        return {"error": str(e)}

def analyze_phone_with_gpt4omini(soup, brand_name, url, api_key):
    """전화번호 분석 함수"""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    # 페이지 텍스트 중 일부만 추출 (너무 길면 토큰이 많이 소모됨)
    # 고객센터 관련 키워드가 포함된 부분 중심으로 추출
    keywords = ['고객센터', '고객지원', '전화번호', '콜센터', '상담전화', '고객상담', '문의', '상담', 'cs', 'contact', 
               '상호명', '사업자', '주소', '이메일', 'COMPANY', 'OWNER', 'TEL', 'ADD', 'ADDRESS', '대표', '대표자']
    relevant_texts = []
    
    # 푸터 영역 특별 처리 (많은 사이트에서 푸터에 회사 정보가 있음)
    footer_elements = soup.select('footer, .footer, #footer, .bt_info, .company_info, .shop_info, .info')
    for footer in footer_elements:
        if footer:
            relevant_texts.append(footer.get_text(" ", strip=True)[:500])
    
    # 키워드 기반 검색도 계속 수행
    for keyword in keywords:
        elements = soup.find_all(string=lambda string: string and keyword in string.lower())
        for element in elements:
            # 해당 요소와 상위 3개 레벨의 부모 요소 텍스트 추출
            current = element
            for _ in range(3):
                if current.parent:
                    relevant_texts.append(current.parent.get_text(" ", strip=True)[:300])
                    current = current.parent
                else:
                    break
    
    # 중복 제거하고 합치기
    all_text = "\n\n".join(list(set(relevant_texts)))
    
    # 페이지 전체 텍스트가 없으면 일부만 가져오기
    if not all_text:
        all_text = soup.get_text(" ", strip=True)[:1000]  # 처음 1000자만
    
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {
                "role": "system",
                "content": "한국어로 답변해주세요. 당신은 웹페이지에서 기업 정보를 추출하는 전문가입니다. 다음 형식의 JSON으로 응답해야 합니다:\n{\n\"company_name\": \"회사명\",\n\"customer_service_number\": \"전화번호\",\n\"business_address\": \"주소\",\n\"email\": \"이메일\"\n}\n모든 필드는 찾을 수 없으면 \"정보 없음\"으로 표시하세요. 주의: 주소는 '서울강남-05813호'와 같은 형식의 통신판매업신고번호가 아니라 실제 우편 주소(예: '서울시 강남구 삼성동 123-45')여야 합니다."
            },
            {
                "role": "user",
                "content": f"다음은 '{brand_name}' 브랜드의 공식 홈페이지({url}) 내용 중 일부입니다. 이 내용에서 상호명, 고객센터/상담 전화번호, 주소, 이메일을 찾아 JSON 형식으로 응답해주세요.\n\n{all_text}"
            }
        ],
        "max_tokens": 200
    }
    
    response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
    return response.json()

def get_captcha_answer(image_path, question, api_key):
    """GPT-4o에 이미지 전송하여 답변 얻기"""
    # 이미지를 base64로 인코딩
    with open(image_path, "rb") as image_file:
        encoded_image = base64.b64encode(image_file.read()).decode('utf-8')
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    payload = {
        "model": "gpt-4o",
        "messages": [
            {
                "role": "system",
                "content": "당신은 이미지를 정확하게 해석하는 AI 도우미입니다. 이미지에 표시된 내용을 보고 질문에 대한 정확한 답변만 숫자 혹은 단어로 제공해주세요."
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": f"다음 이미지를 보고 질문에 답해주세요. 질문: {question}. 필요한 내용만 정확히 답변해주세요."},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{encoded_image}"
                        }
                    }
                ]
            }
        ],
        "max_tokens": 100
    }
    
    response = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload)
    result = response.json()
    
    if "choices" in result:
        answer = result["choices"][0]["message"]["content"].strip()
        
        # 토큰 사용량 계산 및 출력
        if "usage" in result:
            # 토큰 사용량 계산
            prompt_tokens = result["usage"]["prompt_tokens"]
            completion_tokens = result["usage"]["completion_tokens"]
            total_tokens = result["usage"]["total_tokens"]
            
            # 토큰당 가격 (달러) - GPT-4o 가격
            prompt_price_per_token = 0.00001  # $0.01 / 1K tokens
            completion_price_per_token = 0.00003  # $0.03 / 1K tokens
            
            # 달러 비용 계산
            prompt_cost_usd = prompt_tokens * prompt_price_per_token
            completion_cost_usd = completion_tokens * completion_price_per_token
            total_cost_usd = prompt_cost_usd + completion_cost_usd
            
            # 한화로 환산 (환율: 1 USD = 약 1,350 KRW로 가정)
            exchange_rate = 1350
            total_cost_krw = total_cost_usd * exchange_rate
            
            print("\nGPT-4o 캡챠 토큰 사용량:")
            print(f"프롬프트 토큰: {prompt_tokens}")
            print(f"응답 토큰: {completion_tokens}")
            print(f"총 토큰: {total_tokens}")
            print(f"예상 비용: ${total_cost_usd:.6f} (약 {total_cost_krw:.2f}원)")
        
        # 숫자만 추출
        numeric_answer = ''.join(filter(str.isdigit, answer))
        return numeric_answer if numeric_answer else answer
    else:
        print("API 응답 오류:", result)
        return "3"  # 오류 시 기본값 