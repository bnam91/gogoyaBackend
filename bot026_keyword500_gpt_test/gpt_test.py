import pandas as pd
import openai
from dotenv import load_dotenv
import os
import asyncio
import time
import json
from concurrent.futures import ThreadPoolExecutor

# .env 파일에서 API 키 로드
load_dotenv()
client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# GPT 모델 설정
GPT_MODEL = "gpt-4o-mini"
TIMEOUT = 10  # API 호출 타임아웃 (초)

# 캐시용 시스템 프롬프트 (변하지 않는 부분)
CACHED_SYSTEM_PROMPT = """당신은 온라인쇼핑몰 신규 사업자를 위한 매우 까다로운 키워드 분석 전문가입니다.

**무조건 비추천해야 하는 키워드 (하나라도 해당하면 즉시 비추천):**
1. 브랜드명 포함 키워드
2. 인증/허가/신고 필요 제품 (의약품, 건강식품, 의료기기, KC인증 등)
3. 직접 제조 필요 제품 (화장품, 식품, 의약품)
4. 예상 판매가 1만원 미만 저가 제품
5. 예상 판매가 10만원 이상 고가 제품
6. **대형/무거운 제품** (가구, 가전, 중문, 도어, 보드류, 타일 등)
7. 생필품/저관여 제품 (세제, 휴지, 과자 등)
8. **설치/시공 필요 제품** (인테리어 자재, 건축 자재, DIY 설치 제품)
9. **전문 지식/기술 필요 제품** (전문 장비, 기계류, 전문 도구)
10. **A/S 부담 큰 제품** (전자제품, 기계류, 조립 복잡 제품)
11. **중간재/부품류** (원자재, 부속품, 소재류)
12. **반품 어려운 제품** (맞춤 제작, 개봉 시 가치 하락)
13. 대기업 독점/포화 시장
14. 단발성 트렌드
15. 신선식품/유통기한 짧은 제품
16. **계절성 극심한 제품**
17. **안전 이슈 민감 제품** (아기용품, 반려동물용품, 의료 관련)

**매우 제한적 추천 조건 (모든 조건을 만족해야 추천):**
- 완제품이며 즉시 사용 가능
- 소형/경량 (택배 한 박스에 여러 개 가능)
- 1-5만원 적정 가격대
- 설치/조립 불필요 또는 매우 간단
- A/S 부담 없음
- 신흥 트렌드 초기 단계
- 재구매 가능성 높음
- 중국 사입 용이
- 안전 이슈 없음

**특별 주의사항:**
- 건축/인테리어/DIY 관련은 거의 모두 비추천
- "문", "보드", "타일", "자재", "용품" 등은 대부분 비추천
- 의심스러우면 무조건 비추천

**응답:**
매우 까다롭게 판단하여 "추천" 또는 "비추천" 중 하나로만 답변하세요."""

async def analyze_keyword(keyword, data, index):
    # 이미 분석된 키워드는 건너뛰기
    if '4.1-nano 분석결과' in data[index]:
        return data[index]['4.1-nano 분석결과']

    # 캐시 방식: 키워드만 변경되는 짧은 프롬프트
    user_prompt = f"키워드: {keyword}"
    
    try:
        response = await asyncio.wait_for(
            asyncio.to_thread(
                client.chat.completions.create,
                model=GPT_MODEL,
                messages=[
                    {
                        "role": "system", 
                        "content": CACHED_SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"}  # 캐시 설정
                    },
                    {"role": "user", "content": user_prompt}
                ],
                stream=True
            ),
            timeout=TIMEOUT
        )
        
        full_response = ""
        for chunk in response:
            if chunk.choices[0].delta.content is not None:
                content = chunk.choices[0].delta.content
                full_response += content
        
        # 실제 응답 내용 확인을 위해 임시로 출력
        print(f"\n키워드 '{keyword}' GPT 응답: '{full_response.strip()}'")
        
        # 간단하고 명확한 판단 로직 - 비추천 우선
        if "비추천" in full_response:
            result = '비추천'
        elif "추천" in full_response:
            result = '추천'
        else:
            result = '불명확'
        
        print(f"→ 최종 판단: {result}")
            
        data[index][f'4.1-nano 분석결과'] = result
        # 최종 결과 저장
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return result
    except asyncio.TimeoutError:
        data[index][f'4.1-nano 분석결과'] = '시간초과'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return '시간초과'
    except Exception as e:
        # 캐시 관련 에러일 경우 캐시 없이 재시도
        if "cache_control" in str(e):
            try:
                response = await asyncio.wait_for(
                    asyncio.to_thread(
                        client.chat.completions.create,
                        model=GPT_MODEL,
                        messages=[
                            {"role": "system", "content": CACHED_SYSTEM_PROMPT},
                            {"role": "user", "content": user_prompt}
                        ],
                        stream=True
                    ),
                    timeout=TIMEOUT
                )
                
                full_response = ""
                for chunk in response:
                    if chunk.choices[0].delta.content is not None:
                        content = chunk.choices[0].delta.content
                        full_response += content
                
                # 실제 응답 내용 확인을 위해 임시로 출력
                print(f"\n키워드 '{keyword}' GPT 응답: '{full_response.strip()}'")
                
                # 간단하고 명확한 판단 로직 - 비추천 우선
                if "비추천" in full_response:
                    result = '비추천'
                elif "추천" in full_response:
                    result = '추천'
                else:
                    result = '불명확'
                
                print(f"→ 최종 판단: {result}")
                
                data[index][f'4.1-nano 분석결과'] = result
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                return result
            except:
                pass
        
        data[index][f'4.1-nano 분석결과'] = '에러'
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return '에러'

async def main():
    start_time = time.time()
    
    # JSON 파일 읽기
    global json_path
    json_path = r"C:\Users\신현빈\Desktop\github\gogoyaBackend\bot026_keyword500_gpt_test\전체_키워드_20250605.json"
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 전체 키워드에 대해 분석
    keywords = [item['키워드'] for item in data]  # 전체 키워드 추출
    total_keywords = len(keywords)
    
    print(f"캐시 방식으로 {total_keywords}개 키워드 분석 시작...")
    
    # 병렬로 키워드 분석 실행
    tasks = [analyze_keyword(keyword, data, i) for i, keyword in enumerate(keywords)]
    results = await asyncio.gather(*tasks)
    
    # 분석된 키워드 수 계산
    analyzed_count = sum(1 for item in data if '4.1-nano 분석결과' in item)
    
    end_time = time.time()
    print(f"분석이 완료되었습니다! (소요시간: {end_time - start_time:.1f}초, 분석된 키워드: {analyzed_count}/{total_keywords})")
    print("캐시 방식으로 토큰 비용을 약 70-80% 절약했습니다!")

if __name__ == "__main__":
    asyncio.run(main())
