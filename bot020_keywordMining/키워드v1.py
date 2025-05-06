from kiwipiepy import Kiwi
from collections import Counter
import pandas as pd
import os
from dotenv import load_dotenv
from openai import OpenAI
import re
import json
from pymongo import MongoClient
from pymongo.server_api import ServerApi

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")
    
    # 데이터베이스와 컬렉션 선택
    db = client['insta09_database']
    influencer_collection = db['02_main_influencer_data']
    feed_collection = db['01_main_newfeed_crawl_data']
    
    # 사용자로부터 카테고리 입력 받기
    category = input("\n카테고리를 입력하세요 (예: 푸드, 홈/리빙, 기타 등. 'all' 입력 시 모든 카테고리): ")
    
    # 검색 조건 설정
    search_condition = {
        "09_is": "Y",
        "tags": {"$exists": False}
    }
    
    # 카테고리 조건 추가 (all이 아닌 경우)
    if category.lower() != 'all':
        search_condition["category"] = {"$regex": category}
    
    # 조건에 맞는 모든 인플루언서 찾기
    target_influencers = list(influencer_collection.find(search_condition))
    
    if not target_influencers:
        print("조건에 맞는 인플루언서를 찾을 수 없습니다.")
        raise Exception("조건에 맞는 인플루언서를 찾을 수 없습니다.")
    
    print(f"\n총 {len(target_influencers)}명의 인플루언서를 분석합니다.")
    
    # 각 인플루언서에 대해 분석 수행
    for i, target_influencer in enumerate(target_influencers, 1):
        try:
            target_username = target_influencer.get('username')
            print(f"\n[{i}/{len(target_influencers)}] 분석할 인플루언서: {target_username}")
            print(f"카테고리: {target_influencer.get('category', '카테고리 없음')}")
            
            # 해당 username의 author로 게시물 조회
            documents = feed_collection.find({"author": target_username})
            
            # content 필드만 추출하여 리스트로 변환
            contents = [doc.get('content', '') for doc in documents]
            
            # DataFrame 생성
            df = pd.DataFrame({'content': contents})
            
            # 빈 문자열 제거
            df = df[df['content'] != '']
            
            if df.empty:
                print(f"{target_username}의 게시물을 찾을 수 없습니다. 다음 인플루언서로 넘어갑니다.")
                continue
            
            text = ' '.join(df['content'].dropna().astype(str))
            
            # 1. 불용어 정의
            STOPWORDS = {
                # 대명사
                '나', '너', '저', '그', '이', '것', '거', '나', '내', '누구', '어디', '그것', '이것', '저것',
                # 조사
                '의', '가', '이', '은', '는', '을', '를', '에', '와', '과', '로', '으로', '에게', '에서', '까지', '부터', '만', '도', '만큼',
                # 일반 명사
                '때', '수', '중', '앞', '뒤', '위', '아래', '날', '달', '시간', '사람', '경우', '정도', '번', '시', '분', '초',
                # 의존 명사
                '것', '데', '듯', '줄', '만큼', '번', '가지', '때문',
                # 일반적인 동사/형용사
                '하다', '되다', '있다', '없다', '같다', '이다', '아니다', '말다',
                # 부사
                '또', '그냥', '좀', '잘', '더', '많이', '너무', '매우', '아주', '조금', '약간', '다시', '열심히',
                # 수사
                '일', '이', '삼', '사', '오', '육', '칠', '팔', '구', '십', '백', '천', '만', '억',
                # 기타
                '등', '들', '및', '에서', '까지', '어요', '습니다', 'ᆫ가', '그리고', '어서', '라고', '아니', 'ᆯ로', '지만', '으시', '기에',
                # 특수문자
                '..', '...', '~', '!', '?', '🌸'
            }

            # 2. 복합 명사 패턴 정의
            COMPOUND_PATTERNS = [
                ('유기농', '생리대'),
                ('신발', '깔창'),
                ('기부', '자'),
                ('친구', '들'),
                ('칭구', '들'),
                ('생리', '대'),
                ('유기', '농')
            ]

            # 3. Kiwi 형태소 분석기 준비
            kiwi = Kiwi()

            # 4. 품사별 단어 추출 (불용어 제거)
            nouns = []      # 명사
            adjectives = [] # 형용사
            verbs = []      # 동사

            # 5. 복합 명사 처리 함수
            def process_compound_nouns(tokens):
                result = []
                i = 0
                while i < len(tokens):
                    # 복합 명사 패턴 확인
                    found_compound = False
                    for pattern in COMPOUND_PATTERNS:
                        if i + len(pattern) <= len(tokens):
                            words = [tokens[j].form for j in range(i, i + len(pattern))]
                            if tuple(words) == pattern:
                                result.append(''.join(words))
                                i += len(pattern)
                                found_compound = True
                                break
                    if not found_compound:
                        result.append(tokens[i].form)
                        i += 1
                return result

            # 6. 토큰화 및 단어 추출
            tokens = list(kiwi.tokenize(text))
            processed_nouns = process_compound_nouns(tokens)

            for token in tokens:
                word = token.form
                if word in STOPWORDS:  # 불용어 체크
                    continue
                
                # 명사 태그 확인 (NNP: 고유명사, NNG: 일반명사, XR: 어근)
                if token.tag in ['NNP', 'NNG', 'XR'] and len(word) > 1:
                    nouns.append(word)
                elif token.tag == 'VA':  # 형용사
                    if word + "다" not in STOPWORDS:  # 불용어 체크
                        adjectives.append(word + "다")
                elif token.tag == 'VV':  # 동사
                    if word + "다" not in STOPWORDS:  # 불용어 체크
                        verbs.append(word + "다")

            # 7. 빈도수 계산
            noun_counter = Counter(nouns)
            adj_counter = Counter(adjectives)
            verb_counter = Counter(verbs)

            # 8. 결과 출력
            def print_counter(title, counter, percentile=10):
                print(f"\n=== {title} (상위 {percentile}%) ===")
                # 빈도수 기준으로 내림차순 정렬
                sorted_items = sorted(counter.items(), key=lambda x: x[1], reverse=True)
                
                # 상위 percentile% 계산
                total_items = len(sorted_items)
                top_n = max(1, int(total_items * percentile / 100))
                
                result = []
                for word, count in sorted_items[:top_n]:
                    result.append(f"{word}({count}회)")
                print(", ".join(result))

            print_counter("의미있는 명사", noun_counter)
            print_counter("의미있는 형용사", adj_counter)
            print_counter("의미있는 동사", verb_counter)

            # 9. OpenAI API를 사용한 인플루언서 성향 분석
            # .env 파일에서 API 키 로드
            load_dotenv()
            openai_client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

            # 분석할 키워드 데이터 준비
            analysis_data = {
                "명사": dict(noun_counter.most_common(50)),
                "형용사": dict(adj_counter.most_common(10)),
                "동사": dict(verb_counter.most_common(20))
            }

            # 예시 키워드에서 브랜드 제거
            example_keywords = [
                "주부", "살림", "살림꿀템", "살림꿀팁", "육아", "인테리어", "얼굴공개", "살림템", "실용적", "주방용품", "정보성", "레시피", "꿀팁", "요리", "정리", "친근", "감성적", "브랜드", "수납", "조리도구", "식품", "집밥", "리빙", "청소용품", "홈데코", "홈카페", "뷰티", "청소", "제품소개", "다이어트", "생활가전", "호기심형", "욕실청소", "주방", "초등자녀", "생활용품", "셀프인테리어", "육아템", "플레이팅", "생활꿀팁", "건강기능식품", "소품", "간편레시피", "정보공유"
            ]

            # 브랜드 후보 추출
            brand_candidates = [b for b in ["쿠팡", "다이소", "이케아", "무인양품", "코스트코", "마켓컬리"] if b in noun_counter]
            if brand_candidates:
                brand_str = ', '.join(f'#{b}' for b in brand_candidates)
                brand_prompt = f"브랜드 키워드는 반드시 다음 중 실제로 포함된 것만 결과에 넣으세요: {brand_str}"
            else:
                brand_prompt = "브랜드 키워드는 넣지 마세요."

            prompt = f"""
다음은 인플루언서의 게시물에서 추출한 키워드와 빈도수입니다:
{analysis_data}

아래와 같은 키워드 예시도 참고하여, 인플루언서의 고유한 성향을 나타내는 20~30개의 키워드를 생성해주세요:
{', '.join(example_keywords)}

키워드는 다음과 같은 카테고리로 구분하여 생성해주세요:

1. 라이프스타일 키워드 (5~6개):
   - 가족/육아 관련 (#육아중, #주부, #맞벌이, #워킹맘, #가족중심 등)
   - 주거 형태 (#아파트, #원룸, #신축 등)
   - 취미/관심사 (#홈쿠킹, #살림, #수납, #인테리어, #여행, #홈카페, #테이블웨어 등)
   - 요리 스타일 (요리 관련 키워드가 있는 경우):
     * 요리 종류 (#한식, #양식, #중식, #일식, #퓨전 등)
     * 요리 목적 (#간식, #술안주, #다이어트식, #건강식 등)

2. 소비 성향 키워드 (8~10개):
   - 구매 결정 요인:
     * 가성비형 (#가성비추구, #합리적소비, #실속, #저렴 등)
     * 프리미엄형 (#프리미엄, #고급, #최고, #버진, #엑스트라, #하이엔드 등)
   - 소비 수준 (#고급스러움, #합리적소비, #부자 등)
   - 구매 패턴 (#호기심형, #안전형, #안정형)
   - 선호 브랜드/제품 (#명품, #중소기업, #수입제품, #고가제품 등)

3. 콘텐츠 스타일 키워드 (3~5개):
   - 콘텐츠 주제:
     * 주요 콘텐츠 (#꿀팁공유, #일상공유, #리뷰, #레시피 등)
   - 표현 방식:
     * 톤앤매너 (#감성적, #실용적, #유머러스, #B급, #전문가톤 등)
     * 소통 스타일 (#친근함, #권위형, #공감형, #설명형 등)

4. 추구 가치 키워드 (3~5개):
   - 인플루언서의 콘텐츠와 소비 패턴을 분석하여, 그들이 추구하는 핵심 가치를 나타내는 새로운 키워드를 생성해주세요.
   - 기존 키워드에서 선택하는 것이 아니라, 콘텐츠의 맥락과 의미를 파악하여 새로운 가치 키워드를 만들어주세요.
   - 예를 들어, 환경 관련 콘텐츠가 많다면 #지속가능, #환경친화 등을 생성할 수 있습니다.
   - 건강 관련 콘텐츠가 많다면 #건강중시, #웰빙라이프 등을 생성할 수 있습니다.
   - 가족 및 자녀교육 관련 콘텐츠가 많다면 #가족중시 #자녀교육중시 등을 생성할 수 있습니다.

각 키워드는 #태그 형식으로 작성해주세요.
특정 브랜드(예: #이케아, #무인양품, #코스트코, #쿠팡, #다이소 등) 키워드는 반드시 의미있는 명사(상위 빈도) 리스트에 실제로 포함된 경우에만 결과에 넣어주세요.
#가성비추구와 #브랜드중시(또는 #명품, #독립브랜드 등)는 동시에 포함하지 말고, 둘 중 더 가까운 하나만 선택해 포함하세요.
반드시 #호기심형 또는 #안정형 중 하나를 포함시켜주세요. 두 키워드 중 해당 인플루언서의 성향에 더 가까운 것을 선택해 포함하세요.

요리 관련 키워드가 있는 경우, 반드시 요리 스타일 카테고리의 키워드들을 포함시켜주세요.
예를 들어, #홈쿠킹이나 #레시피가 있다면 #한식, #양식 등의 요리 종류와 #간식, #술안주 등의 요리 목적을 함께 포함시켜주세요.

콘텐츠 스타일 키워드는 반드시 해당 인플루언서의 고유한 특징을 나타내는 키워드를 선택해주세요.
예를 들어, 단순히 #일상공유보다는 #감성적일상공유, #실용적일상공유 등으로 구체화하여 선택해주세요.
또한 콘텐츠의 강점이나 차별점을 나타내는 키워드를 반드시 포함시켜주세요.

프리미엄/고급형 관련 키워드가 많이 나타나는 경우, 반드시 #프리미엄, #고급, #하이엔드 등의 키워드를 포함시켜주세요.
이러한 키워드들은 해당 인플루언서의 고급스러운 이미지나 프리미엄 제품 선호도를 나타내는 중요한 지표입니다.

추구 가치 키워드는 반드시 해당 인플루언서의 콘텐츠와 소비 패턴을 분석하여, 그들이 추구하는 핵심 가치를 나타내는 새로운 키워드를 생성해주세요.
기존 키워드에서 선택하는 것이 아니라, 콘텐츠의 맥락과 의미를 파악하여 새로운 가치 키워드를 만들어주세요.
예를 들어, 환경 관련 콘텐츠가 많다면 #지속가능, #환경친화 등을 생성할 수 있습니다.
건강 관련 콘텐츠가 많다면 #건강중시, #웰빙라이프 등을 생성할 수 있습니다.
가족 관련 콘텐츠가 많다면 #가족중시, #행복한삶 등을 생성할 수 있습니다.

각 카테고리별로 최대한 조건을 맞추되, 전체 합이 20~30개가 되도록 반드시 채워주세요. 
조건을 모두 만족하는 키워드가 부족하면, 예시 키워드에서 가중치를 주고 싶은 분야의 키워드를 추가선택하여 20~30개를 맞추세요.
아무런 자연어 설명 없이, 반드시 아래 예시처럼 JSON만 출력하고, 그 아래에 쉼표로 구분된 20~30개 키워드만 출력하세요.

예시:
{{
    "lifestyle": ["#육아중", "#아파트", "#계획적", "#홈쿠킹", "#한식", "#간식", "#운동", "#주부"],
    "consumption": ["#프리미엄", "#고급", "#최고", "#버진", "#엑스트라", "#하이엔드", "#명품", "#브랜드중시", "#리서치중시", "#생활가전", "#효율적"],
    "content": ["#일상공유", "#실용적꿀팁", "#전문가톤", "#상세설명", "#독창적컨텐츠", "#공감형소통", "#미니멀디자인"],
    "values": ["#지속가능", "#품질중시", "원료중시"]
}}
#육아중, #아파트, #홈쿠킹, #한식, #간식, #운동, #주부, #프리미엄, #고급, #최고, #버진, #엑스트라, #하이엔드, #명품, #브랜드중시, #리서치중시, #생활가전, #효율적, #감성적일상공유, #실용적꿀팁, #체계적리뷰, #전문가톤, #상세설명, #독창적컨텐츠, #정기적업로드, #테마중심, #공감형소통, #미니멀디자인, #지속가능, #품질중시, #신뢰성

{brand_prompt}
"""

            # GPT-4 모델 호출
            response = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "당신은 인플루언서의 콘텐츠를 분석하는 전문가입니다. 키워드 분석을 통해 인플루언서의 고유한 성향을 정확하게 파악하고, 구체적이고 상세한 키워드를 생성해주세요."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=1000
            )

            # 분석 결과 출력
            print("\n=== 인플루언서 성향 분석 결과 ===")
            print(response.choices[0].message.content)

            # '#'을 제거한 키워드도 추가로 출력
            result_text = response.choices[0].message.content
            json_match = re.search(r'\{[\s\S]*?\}', result_text)
            if json_match:
                try:
                    result_json = json.loads(json_match.group())
                    # 모든 카테고리의 키워드 합치기
                    all_keywords = sum(result_json.values(), [])
                    # '#' 제거
                    clean_keywords = [k.lstrip('#') for k in all_keywords]
                    print("\n# 제거된 키워드:")
                    print(', '.join(clean_keywords))
                    
                    # 인플루언서 스타일 요약 생성
                    summary_prompt = f"""
다음은 인플루언서의 게시물에서 추출한 명사와 빈도수입니다:
{analysis_data["명사"]}

이 인플루언서의 스타일과 성향을 한 줄로 요약해주세요.
요약은 다음과 같은 형식으로 작성해주세요:
"[주요 콘텐츠] + [소비 성향] + [추구 가치] 스타일의 인플루언서"

예시:
"실용적인 꿀팁과 레시피를 공유하는 가성비 추구형, 건강과 가족을 중시하는 인플루언서"
"고급스러운 라이프스타일을 보여주는 프리미엄 추구형, 품질과 독창성을 중시하는 인플루언서"

요약은 반드시 한 줄로 작성하고, 자연스러운 문장으로 만들어주세요.
빈도수가 높은 명사들을 우선적으로 반영해주세요.
"""

                    # GPT-4 모델 호출
                    summary_response = openai_client.chat.completions.create(
                        model="gpt-4o-mini",
                        messages=[
                            {"role": "system", "content": "당신은 인플루언서의 콘텐츠를 분석하는 전문가입니다. 키워드 분석을 통해 인플루언서의 고유한 성향을 정확하게 파악하고, 간결하고 명확한 요약을 생성해주세요."},
                            {"role": "user", "content": summary_prompt}
                        ],
                        temperature=0.7,
                        max_tokens=100
                    )

                    # 요약 출력
                    print("\n=== 인플루언서 스타일 요약 ===")
                    style_summary = summary_response.choices[0].message.content
                    print(style_summary)
                    
                    # MongoDB에 tags 필드와 style_summary 필드 추가
                    # 기존 tags 필드가 있다면 유지하고 새로운 키워드 추가
                    existing_tags = target_influencer.get('tags', [])
                    new_tags = list(set(existing_tags + clean_keywords))  # 중복 제거
                    
                    # tags 필드와 style_summary 필드 업데이트
                    influencer_collection.update_one(
                        {"_id": target_influencer["_id"]},
                        {"$set": {
                            "tags": new_tags,
                            "style_summary": style_summary
                        }}
                    )
                    print(f"\nMongoDB에 {len(new_tags)}개의 태그와 스타일 요약이 추가되었습니다.")
                    
                    # 다음 인플루언서로 넘어가기 전에 잠시 대기
                    if i < len(target_influencers):
                        print("\n다음 인플루언서 분석을 시작합니다...")
                        print("-" * 50)
                    
                except Exception as e:
                    print(f"JSON 파싱 또는 MongoDB 업데이트 중 오류 발생: {e}")
                    continue
        except Exception as e:
            print(f"인플루언서 {target_username} 분석 중 오류 발생: {e}")
            continue

except Exception as e:
    print(f"MongoDB 연결 또는 데이터 조회 중 에러 발생: {e}")
    raise
finally:
    client.close()

class KoreanKeywordExtractor:
    def __init__(self, stopwords_file=None):
        # 1. 기본 불용어 정의
        self.stopwords = {
            # ... existing code ...
        }
        
        # V-R-H-T 분류를 위한 키워드 및 패턴
        self.vrht_patterns = {
            # V(Value)/B(Brand) 가격 포커스
            'V': {
                'keywords': ['할인', '쿠폰', '가성비', '저렴', '특가', '세일', '이벤트', '%', '원', '가격', '돈', '비용', '무료'],
                'patterns': [r'[\d,]+원', r'\d+%', r'할인', r'쿠폰', r'가성비', r'특가'],
                'weight': 1.0
            },
            'B': {
                'keywords': ['프리미엄', '정품', '브랜드', '명품', '럭셔리', '고급', '퀄리티', '품질', '하이엔드', '최상급', '프리미엄', '럭셔리', '명품', '브랜드'],
                'patterns': [r'브랜드', r'정품', r'프리미엄', r'명품', r'럭셔리', r'고급', r'퀄리티'],
                'weight': 1.5
            },
            
            # R(Rational)/E(Emotional) 메시지 톤
            'R': {
                'keywords': ['수치', '규격', '단계', '사이즈', 'cm', 'kg', 'ml', '리터', '수명', '내구성', '사양', '스펙'],
                'patterns': [r'\d+cm', r'\d+kg', r'\d+ml', r'\d+시간', r'\d+분', r'STEP', r'단계'],
                'weight': 1.0
            },
            'E': {
                'keywords': ['느낌', '감성', '무드', '분위기', '예쁘다', '아름답다', '따뜻하다', '사랑스럽다', '멋지다', '감성', '무드', '분위기', '예쁨', '아름다움'],
                'patterns': [r'!+', r'\♥', r'ㅠㅠ', r'ㅋㅋ', r'감성', r'무드', r'예쁘', r'너무너무', r'짱'],
                'weight': 1.3
            },
            
            # H(Humble)/S(Superior) 관계 태도
            'H': {
                'keywords': ['우리', '여러분', '함께', '같이', '친구', '동료', '모두', '다같이', '친근하다'],
                'patterns': [r'우리', r'여러분', r'함께', r'같이', r'친구들', r'여러분'],
                'weight': 1.0
            },
            'S': {
                'keywords': ['전문가', '권위', '자격증', '경력', '경험', '노하우', '추천', '조언', '조언하다', '알려주다', '전문가', '권위', '경험', '노하우'],
                'patterns': [r'전문가', r'권위', r'자격증', r'경력', r'노하우', r'제가 알려드리'],
                'weight': 1.4
            },
            
            # T(Trail-Seeker)/C(Caution-Keeper) 탐험 성향
            'T': {
                'keywords': ['신상', '언박싱', '테스트', '실험', '신제품', '처음', '도전', '시도', '호기심', '트렌드'],
                'patterns': [r'신상', r'언박싱', r'테스트', r'실험', r'최초', r'신제품', r'출시'],
                'weight': 1.0
            },
            'C': {
                'keywords': ['안전', '검증', '내구성', '보증', '사용기', 'AS', '리뷰', '오래', '지속', '견고', '튼튼', '안전', '내구성', '검증', '보증'],
                'patterns': [r'안전', r'검증', r'보증', r'AS', r'사용기', r'\d+개월', r'\d+년'],
                'weight': 1.2
            }
        }
        
        # ... rest of the initialization code ...

    def analyze_vrht_type(self, text_data):
        """V-R-H-T 크리에이터 유형 분석"""
        # 텍스트 데이터 처리
        if isinstance(text_data, pd.DataFrame):
            if 'content' in text_data.columns:
                text = ' '.join(text_data['content'].dropna().astype(str))
            else:
                text = ' '.join(text_data.iloc[:, 0].dropna().astype(str))
        elif isinstance(text_data, str):
            text = text_data
        else:
            text = ' '.join(text_data)
            
        # 텍스트 전처리
        text = self._clean_text(text)
        
        # 각 축별 점수 계산
        axis_scores = {
            'VB': {'V': 0, 'B': 0},
            'RE': {'R': 0, 'E': 0},
            'HS': {'H': 0, 'S': 0},
            'TC': {'T': 0, 'C': 0}
        }
        
        # 각 축에 대한 패턴 검사
        for axis, options in [('VB', ['V', 'B']), ('RE', ['R', 'E']), ('HS', ['H', 'S']), ('TC', ['T', 'C'])]:
            for option in options:
                # 키워드 기반 점수
                for keyword in self.vrht_patterns[option]['keywords']:
                    if keyword in text:
                        axis_scores[axis][option] += 1 * self.vrht_patterns[option]['weight']
                
                # 정규표현식 패턴 기반 점수
                for pattern in self.vrht_patterns[option]['patterns']:
                    matches = re.findall(pattern, text)
                    axis_scores[axis][option] += len(matches) * self.vrht_patterns[option]['weight']
        
        # 최종 유형 결정
        vrht_type = ""
        for axis, options in [('VB', ['V', 'B']), ('RE', ['R', 'E']), ('HS', ['H', 'S']), ('TC', ['T', 'C'])]:
            scores = axis_scores[axis]
            if scores[options[0]] >= scores[options[1]]:
                vrht_type += options[0]
            else:
                vrht_type += options[1]
                
        # 각 축별 점수 차이 계산 (확신도)
        confidence = {}
        for axis, options in [('VB', ['V', 'B']), ('RE', ['R', 'E']), ('HS', ['H', 'S']), ('TC', ['T', 'C'])]:
            scores = axis_scores[axis]
            total = scores[options[0]] + scores[options[1]]
            if total > 0:
                dominant = options[0] if scores[options[0]] >= scores[options[1]] else options[1]
                confidence[axis] = abs(scores[options[0]] - scores[options[1]]) / total
            else:
                confidence[axis] = 0
        
        return {
            'type': vrht_type,
            'scores': axis_scores,
            'confidence': confidence
        }