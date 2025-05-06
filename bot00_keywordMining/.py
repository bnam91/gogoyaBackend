from kiwipiepy import Kiwi
from collections import Counter, defaultdict
import pandas as pd
import numpy as np
import re
from sklearn.feature_extraction.text import TfidfVectorizer
import os
from dotenv import load_dotenv
import openai

# .env 파일 로드
load_dotenv()

class KoreanKeywordExtractor:
    def __init__(self, stopwords_file=None):
        # 1. 기본 불용어 정의
        self.stopwords = {
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
            '..', '...', '~', '!', '?', '🌸',
            # 일반적인 SNS 단어들
            '게시글', '답글', '팔로우', '좋아요', '공유', '게시', '업로드', '포스팅', '스토리'
        }
        
        # 불용어 파일이 있으면 추가 로드
        if stopwords_file:
            try:
                with open(stopwords_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        self.stopwords.add(line.strip())
            except Exception as e:
                print(f"불용어 파일 로드 중 오류: {e}")
        
        # 2. 복합 명사 패턴 정의 (사용자 지정 + 자동 감지)
        self.compound_patterns = [
            ('유기농', '생리대'),
            ('신발', '깔창'),
            ('기부', '자'),
            ('친구', '들'),
            ('칭구', '들'),
            ('생리', '대'),
            ('유기', '농')
        ]
        
        # 3. Kiwi 형태소 분석기 준비
        self.kiwi = Kiwi()
        
        # 형태소별 태그 그룹
        self.noun_tags = ['NNG', 'NNP', 'XR']
        self.verb_tags = ['VV']
        self.adj_tags = ['VA']
        
        # 도메인별 키워드 가중치
        self.domain_weights = {
            'business': ['상점', '구매', '세트', '판매', '오픈', '마켓', '브랜드', '스토어', '제품', '출시'],
            'lifestyle': ['라이프', '스타일', '취미', '여행', '일상', '음식', '요리', '홈', '인테리어', '데코'],
            'beauty': ['뷰티', '화장품', '메이크업', '스킨케어', '코스메틱', '헤어', '네일', '피부', '미용', '스타일링'],
            'food': ['맛집', '레시피', '음식', '요리', '베이킹', '디저트', '카페', '맛', '식당', '푸드'],
            'fashion': ['패션', '의류', '옷', '스타일', '코디', '브랜드', '신발', '가방', '액세서리', '쇼핑'],
            'parenting': ['육아', '아이', '엄마', '아빠', '자녀', '출산', '교육', '학교', '양육', '가족'],
            'fitness': ['운동', '헬스', '다이어트', '요가', '필라테스', '건강', '체중', '근육', '트레이닝', '피트니스'],
            'travel': ['여행', '호텔', '관광', '휴가', '비행', '리조트', '액티비티', '투어', '해외', '국내'],
            'tech': ['기술', '아이폰', '갤럭시', '앱', '디지털', '가젯', '리뷰', '스마트', '테크', '전자'],
            'environment': ['환경', '친환경', '지속가능', '에코', '재활용', '자연', '유기농', '제로웨이스트', '그린', '플라스틱'],
            'social_cause': ['기부', '후원', '캠페인', '봉사', '나눔', '사회', '단체', '돕다', '참여', '지원'],
            'pets': ['반려동물', '강아지', '고양이', '펫', '동물', '견', '묘', '사료', '산책', '입양']
        }
        
        # 감성 사전 (확장 가능)
        self.positive_words = {'좋다', '행복하다', '만족하다', '편하다', '마음에들다', '좋아하다', '훌륭하다', '예쁘다', '맛있다', '깔끔하다', '뿌듯하다'}
        self.negative_words = {'나쁘다', '불편하다', '싫다', '불만이다', '아쉽다', '실망하다', '별로다', '속상하다', '힘들다', '어렵다'}
        
        # V-R-H-T 분류를 위한 키워드 및 패턴
        self.vrht_patterns = {
            # V(Value)/B(Brand) 가격 포커스
            'V': {
                'keywords': ['할인', '쿠폰', '가성비', '저렴', '특가', '세일', '이벤트', '%', '원', '가격', '돈', '비용', '무료'],
                'patterns': [r'[\d,]+원', r'\d+%', r'할인', r'쿠폰', r'가성비', r'특가']
            },
            'B': {
                'keywords': ['프리미엄', '정품', '브랜드', '명품', '럭셔리', '고급', '퀄리티', '품질', '하이엔드', '최상급'],
                'patterns': [r'브랜드', r'정품', r'프리미엄', r'명품', r'럭셔리']
            },
            
            # R(Rational)/E(Emotional) 메시지 톤
            'R': {
                'keywords': ['수치', '규격', '단계', '사이즈', 'cm', 'kg', 'ml', '리터', '수명', '내구성', '사양', '스펙'],
                'patterns': [r'\d+cm', r'\d+kg', r'\d+ml', r'\d+시간', r'\d+분', r'STEP', r'단계']
            },
            'E': {
                'keywords': ['느낌', '감성', '무드', '분위기', '예쁘다', '아름답다', '따뜻하다', '사랑스럽다', '멋지다'],
                'patterns': [r'!+', r'\♥', r'ㅠㅠ', r'ㅋㅋ', r'감성', r'무드', r'예쁘', r'너무너무', r'짱']
            },
            
            # H(Humble)/S(Superior) 관계 태도
            'H': {
                'keywords': ['우리', '여러분', '함께', '같이', '친구', '동료', '모두', '다같이', '친근하다'],
                'patterns': [r'우리', r'여러분', r'함께', r'같이', r'친구들', r'여러분']
            },
            'S': {
                'keywords': ['전문가', '권위', '자격증', '경력', '경험', '노하우', '추천', '조언', '조언하다', '알려주다'],
                'patterns': [r'전문가', r'권위', r'자격증', r'경력', r'노하우', r'제가 알려드리']
            },
            
            # T(Trail-Seeker)/C(Caution-Keeper) 탐험 성향
            'T': {
                'keywords': ['신상', '언박싱', '테스트', '실험', '신제품', '처음', '도전', '시도', '호기심', '트렌드'],
                'patterns': [r'신상', r'언박싱', r'테스트', r'실험', r'최초', r'신제품', r'출시']
            },
            'C': {
                'keywords': ['안전', '검증', '내구성', '보증', '사용기', 'AS', '리뷰', '오래', '지속', '견고', '튼튼'],
                'patterns': [r'안전', r'검증', r'보증', r'AS', r'사용기', r'\d+개월', r'\d+년']
            }
        }
        
    def _clean_text(self, text):
        """텍스트 전처리 함수"""
        # HTML 태그 제거
        text = re.sub(r'<[^>]+>', ' ', text)
        # URL 제거
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', ' ', text)
        # 특수문자 제거 (일부 한글 관련 특수문자 유지)
        text = re.sub(r'[^\w\s가-힣ㄱ-ㅎㅏ-ㅣ]', ' ', text)
        # 공백 정리
        text = re.sub(r'\s+', ' ', text).strip()
        return text
        
    def _detect_compound_nouns(self, tokens, window_size=3, min_freq=3):
        """자동 복합 명사 탐지 함수"""
        # 명사 시퀀스 추출
        noun_sequences = []
        current_seq = []
        
        for token in tokens:
            if token.tag in self.noun_tags and token.form not in self.stopwords:
                current_seq.append(token.form)
            else:
                if len(current_seq) >= 2:
                    noun_sequences.append(current_seq)
                current_seq = []
        
        # 마지막 시퀀스 처리
        if len(current_seq) >= 2:
            noun_sequences.append(current_seq)
            
        # 가능한 복합 명사 패턴 생성 및 카운트
        compound_candidates = Counter()
        
        for seq in noun_sequences:
            for i in range(len(seq)):
                for j in range(i+1, min(i+window_size+1, len(seq)+1)):
                    if j-i >= 2:  # 최소 2개 이상의 명사로 구성된 패턴만 고려
                        compound = tuple(seq[i:j])
                        compound_candidates[compound] += 1
        
        # 빈도수가 임계값을 넘는 패턴만 선택
        detected_patterns = [pattern for pattern, count in compound_candidates.items() if count >= min_freq]
        return detected_patterns
        
    def extract_keywords(self, text_data, use_tfidf=True, top_n=10, auto_compound=True):
        """키워드 추출 메인 함수"""
        if isinstance(text_data, pd.DataFrame):
            # DataFrame에서 텍스트 추출
            if 'content' in text_data.columns:
                texts = text_data['content'].dropna().astype(str).tolist()
            else:
                # 첫 번째 열을 사용
                texts = text_data.iloc[:, 0].dropna().astype(str).tolist()
        elif isinstance(text_data, str):
            # 단일 문자열인 경우
            texts = [text_data]
        else:
            # 리스트 또는 기타 이터러블로 가정
            texts = list(text_data)
        
        # 텍스트 전처리
        cleaned_texts = [self._clean_text(text) for text in texts]
        
        # 문서별 토큰화 결과 저장
        all_tokens = []
        for text in cleaned_texts:
            tokens = list(self.kiwi.tokenize(text))
            all_tokens.append(tokens)
            
        # 자동 복합 명사 탐지 (옵션)
        if auto_compound:
            # 모든 토큰을 합쳐서 복합 명사 탐지
            flat_tokens = [token for doc_tokens in all_tokens for token in doc_tokens]
            detected_compounds = self._detect_compound_nouns(flat_tokens)
            self.compound_patterns.extend(detected_compounds)
            print(f"자동 탐지된 복합 명사 패턴: {detected_compounds}")
            
        # 품사별 단어 추출
        doc_nouns = []      # 문서별 명사
        doc_adjectives = [] # 문서별 형용사
        doc_verbs = []      # 문서별 동사
        
        for tokens in all_tokens:
            nouns = []
            adjectives = []
            verbs = []
            
            # 복합 명사 처리
            i = 0
            while i < len(tokens):
                found_compound = False
                for pattern in self.compound_patterns:
                    if i + len(pattern) <= len(tokens):
                        words = [tokens[j].form for j in range(i, i + len(pattern))]
                        if tuple(words) == pattern:
                            compound_word = ''.join(words)
                            if compound_word not in self.stopwords and len(compound_word) > 1:
                                nouns.append(compound_word)
                            i += len(pattern)
                            found_compound = True
                            break
                            
                if not found_compound:
                    token = tokens[i]
                    word = token.form
                    
                    if word not in self.stopwords and len(word) > 1:
                        # 명사 태그 확인
                        if token.tag in self.noun_tags:
                            nouns.append(word)
                        # 형용사 태그 확인
                        elif token.tag in self.adj_tags:
                            adjectives.append(word + "다")
                        # 동사 태그 확인
                        elif token.tag in self.verb_tags:
                            verbs.append(word + "다")
                    i += 1
                    
            doc_nouns.append(nouns)
            doc_adjectives.append(adjectives)
            doc_verbs.append(verbs)
            
        # TF-IDF 계산 (옵션)
        if use_tfidf and len(texts) > 1:
            # 명사에 대한 TF-IDF
            noun_docs = [' '.join(nouns) for nouns in doc_nouns]
            vectorizer = TfidfVectorizer()
            tfidf_matrix = vectorizer.fit_transform(noun_docs)
            feature_names = vectorizer.get_feature_names_out()
            
            # 문서별 TF-IDF 값 추출
            tfidf_scores = defaultdict(float)
            for i in range(len(texts)):
                feature_index = tfidf_matrix[i, :].nonzero()[1]
                tfidf_vals = zip(feature_index, [tfidf_matrix[i, x] for x in feature_index])
                for idx, val in tfidf_vals:
                    word = feature_names[idx]
                    tfidf_scores[word] += val
                    
            # 상위 키워드 선택
            top_nouns = sorted(tfidf_scores.items(), key=lambda x: x[1], reverse=True)[:top_n]
            noun_counter = {word: int(score * 100) for word, score in top_nouns}  # 점수를 빈도로 변환
        else:
            # 단순 빈도수 계산
            all_nouns = [noun for doc in doc_nouns for noun in doc]
            noun_counter = Counter(all_nouns)
            noun_counter = {k: v for k, v in noun_counter.most_common(top_n)}
            
        # 형용사와 동사도 계산
        all_adjectives = [adj for doc in doc_adjectives for adj in doc]
        all_verbs = [verb for doc in doc_verbs for verb in doc]
        
        adj_counter = Counter(all_adjectives)
        verb_counter = Counter(all_verbs)
        
        return {
            'nouns': noun_counter,
            'adjectives': dict(adj_counter.most_common(top_n)),
            'verbs': dict(verb_counter.most_common(top_n))
        }
        
    def extract_phrase_patterns(self, text_data, patterns=None):
        """특정 패턴(예: 명사+조사 조합)을 추출하는 함수"""
        if patterns is None:
            # 기본 패턴: 명사 + 조사(JKS, JKO, JKB)
            patterns = [
                (self.noun_tags, ['JKS']),  # 주격 조사 (이/가)
                (self.noun_tags, ['JKO']),  # 목적격 조사 (을/를)
                (self.noun_tags, ['JKB'])   # 부사격 조사 (에/에서)
            ]
            
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
        
        # 토큰화
        tokens = list(self.kiwi.tokenize(text))
        
        # 패턴 추출
        results = defaultdict(Counter)
        
        for i in range(len(tokens) - 1):
            for first_tags, second_tags in patterns:
                if tokens[i].tag in first_tags and tokens[i+1].tag in second_tags:
                    pattern_key = f"{tokens[i].tag}+{tokens[i+1].tag}"
                    pattern_text = f"{tokens[i].form}+{tokens[i+1].form}"
                    if tokens[i].form not in self.stopwords and len(tokens[i].form) > 1:
                        results[pattern_key][pattern_text] += 1
                        
        return dict(results)
        
    def analyze_sentiment(self, text_data):
        """간단한 감성 분석 (긍정/부정 키워드 기반)"""
        # 텍스트 데이터 처리
        if isinstance(text_data, pd.DataFrame):
            if 'content' in text_data.columns:
                texts = text_data['content'].dropna().astype(str).tolist()
            else:
                texts = text_data.iloc[:, 0].dropna().astype(str).tolist()
        elif isinstance(text_data, str):
            texts = [text_data]
        else:
            texts = list(text_data)
            
        # 문서별 감성 점수 계산
        sentiment_scores = []
        
        for text in texts:
            # 텍스트 전처리
            text = self._clean_text(text)
            
            # 토큰화
            tokens = list(self.kiwi.tokenize(text))
            
            # 형용사 추출
            sentiment_words = []
            for token in tokens:
                if token.tag in self.adj_tags:
                    word = token.form + "다"
                    sentiment_words.append(word)
                    
            # 감성 점수 계산
            pos_count = sum(1 for word in sentiment_words if word in self.positive_words)
            neg_count = sum(1 for word in sentiment_words if word in self.negative_words)
            
            if pos_count + neg_count > 0:
                score = (pos_count - neg_count) / (pos_count + neg_count)
            else:
                score = 0
                
            sentiment_scores.append(score)
            
        return sentiment_scores
    
    def detect_domains(self, keywords):
        """키워드를 기반으로 도메인(주제 영역) 탐지"""
        domain_scores = {}
        
        for domain, domain_keywords in self.domain_weights.items():
            score = 0
            for keyword in keywords:
                if keyword in domain_keywords:
                    score += 1
                # 부분 일치도 점수에 반영 (가중치 줄임)
                elif any(domain_keyword in keyword or keyword in domain_keyword for domain_keyword in domain_keywords):
                    score += 0.5
            domain_scores[domain] = score
            
        # 점수가 있는 도메인만 반환
        return {domain: score for domain, score in domain_scores.items() if score > 0}
    
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
                        axis_scores[axis][option] += 1
                
                # 정규표현식 패턴 기반 점수
                for pattern in self.vrht_patterns[option]['patterns']:
                    matches = re.findall(pattern, text)
                    axis_scores[axis][option] += len(matches)
        
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
    
    def extract_influencer_keywords(self, text_data, num_keywords=20):
        """인플루언서 키워드 추출 통합 함수"""
        # 1. 기본 키워드 추출
        keywords = self.extract_keywords(text_data, use_tfidf=True, top_n=50)
        
        # 2. 감성 분석
        sentiment = np.mean(self.analyze_sentiment(text_data))
        
        # 3. 패턴 분석
        phrases = self.extract_phrase_patterns(text_data)
        
        # 4. VRHT 유형 분석
        vrht_data = self.analyze_vrht_type(text_data)
        
        # 5. 점수 계산을 위한 키워드 풀 생성
        keyword_pool = []
        
        # 명사 추가 (가중치 1.0)
        for word, count in keywords['nouns'].items():
            keyword_pool.append((word, count, 'noun', 1.0))
            
        # 형용사 추가 (가중치 0.8)
        for word, count in keywords['adjectives'].items():
            keyword_pool.append((word, count, 'adj', 0.8))
            
        # 동사 추가 (가중치 0.7)
        for word, count in keywords['verbs'].items():
            keyword_pool.append((word, count, 'verb', 0.7))
            
        # 6. 패턴에서 명사 추출 (가중치 0.9)
        extracted_nouns = set()
        for pattern_type, patterns in phrases.items():
            for pattern_text, count in patterns.items():
                noun = pattern_text.split('+')[0]
                if noun not in self.stopwords and len(noun) > 1:
                    extracted_nouns.add(noun)
                    keyword_pool.append((noun, count, 'pattern_noun', 0.9))
        
        # 7. VRHT 유형 기반 키워드 추가 (가중치 1.2)
        vrht_type = vrht_data['type']
        for char in vrht_type:
            for keyword in self.vrht_patterns[char]['keywords']:
                if keyword not in self.stopwords and len(keyword) > 1:
                    keyword_pool.append((keyword, 3, 'vrht_keyword', 1.2))
        
        # 8. 키워드 점수 계산
        keyword_scores = {}
        for word, count, type_, weight in keyword_pool:
            if word not in keyword_scores:
                keyword_scores[word] = 0
            keyword_scores[word] += count * weight
        
        # 9. 도메인 탐지
        domain_scores = self.detect_domains(keyword_scores.keys())
        
        # 10. 도메인 정보를 기반으로 키워드 가중치 조정
        if domain_scores:
            top_domains = sorted(domain_scores.items(), key=lambda x: x[1], reverse=True)[:3]
            for domain, _ in top_domains:
                for keyword in self.domain_weights[domain]:
                    if keyword in keyword_scores:
                        keyword_scores[keyword] *= 1.2  # 주요 도메인 키워드 가중치 증가
        
        # 11. 상위 키워드 선택
        final_keywords = sorted(keyword_scores.items(), key=lambda x: x[1], reverse=True)[:num_keywords]
        
        # 12. 결과 반환
        return {
            'keywords': [k for k, v in final_keywords],
            'domains': top_domains if domain_scores else [],
            'sentiment': sentiment,
            'vrht_type': vrht_data
        }

def analyze_with_chatgpt(influencer_data):
    """ChatGPT API를 사용하여 인플루언서 성향을 분석하고 키워드를 추출"""
    # OpenAI API 키 설정
    client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
    
    # 분석 데이터 준비
    keywords = influencer_data['keywords']
    domains = [d[0] for d in influencer_data['domains']] if influencer_data['domains'] else []
    sentiment = influencer_data['sentiment']
    vrht_type = influencer_data['vrht_type']['type']
    vrht_description = influencer_data['vrht_type'].get('description', '')
    
    # 프롬프트 구성
    prompt = f"""
    다음은 인플루언서의 분석 데이터입니다:
    
    1. 핵심 키워드: {', '.join(keywords)}
    2. 주요 도메인: {', '.join(domains)}
    3. 감성 점수: {sentiment:.2f}
    4. V-R-H-T 유형: {vrht_type} ({vrht_description})
    
    이 데이터를 바탕으로 인플루언서의 성향을 나타내는 15개의 핵심 키워드를 추출해주세요.
    단순한 명사나 동사가 아닌, 인플루언서의 성향과 특성을 나타내는 의미 있는 키워드를 선택해주세요.
    예시: 가성비, 프리미엄, 실용주의, 감성적, 전문가, 친근함, 호기심, 안정성, 트렌디, 클래식, 럭셔리, 미니멀, 내추럴, 유니크, 에코
    
    설명이나 추가 문장 없이 키워드만 쉼표로 구분하여 나열해주세요.
    """
    
    try:
        # ChatGPT API 호출
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "당신은 인플루언서 분석 전문가입니다. 주어진 데이터를 바탕으로 인플루언서의 성향과 특성을 나타내는 의미 있는 키워드만 추출해주세요."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7,
            max_tokens=100
        )
        
        # 응답 반환
        return response.choices[0].message.content.strip()
        
    except Exception as e:
        print(f"ChatGPT API 호출 중 오류 발생: {e}")
        return "분석을 완료할 수 없습니다."

def analyze_influencer(excel_file, num_keywords=20):
    """인플루언서 분석 실행 함수"""
    # 데이터 로드
    df = pd.read_excel(excel_file)
    
    # 키워드 추출기 초기화
    extractor = KoreanKeywordExtractor()
    
    # 인플루언서 키워드 추출
    print("인플루언서 키워드 추출 중...")
    influencer_data = extractor.extract_influencer_keywords(df, num_keywords=num_keywords)
    
    # 결과 출력
    print("\n=== 인플루언서 핵심 키워드 ===")
    for i, keyword in enumerate(influencer_data['keywords'], 1):
        print(f"{i}. {keyword}")
    
    if influencer_data['domains']:
        print("\n=== 주요 도메인(성향) ===")
        for domain, score in influencer_data['domains']:
            print(f"- {domain}: {score:.1f}")
    
    print(f"\n=== 콘텐츠 감성 분석 ===")
    sentiment = influencer_data['sentiment']
    print(f"감성 점수: {sentiment:.2f} (-1: 매우 부정적, 1: 매우 긍정적)")
    
    # V-R-H-T 크리에이터 유형 출력
    vrht_type = influencer_data['vrht_type']['type']
    print("\n=== V-R-H-T 크리에이터 유형 ===")
    print(f"유형 코드: {vrht_type}")
    
    # 유형별 의미 설명
    vrht_descriptions = {
        'VRHT': '실속 친구형 탐험가 - 가성비·팩트·친근·신제품 테스트',
        'VRHC': '실속 친구형 안전러 - 가성비·팩트·친근·검증 강조',
        'VRST': '실속 권위형 탐험가 - 가성비·팩트·전문가·신제품',
        'VRSC': '실속 권위형 안전러 - 가성비·팩트·전문가·내구성',
        'VEHT': '가성비 감성 친구 탐험가 - 감성 연출·친근·신상',
        'VEHC': '가성비 감성 친구 안전러 - 감성 연출·친근·검증',
        'VEST': '가성비 감성 권위 탐험가 - 감성·권위·신제품',
        'VESC': '가성비 감성 권위 안전러 - 감성·권위·내구성',
        'BRHT': '프리미엄 친구형 탐험가 - 브랜드·스펙·친근·신제품',
        'BRHC': '프리미엄 친구형 안전러 - 브랜드·스펙·친근·검증',
        'BRST': '프리미엄 권위 탐험가 - 하이엔드·전문가·신제품',
        'BRSC': '프리미엄 권위 안전러 - 하이엔드·전문가·내구성',
        'BEHT': '럭셔리 감성 친구 탐험가 - 무드·친근·신제품',
        'BEHC': '럭셔리 감성 친구 안전러 - 무드·친근·검증',
        'BEST': '럭셔리 감성 권위 탐험가 - 무드·권위·신제품',
        'BESC': '럭셔리 감성 권위 안전러 - 무드·권위·내구성'
    }
    
    vrht_description = vrht_descriptions.get(vrht_type, '일반적인 유형')
    print(f"유형 설명: {vrht_description}")
    
    # 축별 확신도 출력
    confidence = influencer_data['vrht_type']['confidence']
    print("\n각 축별 확신도:")
    for axis, conf in confidence.items():
        if axis == 'VB':
            print(f"- 가격 포커스(V/B): {conf:.2f}")
        elif axis == 'RE':
            print(f"- 메시지 톤(R/E): {conf:.2f}")
        elif axis == 'HS':
            print(f"- 관계 태도(H/S): {conf:.2f}")
        elif axis == 'TC':
            print(f"- 탐험 성향(T/C): {conf:.2f}")
    
    # 추가: 핵심 키워드의 의미 추론
    print("\n=== 인플루언서 성향 분석 ===")
    interpret_keywords(influencer_data)
    
    # ChatGPT를 사용한 추가 분석
    print("\n=== ChatGPT 기반 성향 키워드 ===")
    chatgpt_analysis = analyze_with_chatgpt(influencer_data)
    print(chatgpt_analysis)
    
    return influencer_data

def interpret_keywords(influencer_data):
    """키워드를 바탕으로 인플루언서 성향 추론"""
    keywords = influencer_data['keywords']
    domains = [d[0] for d in influencer_data['domains']] if influencer_data['domains'] else []
    sentiment = influencer_data['sentiment']
    vrht_type = influencer_data['vrht_type']['type'] if 'vrht_type' in influencer_data else ""
    
    # 성향 분석을 위한 카테고리 정의
    categories = {
        'business': ['상점', '구매', '세트', '판매', '오픈', '브랜드', '스토어', '제품', '출시', '구매완료', '취소', '추가'],
        'lifestyle': ['라이프', '스타일', '주방', '살림', '생활', '홈', '인테리어', '세트', '선물'],
        'family': ['엄마', '아이', '아빠', '가족', '자녀', '육아'],
        'food': ['맛있다', '요리', '주방', '음식', '먹다', '만들다', '맛'],
        'social_cause': ['기부', '후원', '봉사', '나눔', '사회', '돕다', '참여'],
        'pets': ['반려동물', '강아지', '고양이', '동물', '구조대', '보호']
    }
    
    # 각 카테고리별 점수 계산
    category_scores = {category: 0 for category in categories}
    for keyword in keywords:
        for category, category_keywords in categories.items():
            if keyword in category_keywords:
                category_scores[category] += 1
            elif any(ck in keyword or keyword in ck for ck in category_keywords):
                category_scores[category] += 0.5
    
    # 상위 카테고리 선택
    top_categories = sorted(category_scores.items(), key=lambda x: x[1], reverse=True)[:3]
    for category, score in top_categories:
        if score > 0:
            print(f"- {category.replace('_', ' ').title()}: 관련 키워드 {score:.1f}개 발견")
    
    # 감성 분석 결과에 따른 해석
    if sentiment > 0.3:
        print("- 전반적으로 긍정적인 톤을 사용하는 인플루언서입니다.")
    elif sentiment < -0.3:
        print("- 다소 비판적이거나 부정적인 톤을 사용하는 인플루언서입니다.")
    else:
        print("- 중립적인 톤을 사용하는 인플루언서입니다.")
    
    # V-R-H-T 유형에 따른 추가 해석
    if vrht_type:
        # 첫 번째 문자 (V/B) - 가격 포커스
        if vrht_type[0] == 'V':
            print("- 가성비와 할인, 쿠폰 등을 강조하는 경향이 있습니다.")
        else:  # B
            print("- 프리미엄과 브랜드 가치, 품질을 강조하는 경향이 있습니다.")
        
        # 두 번째 문자 (R/E) - 메시지 톤
        if vrht_type[1] == 'R':
            print("- 수치와 논리, 팩트 기반의 설명을 주로 사용합니다.")
        else:  # E
            print("- 감성과 무드, 감각적 표현을 주로 사용합니다.")
            
        # 세 번째 문자 (H/S) - 관계 태도
        if vrht_type[2] == 'H':
            print("- 친근하고 동등한 위치에서 소통하는 스타일입니다.")
        else:  # S
            print("- 전문가로서의 권위와 지식을 바탕으로 소통하는 스타일입니다.")
            
        # 네 번째 문자 (T/C) - 탐험 성향
        if vrht_type[3] == 'T':
            print("- 신제품, 새로운 트렌드에 호기심이 많고 실험적인 콘텐츠를 선호합니다.")
        else:  # C
            print("- 검증된 제품, 안전성과 내구성을 중시하는 콘텐츠를 선호합니다.")

# 실행 코드
if __name__ == "__main__":
    analyze_influencer('jiemma_posts.xlsx', num_keywords=20)