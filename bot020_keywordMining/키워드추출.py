from kiwipiepy import Kiwi
from collections import Counter
import pandas as pd

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

# 3. Excel 파일에서 데이터 읽기
df = pd.read_excel('jiemma_posts.xlsx')
text = ' '.join(df['content'].dropna().astype(str))

# 4. Kiwi 형태소 분석기 준비
kiwi = Kiwi()

# 5. 품사별 단어 추출 (불용어 제거)
nouns = []      # 명사
adjectives = [] # 형용사
verbs = []      # 동사

# 6. 복합 명사 처리 함수
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

# 7. 토큰화 및 단어 추출
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

# 8. 빈도수 계산
noun_counter = Counter(nouns)
adj_counter = Counter(adjectives)
verb_counter = Counter(verbs)

# 9. 결과 출력
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