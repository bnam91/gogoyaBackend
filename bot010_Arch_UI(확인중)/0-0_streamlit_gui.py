# streamlit run 0-0_streamlit_gui.py
# cd "C:\Users\신현빈\Desktop\github\gogoyaBackend\bot010_Arch_UI(확인중)"; streamlit run 0-0_streamlit_gui.py

import streamlit as st
import sys
import os

# 상위 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pymongo import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
import subprocess
import webbrowser
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from auth import get_credentials

# 페이지 레이아웃을 wide 모드로 설정하고 사이드바를 초기에 닫힌 상태로 설정
st.set_page_config(
    layout="wide", 
    page_title="🚀 인스타그램 데이터 분석 대시보드",
    initial_sidebar_state="collapsed"  # 사이드바 초기 상태를 닫힌 상태로 설정
)

# 페이지 여백 줄이기
st.markdown("""
    <style>
        .block-container {
            padding-top: 1.5rem;
            padding-bottom: 0rem;
            padding-left: 1rem;
            padding-right: 1rem;
        }
        /* 라디오 버튼 그룹의 마진 조정 */
        .st-emotion-cache-1n76uvr {
            margin-top: -15px;
        }
        /* 라디오 버튼 그룹 중앙 정렬 */
        div[role="radiogroup"] {
            display: flex;
            justify-content: center;
            gap: 1rem;
        }
        /* 모든 입력 요소 중앙 정렬 */
        .stTextInput, .collection-select, .category-select, .stButton {
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100%;
        }
        /* 입력 필드 너비 조정 */
        .stTextInput > div {
            width: 100%;
            margin-top: 25px;
        }
        /* 컬렉션 selectbox 너비 조정 */
        .collection-select > div {
            width: 100%;
            margin-top: 15px;
        }
        /* 카테고리 selectbox 너비 조정 */
        .category-select > div {
            width: 100%;
            margin-top: 25px;
        }
        /* 버튼 너비 조정 */
        .stButton > button {
            width: 100%;
            margin-top: 25px;
        }
        [data-testid=stSidebar] {
            background-color: #f0f2f6;
            padding-top: 1rem;
        }
        .sidebar-button {
            width: 100%;
            margin: 0.2rem 0;
            padding: 0.5rem;
        }
        /* 데이터프레임 스타일링 */
        .stDataFrame {
            margin-top: -18px;
        }
        /* 컬렉션 선택 콤보박스 마진 조정 */
        [data-testid="stSelectbox"] {
            margin-bottom: -10px;
        }
        /* 구분선(divider) 마진 조정 */
        .element-container:has([data-testid="stMarkdownContainer"]) hr {
            margin-top: 10px;
            margin-bottom: 20px;
        }
    </style>
    """, unsafe_allow_html=True)

# 파일 상단에 run_script 함수 추가
def run_script(script_name, button_name, output_container):
    try:
        # 상태 메시지를 표시할 placeholder 생성
        status = st.empty()
        status.write(f'🟢 {button_name} 실행 중...')
        # 시작 토스트 메시지
        st.toast(f'{button_name}이(가) 시작되었습니다.', icon='🚀')
        
        # Windows에서 새 터미널 창을 열어 스크립트 실행
        process = subprocess.Popen(f'start /wait cmd /K python {script_name}', shell=True)
        
        # 프로세스가 종료될 때까지 대기 (터미널 창이 닫힐 때까지)
        process.wait()
        
        # 터미널이 실제로 닫힐 때 상태 업데이트
        status.write(f'🔴 {button_name} 종료됨')
        # 종료 토스트 메시지
        st.toast(f'{button_name}이(가) 종료되었습니다.', icon='✅')
        
    except Exception as e:
        st.error(f'오류가 발생했습니다: {str(e)}')
        st.toast(f'{button_name} 실행 중 오류가 발생했습니다.', icon='❌')
        st.stop()

# 플로잉 추출을 위한 대화상자 함수 정의
@st.dialog("팔로잉 추출")
def following_extract_dialog():
    st.write("인스타그램 프로필 URL 또는 사용자명을 입력하세요:")
    
    # 폼 버튼 중앙 정렬을 위한 CSS
    st.markdown("""
        <style>
            .stFormSubmitButton {
                display: flex;
                justify-content: center;
            }
            .stFormSubmitButton button {
                width: 20% !important;
            }
        </style>
    """, unsafe_allow_html=True)
    
    with st.form("url_input_form"):
        url = st.text_input("", placeholder="예: https://www.instagram.com/username/ 또는 username")
        
        if st.form_submit_button("확인"):
            if url:
                # URL 형식 처리
                if not url.startswith('http'):
                    url = url.strip('/')
                    url = url.replace('instagram.com/', '')
                    url = f'https://www.instagram.com/{url}/'
                
                try:
                    # URL을 임시 파일에 저장
                    with open('temp_profile_url.txt', 'w', encoding='utf-8') as f:
                        f.write(url)
                    # 스크립트 실행 상태 저장
                    st.session_state.run_script = {
                        'script_name': '2-1_following_extract.py',
                        'button_name': '팔로잉 추출',
                        'container': st.empty()
                    }
                    st.rerun()
                except Exception as e:
                    st.error(f"URL 저장 중 오류 발생: {str(e)}")

# MongoDB 연결 설정 전에 사이드바 추가
with st.sidebar:
    st.header("메뉴")
    
    # 데이터 분석 섹션
    with st.expander("📊데이터 분석"):
        if st.button("뉴피드 크롤링", key="today_feed", help="신규 피드를 크롤링합니다."):
            run_script('1-1_newfeed_crawl.py', '뉴피드 크롤링', st.empty())
        if st.button("AI 추출(4o-mini)", key="brand_extract", help="AI가 공구피드인지 분석합니다."):
            run_script('1-2_newfeed_analysis_(4omini).py', 'AI 추출(4o-mini)', st.empty())
        if st.button("🌞오늘의 아이템 찾기", key="today_item", help="최근 n일간 신규 피드에서 아이템을 찾습니다."):
            run_script('1-3_item_today.py', '오늘의 아이템 찾기', st.empty())
    
    # SNS 분석 섹션
    with st.expander("👥SNS 분석"):
        if st.button("팔로잉 추출", key="following_extract", help="계정을 입력하면 팔로잉 추출이 진행됩니다."):
            following_extract_dialog()
        
        # 저장된 스크립트 실행 상태가 있으면 실행
        if 'run_script' in st.session_state:
            script_info = st.session_state.run_script
            run_script(
                script_info['script_name'],
                script_info['button_name'],
                script_info['container']
            )
            del st.session_state.run_script

        if st.button("인플루언서 분석", key="influencer_analysis", help="팔로잉 추출한 인원을 분석 후 팔로우합니다."):
            run_script('2-2_influencer_processing_v2.py', '인플루언서 분석', st.empty())
        if st.button("비전 분석", key="vision_analysis", help="팔로워한 계정 2차 분석(카테고리, 릴스조회수)"):
            run_script('2-3_vision_mod_v1.py', '비전 분석', st.empty())
        if st.button("등급 분류", key="grade_classification", help="등급 분류 후 시트에 저장합니다. 비전분석을 완료해주세요"):
            run_script('st_test3.py', '등급 분류', st.empty())
    
    # DM 관리 섹션
    with st.expander("💌DM 관리"):
        if st.button("DM팔로우시트 열기", key="dm_sheet", help="DM보낼 인원 및 내용을 입력합니다"):
            try:
                sheet_id = '1W5Xz4uaqSPysGLk28w6ybFHkGAPcz19_1BHdOih0Hoc'  # DM 팔로우시트의 ID
                sheet_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}'
                webbrowser.open(sheet_url)
                st.toast('DM 팔로우시트가 새 탭에서 열렸습니다.', icon='✅')
            except Exception as e:
                st.error(f'스프레드시트를 여는 중 오류가 발생했습니다: {str(e)}')
                st.toast('스프레드시트 열기 실패', icon='❌')
        if st.button("DM보내기", key="send_dm"):
            run_script('st_test2.py', 'DM보내기', st.empty())
        if st.button("팔로우하기", key="follow"):
            run_script('st_test2.py', '팔로우하기', st.empty())
            
    # 시트연동 섹션 추가
    with st.expander("📑전화걸기"):
        if st.button("전화걸기 모듈실행", key="load_sheet"):
            run_script('st_test2.py', '시트 연동', st.empty())
        if st.button("시트 보류", key="update_sheet"):
            run_script('st_test2.py', '시트 보류', st.empty())
        if st.button("시트 내보내기", key="export_sheet"):
            run_script('st_test2.py', '시트 내보내기', st.empty())

    # 필터 섹션 추가 - 04_test_item_today_data가 선택되었을 때만 표시
    if 'collection_select' in st.session_state and st.session_state.collection_select == '04_main_item_today_data':
        st.divider()
        st.subheader("필터 옵션")
        
        # 필터 옵션을 위한 컨테이너
        filter_container = st.container()
        
        with filter_container:
            # 필터링 레이블 없이 다중선택 위젯 표시
            filter_options = st.multiselect(
                "아이템 카테고리 제외하기 :",  # 레이블 제거
                options=["", "🍽주방용품&식기", "🛋생활용품&가전", "🥦식품&건강식품", "🧴뷰티&헬스", "👶유아&교육", "👗의류&잡화", "🚗기타"],
                default=["","👶유아&교육", "👗의류&잡화"],  # 기본 필터 설정
                key="filter_multiselect"
            )
            
            # 카테고리 필터 추가            
            col1, col2 = st.columns([3, 1])
            
            with col1:
                category_filter = st.selectbox(
                    "인플루언서 카테고리 선택 :",
                    ['뷰티', '패션', '홈/리빙', '푸드', '육아', '건강', '맛집탐방', '전시/공연', '반려동물', '기타'],
                    placeholder="카테고리 선택",
                    key="category_filter_select"
                )
            
            with col2:
                category_threshold = st.text_input("", placeholder="%", key="category_filter_threshold")
            
            # 필터 적용 버튼 -> 시트에 추가하기 버튼으로 변경
            add_to_sheet = st.button("시트에 추가하기", key="add_to_sheet_button")
            
            # 필터 초기화 버튼
            reset_filter = st.button("필터 초기화", key="reset_filter_button")
            
            # 시트에 추가하기 버튼 클릭 처리
            if add_to_sheet:
                try:
                    # 구글 시트 열기 - brand_today 시트로 직접 이동
                    sheet_id = '1W5Xz4uaqSPysGLk28w6ybFHkGAPcz19_1BHdOih0Hoc'
                    gid = '1773602371'  # brand_today 시트의 gid
                    sheet_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/edit?gid={gid}#gid={gid}'
                    webbrowser.open(sheet_url)
                    st.toast('brand_today 시트가 새 탭에서 열렸습니다.', icon='✅')
                except Exception as e:
                    st.error(f'스프레드시트를 여는 중 오류가 발생했습니다: {str(e)}')
                    st.toast('스프레드시트 열기 실패', icon='❌')
            
            if reset_filter:
                st.session_state.filter_multiselect = []
                st.session_state.category_filter_select = None
                st.session_state.category_filter_threshold = ""
                st.rerun()

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'), tlsAllowInvalidCertificates=True)

# 데이터베이스 및 컬렉션 선택
db = client['insta09_database']
collections = [
    
    '01_main_newfeed_crawl_data',
    '02_main_influencer_data', 
    '03_main_following_extract_data',
    '04_main_item_today_data'
    
]

# 페이지 제목
st.title('🚀인스타그램 데이터 분석 대시보드')

# 컬렉션 선택 콤보박스
selected_collection = st.selectbox('컬렉션 선택', 
                                 options=['empty_data'] + collections,  # 'empty_data'를 첫 번째 옵션으로 추가
                                 key="collection_select")

# 좌우 여백을 위한 컬럼 추가하여 중앙 정렬 (빈 칼럼 추가)
left_space, col1, col2, col3, empty1, col4, col5, col6, empty2, col7, col8, right_space = st.columns(
    [
        0.01,  # 왼쪽 여백
        3,    # 첫 번째 컬럼 (검색 타입)
        1.5,    # 두 번째 컬럼 (검색 입력)
        0.8,    # 검색 버튼
        0.5, # 빈 공간
        1.5,  # 다섯 번째 컬럼 (카테고리 선택)
        0.7,    # 여섯 번째 컬럼 (퍼센트 입력)
        1,  # 카테고리 검색 버튼
        0.5,   # 빈 공간
        1,  # 아홉 번째 컬럼 (엑셀 저장 버튼)
        1,    # 열 번째 컬럼 (초기화 버튼)
        0.1   # 오른쪽 여백
    ])

with left_space:
    st.empty()

with col1:
    search_type = st.radio(
        "",
        ["브랜드", "아이템", "인플루언서"],
        horizontal=True
    )

with col2:
    search_input = st.text_input("", placeholder="검색어 입력")

with col3:
    search_button = st.button("검색")

with empty1:
    st.empty()

with col4:
    category = st.selectbox('', 
                             ['뷰티', '패션', '홈/리빙', '푸드', '육아', 
                              '건강', '맛집탐방', '전시/공연', '반려동물', '기타'], 
                             placeholder="카테고리 선택",
                             key="category_select")

with col5:
    category_input = st.text_input("", placeholder="%")

with col6:
    category_search = st.button("카테고리 검색")

with empty2:
    st.empty()

with col7:
    excel_button = st.button("엑셀저장")

with col8:
    reset_button = st.button("초기화")

with right_space:
    st.empty()

# 데이터 표시 영역
st.divider()

# 빈 데이터프레임 생성
empty_df = pd.DataFrame()
display_df = empty_df  # 초기값 설정

# 카테고리 검색 처리
if category_search:
    try:
        # 입력값 검증
        if not category:
            st.warning("카테고리를 선택해주세요.")
        elif not category_input:
            st.warning("퍼센트를 입력해주세요.")
        else:
            # 퍼센트 값을 숫자로 변환
            try:
                percent_threshold = float(category_input)
            except ValueError:
                st.error("올바른 숫자를 입력해주세요.")
                percent_threshold = 0

            # 02_test_influencer_data 컬렉션에서 데이터 가져오기
            collection = db['02_main_influencer_data']
            data = list(collection.find({}, {'_id': 0}))
            
            if data:
                df = pd.DataFrame(data)
                filtered_data = []
                
                for _, row in df.iterrows():
                    if pd.notna(row.get('category')) and row['category']:
                        categories = row['category'].split(',')
                        for cat in categories:
                            if category in cat:
                                try:
                                    percent_str = cat.split('(')[1].rstrip('%)')
                                    percent = float(percent_str)
                                    if percent >= percent_threshold:
                                        filtered_data.append(row)
                                        break
                                except Exception as e:
                                    continue
                
                filtered_df = pd.DataFrame(filtered_data)
                
                if not filtered_data:
                    st.warning(f"{category} 카테고리에서 {percent_threshold}% 이상인 데이터가 없습니다.")
                    display_df = empty_df
                else:
                    display_df = filtered_df
                    st.write(f"총 {len(filtered_data)}개의 데이터가 있습니다.")
            else:
                st.warning("데이터가 없습니다.")
                display_df = empty_df
                
    except Exception as e:
        st.error(f"카테고리 검색 중 오류 발생: {e}")
        display_df = empty_df

# 검색 버튼 클릭 처리
elif search_button:
    try:
        if search_type == "브랜드":
            # 02_test_influencer_data 컬렉션에서 검색
            collection = db['02_main_influencer_data']
            
            # brand.name에 검색어가 포함된 경우 검색 (대소문자 구분 없이)
            query = {
                "brand": {
                    "$elemMatch": {
                        "name": {"$regex": search_input, "$options": "i"}
                    }
                }
            }
            
            # find()를 사용하여 모든 일치하는 문서 검색
            brand_data_list = list(collection.find(query))
            
            if brand_data_list:
                items_data = []
                
                # 각 인플루언서의 데이터를 처리
                for influencer_data in brand_data_list:
                    for brand in influencer_data.get('brand', []):
                        # 검색한 브랜드명과 일치하는 경우만 처리
                        if search_input.lower() in brand.get('name', '').lower():
                            brand_name = brand.get('name', '')
                            
                            for product in brand.get('products', []):
                                item_data = {
                                    '인플루언서': influencer_data.get('clean_name', ''),
                                    '아이디': influencer_data.get('username', ''),
                                    '팔로워': influencer_data.get('followers', ''),
                                    '카테고리': influencer_data.get('category', ''),
                                    '등급': influencer_data.get('grade', ''),
                                    '브랜드명': brand_name,
                                    '아이템명': product.get('item', ''),
                                    '타입': product.get('type', ''),
                                    '언급일자': product.get('mentioned_date', ''),
                                    '예상일자': product.get('expected_date', ''),
                                    '종료일자': product.get('end_date', ''),
                                    '피드링크': product.get('item_feed_link', ''),
                                    '릴스조회수': influencer_data.get('reels_views(15)', '0')
                                }
                                items_data.append(item_data)
                
                if items_data:
                    display_df = pd.DataFrame(items_data)
                    columns_order = [
                        '인플루언서', '아이디', '팔로워', '릴스조회수', '카테고리', '등급',
                        '브랜드명', '아이템명', '타입', '언급일자', '예상일자', 
                        '종료일자', '피드링크'
                    ]
                    display_df = display_df[columns_order]
                    st.write(f"브랜드 '{search_input}'에 대해 총 {len(items_data)}개의 아이템이 검색되었습니다.")
                else:
                    display_df = empty_df
                    st.warning("해당 브랜드의 아이템 정보가 없습니다.")
            else:
                display_df = empty_df
                st.warning("검색어와 일치하는 브랜드를 찾을 수 없습니다.")
        
        elif search_type == "인플루언서":
            # 02_test_influencer_data 컬렉션에서 검색
            collection = db['02_main_influencer_data']
            
            # username 또는 clean_name에 검색어가 포함된 경우 검색 (대소문자 구분 없이)
            query = {
                "$or": [
                    {"username": {"$regex": search_input, "$options": "i"}},
                    {"clean_name": {"$regex": search_input, "$options": "i"}}
                ]
            }
            
            # find()를 사용하여 모든 일치하는 문서 검색
            influencer_data_list = list(collection.find(query))
            
            if influencer_data_list:  # influencer_data를 influencer_data_list로 변경
                # 브랜드별 아이템 리스트 추출
                items_data = []
                
                # 각 인플루언서의 데이터를 처리
                for influencer_data in influencer_data_list:
                    for brand in influencer_data.get('brand', []):
                        brand_name = brand.get('name', '')
                        
                        for product in brand.get('products', []):
                            item_data = {
                                '인플루언서': influencer_data.get('clean_name', ''),
                                '아이디': influencer_data.get('username', ''),
                                '팔로워': influencer_data.get('followers', ''),
                                '카테고리': influencer_data.get('category', ''),
                                '등급': influencer_data.get('grade', ''),
                                '브랜드명': brand_name,
                                '아이템명': product.get('item', ''),
                                '타입': product.get('type', ''),
                                '언급일자': product.get('mentioned_date', ''),
                                '예상일자': product.get('expected_date', ''),
                                '종료일자': product.get('end_date', ''),
                                '피드링크': product.get('item_feed_link', ''),
                                '릴스조회수': influencer_data.get('reels_views(15)', '0')
                            }
                            items_data.append(item_data)
                
                if items_data:
                    display_df = pd.DataFrame(items_data)
                    columns_order = [
                        '브랜드명', '아이템명', '등급', '인플루언서', '아이디', 
                        '팔로워', '릴스조회수', '카테고리', 
                        '언급일자', '타입', '예상일자', '종료일자', '피드링크'
                    ]
                    display_df = display_df[columns_order]
                    st.write(f"검색어 '{search_input}'에 대해 총 {len(items_data)}개의 아이템이 검색되었습니다.")
                else:
                    display_df = empty_df
                    st.warning("검색된 인플루언서의 아이템 정보가 없습니다.")
            else:
                display_df = empty_df
                st.warning("검색어와 일치하는 인플루언서를 찾을 수 없습니다.")
        
        elif search_type == "아이템":
            # 02_test_influencer_data 컬렉션에서 검색
            collection = db['02_main_influencer_data']
            
            # products.item에 검색어가 포함된 경우 검색 (대소문자 구분 없이)
            query = {
                "brand": {
                    "$elemMatch": {
                        "products": {
                            "$elemMatch": {
                                "item": {"$regex": search_input, "$options": "i"}
                            }
                        }
                    }
                }
            }
            
            # find()를 사용하여 모든 일치하는 문서 검색
            item_data_list = list(collection.find(query))
            
            if item_data_list:
                items_data = []
                
                # 각 인플루언서의 데이터를 처리
                for influencer_data in item_data_list:
                    for brand in influencer_data.get('brand', []):
                        brand_name = brand.get('name', '')
                        
                        for product in brand.get('products', []):
                            # 검색한 아이템명과 일치하는 경우만 처리
                            if search_input.lower() in product.get('item', '').lower():
                                item_data = {
                                    '아이템명': product.get('item', ''),
                                    '브랜드명': brand_name,
                                    '타입': product.get('type', ''),
                                    '인플루언서': influencer_data.get('clean_name', ''),
                                    '아이디': influencer_data.get('username', ''),
                                    '팔로워': influencer_data.get('followers', ''),
                                    '릴스조회수': influencer_data.get('reels_views(15)', '0'),
                                    '카테고리': influencer_data.get('category', ''),
                                    '등급': influencer_data.get('grade', ''),
                                    '언급일자': product.get('mentioned_date', ''),
                                    '예상일자': product.get('expected_date', ''),
                                    '종료일자': product.get('end_date', ''),
                                    '피드링크': product.get('item_feed_link', '')
                                }
                                items_data.append(item_data)
                
                if items_data:
                    display_df = pd.DataFrame(items_data)
                    columns_order = [
                        '브랜드명', '아이템명', '등급', '인플루언서', '아이디', 
                        '팔로워', '릴스조회수', '카테고리', 
                        '언급일자', '타입', '예상일자', '종료일자', '피드링크'
                    ]
                    display_df = display_df[columns_order]
                    st.write(f"아이템 '{search_input}'에 대해 총 {len(items_data)}개의 결과가 검색되었습니다.")
                else:
                    display_df = empty_df
                    st.warning("해당 아이템 정보가 없습니다.")
            else:
                display_df = empty_df
                st.warning("검색어와 일치하는 아이템을 찾을 수 없습니다.")
        
        else:
            # 기존의 브랜드, 아이템 검색 로직
            if selected_collection and selected_collection != 'empty_data':
                collection = db[selected_collection]
                data = list(collection.find({}, {'_id': 0}))
                
                if data:
                    df = pd.DataFrame(data)
                    for col in df.columns:
                        if 'views' in col or 'likes' in col or 'comments' in col:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    display_df = df
                    st.write(f"총 {len(data)}개의 데이터가 있습니다.")
                else:
                    display_df = empty_df
                    st.warning("데이터가 없습니다.")
            else:
                display_df = empty_df
    except Exception as e:
        st.error(f"검색 중 오류가 발생했습니다: {e}")
        display_df = empty_df

# 컬렉션 선택 처리
elif selected_collection and selected_collection != 'empty_data':
    try:
        collection = db[selected_collection]
        data = list(collection.find({}, {'_id': 0}))
        
        if data:
            df = pd.DataFrame(data)
            
            # 04_test_item_today_data 컬렉션이 선택되고 필터가 적용된 경우
            if selected_collection == '04_main_item_today_data':
                # 아이템 카테고리 필터 적용
                if 'filter_multiselect' in st.session_state and st.session_state.filter_multiselect:
                    st.info(f"선택된 아이템 필터: {', '.join(st.session_state.filter_multiselect)}")
                    # item_category 컬럼을 기준으로 필터링
                    df = df[~df['item_category'].isin(st.session_state.filter_multiselect)]
                
                # 카테고리 필터 적용
                if ('category_filter_select' in st.session_state and st.session_state.category_filter_select and
                    'category_filter_threshold' in st.session_state and st.session_state.category_filter_threshold):
                    try:
                        category = st.session_state.category_filter_select
                        threshold = float(st.session_state.category_filter_threshold)
                        st.info(f"카테고리 필터: {category} {threshold}% 이상")
                        
                        # 카테고리 필터링 로직
                        filtered_data = []
                        for _, row in df.iterrows():
                            if pd.notna(row.get('category')) and row['category']:
                                categories = row['category'].split(',')
                                for cat in categories:
                                    if category in cat:
                                        try:
                                            percent_str = cat.split('(')[1].rstrip('%)')
                                            percent = float(percent_str)
                                            if percent >= threshold:
                                                filtered_data.append(row)
                                                break
                                        except Exception:
                                            continue
                        
                        if filtered_data:
                            df = pd.DataFrame(filtered_data)
                        else:
                            st.warning(f"{category} 카테고리에서 {threshold}% 이상인 데이터가 없습니다.")
                    except ValueError:
                        st.error("카테고리 필터에 올바른 숫자를 입력해주세요.")
            
            for col in df.columns:
                if 'views' in col or 'likes' in col or 'comments' in col:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            display_df = df
            st.write(f"총 {len(df)}개의 데이터가 있습니다.")
        else:
            display_df = empty_df
            st.warning("데이터가 없습니다.")
    except Exception as e:
        st.error(f"데이터 로딩 중 오류 발생: {e}")
        display_df = empty_df

# 최종 데이터프레임 표시 (한 번만 호출)
st.dataframe(
    display_df,
    use_container_width=True,
    height=600,
    key="main_data_frame",
    column_config={
        "피드링크": st.column_config.LinkColumn("피드링크"),
        "post_url": st.column_config.LinkColumn("post_url"),
        "profile_link": st.column_config.LinkColumn("profile_link"),
        "out_link": st.column_config.LinkColumn("out_link"),
        "item_feed_link": st.column_config.LinkColumn("item_feed_link")
    }
)

# MongoDB 연결 확인
try:
    client.admin.command('ping')
    st.success("MongoDB 연결 성공!")
except Exception as e:
    st.error(f"MongoDB 연결 실패: {e}")
