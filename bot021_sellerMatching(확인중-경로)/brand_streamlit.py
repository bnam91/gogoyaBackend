'''
streamlit run 머신러닝/분포도.py
'''

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from pymongo import MongoClient
from pymongo.server_api import ServerApi

# MongoDB 연결
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    # 데이터베이스와 컬렉션 선택
    db = client['insta09_database']
    collection = db['02_main_influencer_data']
    
    # 페이지 설정
    st.set_page_config(page_title="인플루언서 릴스 분석", layout="wide")
    st.title("인플루언서 릴스 분석 대시보드")

    # 브랜드명 입력
    brand_name = st.sidebar.text_input("브랜드명 입력", "BAS")
    
    # BAS 브랜드 데이터 조회
    data = list(collection.find(
        {"brand": {"$elemMatch": {"name": brand_name}}},
        {"username": 1, "grade": 1, "reels_views(15)": 1, "category": 1, "_id": 0}
    ))
    
    if not data:
        st.warning(f"'{brand_name}' 브랜드의 데이터가 없습니다.")
        st.stop()
    
    # DataFrame 생성
    df = pd.DataFrame(data)
    
    # 필드명 변경 및 데이터 타입 변환
    df = df.rename(columns={'reels_views(15)': 'reels_views'})
    df['reels_views'] = pd.to_numeric(df['reels_views'], errors='coerce').fillna(0).astype(int)

    # 사이드바 필터
    st.sidebar.header("데이터 필터")
    selected_grade = st.sidebar.multiselect(
        "인플루언서 등급",
        options=sorted(df['grade'].unique()),
        default=sorted(df['grade'].unique())
    )

    # 조회수 범위 필터
    min_views = int(df['reels_views'].min())
    max_views = int(df['reels_views'].max())
    views_range = st.sidebar.slider(
        "조회수 범위",
        min_value=min_views,
        max_value=max_views,
        value=(min_views, max_views)
    )

    # 데이터 필터링
    filtered_df = df[
        (df['grade'].isin(selected_grade)) &
        (df['reels_views'] >= views_range[0]) &
        (df['reels_views'] <= views_range[1])
    ]

    # 상위 10%와 하위 10% 제외
    views_quantile_10 = filtered_df['reels_views'].quantile(0.1)
    views_quantile_90 = filtered_df['reels_views'].quantile(0.9)
    filtered_df = filtered_df[
        (filtered_df['reels_views'] >= views_quantile_10) &
        (filtered_df['reels_views'] <= views_quantile_90)
    ]

    # 메인 컨텐츠
    st.subheader(f"{brand_name} 인플루언서별 릴스 조회수")
    # Plotly 막대 그래프
    fig_bar = px.bar(
        filtered_df.sort_values('reels_views', ascending=True),
        x='username',
        y='reels_views',
        title=f'{brand_name} 인플루언서별 릴스 조회수',
        labels={'username': '인플루언서', 'reels_views': '조회수'},
        color='grade',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    fig_bar.update_layout(
        showlegend=True,
        hovermode='x',
        height=500,
        xaxis_tickangle=-45,
        xaxis={'categoryorder': 'total ascending'}
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # 통계 정보
    st.subheader("주요 통계")
    col_stats1, col_stats2, col_stats3, col_stats4 = st.columns(4)

    with col_stats1:
        st.metric("선택된 인플루언서 수", f"{len(filtered_df):,}명")
    with col_stats2:
        st.metric("평균 조회수", f"{int(filtered_df['reels_views'].mean()):,}")
    with col_stats3:
        st.metric("최대 조회수", f"{int(filtered_df['reels_views'].max()):,}")
    with col_stats4:
        st.metric("최소 조회수", f"{int(filtered_df['reels_views'].min()):,}")

    # 상세 데이터 테이블
    st.subheader("인플루언서 상세 정보")

    # 검색 필터
    search_term = st.text_input("사용자명 검색", "")
    if search_term:
        filtered_df = filtered_df[filtered_df['username'].str.contains(search_term, case=False)]

    # 데이터 테이블
    st.dataframe(
        filtered_df[['username', 'grade', 'reels_views', 'category']]
        .sort_values('reels_views', ascending=False)
        .reset_index(drop=True),
        use_container_width=True
    )

    # 카테고리 분석
    st.subheader("카테고리별 분석")
    # 카테고리 데이터 전처리 (쉼표로 구분된 카테고리 분리)
    categories = filtered_df['category'].str.split(',').explode()
    categories = categories.str.extract(r'([^(]+)')[0].str.strip()
    category_counts = categories.value_counts().head(10)

    # 상위 10개 카테고리 차트
    fig_pie = px.pie(
        values=category_counts.values,
        names=category_counts.index,
        title='상위 10개 카테고리 분포',
        hole=0.4
    )
    fig_pie.update_layout(height=500)
    st.plotly_chart(fig_pie, use_container_width=True)

except Exception as e:
    st.error(f"MongoDB 연결 또는 데이터 조회 중 오류 발생: {str(e)}")
finally:
    client.close()
