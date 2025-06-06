from pymongo import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
import re
from datetime import datetime
import tkinter as tk
from tkinter import filedialog

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'),
                    connectTimeoutMS=30000,  # 연결 타임아웃 30초
                    socketTimeoutMS=30000,   # 소켓 타임아웃 30초
                    maxPoolSize=50)          # 최대 연결 풀 크기 설정

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")
    print("데이터베이스 연결 확인 중...")
    
    # 데이터베이스와 컬렉션 선택
    db = client['insta09_database']
    collection = db['02_main_influencer_data']
    
    # 컬렉션 존재 여부 확인
    if collection.count_documents({}) == 0:
        print("컬렉션이 비어있거나 존재하지 않습니다.")
        exit()
    
    print(f"총 문서 수: {collection.count_documents({})}")
    print("데이터 추출 중...")
    
    # 검색 조건 출력
    min_percentage = 30  # 최소 비율 설정
    min_reels_views = 4000  # 최소 릴스 조회수 설정
    print("\n[검색 조건]")
    print("- 카테고리: 홈/리빙(하드코딩)")
    print(f"- 최소 비율: {min_percentage}% 이상")
    print(f"- 최소 릴스 조회수: {min_reels_views:,}회 이상")
    print("\n[검색 결과]")
    
    # 데이터를 저장할 리스트
    influencer_data = []
    
    # 진행 상황 표시를 위한 변수
    processed_count = 0
    batch_size = 500  # 500개씩 처리
    total_docs = collection.count_documents({"is_contact_excluded": {"$ne": True}})
    
    print(f"총 {total_docs}개의 문서를 처리합니다...")
    print("데이터 처리 시작...")
    
    # 모든 문서를 가져옵니다
    documents = collection.find({
        "is_contact_excluded": {"$ne": True}  # is_contact_excluded가 true인 문서 제외
    }).batch_size(batch_size)  # 배치 사이즈 설정
    
    count = 0
    total_checked = 0
    
    for doc in documents:
        total_checked += 1
        processed_count += 1
        
        # 진행 상황 표시 (500개마다)
        if processed_count % 500 == 0:
            print(f"처리 중... {processed_count}/{total_docs} 문서")
        
        if 'category' in doc and '홈/리빙' in doc['category']:
            # 카테고리 문자열에서 홈/리빙 비율 추출
            category_str = doc['category']
            match = re.search(r'홈/리빙\((\d+)%\)', category_str)
            
            if match:
                percentage = int(match.group(1))
                # 릴스 조회수 확인
                reels_views = doc.get('reels_views(15)', 'N/A')
                if reels_views != 'N/A':
                    try:
                        reels_views = int(reels_views)
                        if percentage >= min_percentage and reels_views >= min_reels_views:
                            influencer_data.append({
                                'Username': doc['username'],
                                'Category': doc['category'],
                                'Profile_Link': doc.get('profile_link', 'N/A'),
                                'Reels_Views(15)': reels_views,
                                'Date': ''
                            })
                            count += 1
                            if count % 10 == 0:  # 매 10개마다 진행상황 출력
                                print(f"조건에 맞는 인플루언서 {count}명 발견")
                    except (ValueError, TypeError):
                        continue

    print(f"\n데이터 처리 완료!")
    print(f"- 검토한 전체 인플루언서 수: {total_checked}명")
    print(f"- 조건에 맞는 인플루언서 수: {count}명")
    
    # 데이터프레임 생성
    df = pd.DataFrame(influencer_data)
    
    # Reels_Views(15) 정렬을 위한 임시 컬럼 생성
    df['sort_value'] = pd.to_numeric(df['Reels_Views(15)'].replace('N/A', '0'), errors='coerce')
    
    # 임시 컬럼 기준으로 내림차순 정렬
    df = df.sort_values(by='sort_value', ascending=False)
    
    # 임시 정렬 컬럼 제거
    df = df.drop('sort_value', axis=1)
    
    if count > 0:
        # tkinter 루트 윈도우 생성
        root = tk.Tk()
        root.withdraw()  # 메인 윈도우 숨기기

        # 현재 시간을 파일명에 포함
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_filename = f'홈리빙_인플루언서_리스트_{current_time}.xlsx'

        # 파일 저장 다이얼로그 표시
        excel_filename = filedialog.asksaveasfilename(
            defaultextension='.xlsx',
            filetypes=[('Excel 파일', '*.xlsx')],
            initialfile=default_filename,
            title='엑셀 파일 저장'
        )

        if excel_filename:  # 사용자가 경로를 선택한 경우
            # 엑셀 파일 저장
            with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)

            print(f"\n[작업 완료]")
            print(f"- 검토한 전체 인플루언서 수: {total_checked}명")
            print(f"- 조건에 맞는 인플루언서 수: {count}명")
            print(f"- 저장된 파일: {excel_filename}")
        else:
            print("\n파일 저장이 취소되었습니다.")
    else:
        print("\n조건에 맞는 데이터가 없습니다.")

except Exception as e:
    print(f"에러 발생: {e}")

finally:
    client.close()
    print("\nMongoDB 연결 종료")
