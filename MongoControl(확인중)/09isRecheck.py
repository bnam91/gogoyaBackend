from pymongo import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
from tkinter import filedialog
import tkinter as tk
from datetime import datetime

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'),
                    connectTimeoutMS=60000,
                    socketTimeoutMS=60000)

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")
    
    # 데이터베이스와 컬렉션 선택
    db = client['insta09_database']
    collection = db['02_main_influencer_data']
    
    # 전체 문서 수 계산
    total_docs = collection.count_documents({})
    
    # '09_is' 필드가 'N'인 문서 수
    n_count = collection.count_documents({"09_is": "N"})
    
    # '09_is' 필드가 'Y'인 문서 수
    y_count = collection.count_documents({"09_is": "Y"})
    
    # '09_is' 필드가 없는 문서 수
    no_field_count = collection.count_documents({"09_is": {"$exists": False}})
    
    # '09_is' 필드의 모든 고유값 확인
    distinct_values = collection.distinct("09_is")
    
    print("\n=== 데이터베이스 통계 정보 ===")
    print(f"전체 문서 수: {total_docs:,}개")
    print(f"'09_is' 필드가 'N'인 문서: {n_count:,}개")
    print(f"'09_is' 필드가 'Y'인 문서: {y_count:,}개")
    print(f"'09_is' 필드가 없는 문서: {no_field_count:,}개")
    
    print("\n=== '09_is' 필드의 모든 고유값 ===")
    for value in distinct_values:
        count = collection.count_documents({"09_is": value})
        print(f"값 '{value}': {count:,}개")
    
    print("\n=== 합계 확인 ===")
    total_counted = n_count + y_count + no_field_count
    print(f"전체 문서 수: {total_docs:,}개")
    print(f"확인된 문서 합계: {total_counted:,}개")
    print(f"차이: {total_docs - total_counted:,}개")
    print("==========================\n")
    
    # 09_is 필드가 "N"인 문서 검색
    query = {"09_is": "N"}
    projection = {"username": 1, "profile_link": 1, "_id": 0}
    
    # 데이터 가져오기
    documents = list(collection.find(query, projection))
    
    if documents:
        # DataFrame 생성
        df = pd.DataFrame(documents)
        
        # 현재 날짜와 시간 가져오기
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_filename = f"공구재확인_{current_datetime}.xlsx"
        
        # GUI로 저장 경로 선택
        root = tk.Tk()
        root.withdraw()  # GUI 창 숨기기
        
        file_path = filedialog.asksaveasfilename(
            initialfile=default_filename,
            defaultextension='.xlsx',
            filetypes=[("Excel files", "*.xlsx")],
            title="엑셀 파일 저장 위치 선택"
        )
        
        if file_path:
            # 엑셀 파일로 저장
            df.to_excel(file_path, index=False)
            print(f"파일이 성공적으로 저장되었습니다: {file_path}")
        else:
            print("저장이 취소되었습니다.")
    else:
        print("검색 결과가 없습니다.")

except Exception as e:
    print(f"오류 발생: {e}")

finally:
    # MongoDB 연결 종료
    client.close()
    print("MongoDB 연결이 종료되었습니다.")
