import sys
import os
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from googleapiclient.discovery import build
from auth import get_credentials
import pandas as pd

def read_sourcing_list():
    # 스프레드시트 ID 추출 (URL에서)
    SPREADSHEET_ID = '1WTR3aDHxrX9r9DTNfVsu68aoL75aR8RX-4K8lqi5aek'
    RANGE_NAME = '소싱리스트!A:Z'  # 전체 데이터를 가져온 후 필요한 열만 선택
    JSON_FILE_PATH = os.path.join(os.path.dirname(__file__), 'sourcing_list.json')

    try:
        # Google Sheets API 서비스 생성
        creds = get_credentials()
        service = build('sheets', 'v4', credentials=creds)

        # 데이터 가져오기
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            print(json.dumps({"error": "데이터가 없습니다."}, ensure_ascii=False))
            return None

        # 헤더와 데이터 분리
        headers = values[0]
        data = values[1:]

        # 데이터프레임 생성
        try:
            # 빈 값으로 채워진 데이터프레임 생성
            df = pd.DataFrame(data, columns=headers[:len(data[0])] if data else headers)
            
            # 필요한 열 찾기 (대소문자 구분 없이)
            brand_col = next((col for col in df.columns if '브랜드' in col), None)
            item_col = next((col for col in df.columns if '아이템' in col), None)
            url_col = next((col for col in df.columns if 'URL' in col), None)
            
            if not all([brand_col, item_col, url_col]):
                error_info = {
                    "error": "필요한 열을 찾을 수 없습니다.",
                    "columns": {
                        "브랜드": brand_col,
                        "아이템": item_col,
                        "URL": url_col
                    }
                }
                print(json.dumps(error_info, ensure_ascii=False))
                return None
            
            # 찾은 열로 데이터프레임 생성
            result_df = df[[brand_col, item_col, url_col]]
            result_df.columns = ['브랜드', '아이템', 'URL']  # 열 이름 통일
            
            # 데이터프레임을 JSON 형식으로 변환
            json_data = result_df.to_dict(orient='records')
            result_json = {"data": json_data}
            
            # JSON 파일로 저장
            with open(JSON_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump(result_json, f, ensure_ascii=False, indent=2)
            
            print(json.dumps({"message": f"데이터가 {JSON_FILE_PATH}에 저장되었습니다."}, ensure_ascii=False))
            print(json.dumps(result_json, ensure_ascii=False))
            
            return result_df
            
        except Exception as e:
            print(json.dumps({"error": f"데이터프레임 생성 중 오류 발생: {str(e)}"}, ensure_ascii=False))
            return None

    except Exception as e:
        print(json.dumps({"error": f"에러 발생: {str(e)}"}, ensure_ascii=False))
        return None

if __name__ == '__main__':
    result = read_sourcing_list()
    if result is not None:
        print("데이터 처리 완료")
    else:
        print("데이터 처리 실패")
