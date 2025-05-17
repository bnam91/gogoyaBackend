import requests
import time
import json
from datetime import datetime
import sys
from pathlib import Path

# 현재 디렉토리를 파이썬 경로에 추가
current_dir = Path(__file__).parent
if str(current_dir) not in sys.path:
    sys.path.append(str(current_dir))

from config import headers, PAGE_ID
from utils.database_utils import duplicate_database, add_items_to_database, get_page_url
from utils.callout_utils import get_block_children, find_callout_block, create_page_with_callout

def create_subpage_with_callout(target_id="1f6111a5-7788-80b8-8ab2-faf5d9fab77c"):
    """
    콜아웃 블록을 찾아서 하위 페이지를 생성하고 데이터베이스를 추가하는 함수
    
    Args:
        target_id (str): 찾을 콜아웃 블록의 ID
        
    Returns:
        tuple: (하위 페이지 ID, 데이터베이스 ID) 또는 (None, None) (실패 시)
    """
    # 현재 페이지의 블록들을 가져옴
    print("블록 정보를 가져오는 중...")
    url = f"https://api.notion.com/v1/blocks/{PAGE_ID}/children"
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"블록 조회 중 오류 발생: {response.status_code}")
        print(response.text)
        return None, None
    
    blocks = response.json()["results"]
    
    # 특정 ID의 callout 블록 찾기
    print(f"\n찾으려는 블록 ID: {target_id}")
    callout_data = find_callout_block(blocks, target_id)
    if not callout_data:
        print(f"\nID가 {target_id}인 callout 블록을 찾을 수 없습니다.")
        print("\n현재 페이지의 블록 ID 목록:")
        for block in blocks:
            print(f"ID: {block.get('id')}, 타입: {block.get('type')}")
        return None, None
    
    # 현재 날짜를 YYYY-MM-DD 형식으로 가져오기
    current_date = datetime.now().strftime("%Y-%m-%d")
    subpage_title = f"하위페이지_{current_date}"
    
    # 하위 페이지 생성 (callout 내용과 서식 복제)
    subpage_id = create_page_with_callout(PAGE_ID, subpage_title, callout_data)
    if not subpage_id:
        return None, None
    
    print(f"하위 페이지가 성공적으로 생성되었습니다. (제목: {subpage_title}, ID: {subpage_id})")
    
    # 페이지 공유 링크 가져오기
    page_url = get_page_url(subpage_id)
    
    # 빈 블록 추가
    empty_block_url = f"https://api.notion.com/v1/blocks/{subpage_id}/children"
    empty_block_payload = {
        "children": [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": []
                }
            }
        ]
    }
    
    empty_block_response = requests.patch(empty_block_url, headers=headers, json=empty_block_payload)
    if empty_block_response.status_code != 200:
        print("빈 블록 추가 중 오류 발생")
    
    # 데이터베이스 복제
    database_id = duplicate_database(subpage_id)
    
    return subpage_id, database_id

if __name__ == "__main__":
    # 하위 페이지와 데이터베이스 생성
    page_id, db_id = create_subpage_with_callout()
    
    if page_id and db_id:
        try:
            # JSON 파일 로드
            with open('module/sourcing_list.json', 'r', encoding='utf-8') as f:
                sourcing_data = json.load(f)
            
            # 데이터베이스에 아이템 추가
            add_items_to_database(db_id, sourcing_data['data'])
            print("모든 작업이 성공적으로 완료되었습니다.")
        except Exception as e:
            print(f"데이터 추가 중 오류 발생: {str(e)}")
    else:
        print("페이지 또는 데이터베이스 생성에 실패했습니다.") 