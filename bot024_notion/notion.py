import requests
import os
from dotenv import load_dotenv
import json
from datetime import datetime
import pandas as pd
import time
import sys
from pathlib import Path

# 현재 디렉토리를 파이썬 경로에 추가
current_dir = Path(__file__).parent
if str(current_dir) not in sys.path:
    sys.path.append(str(current_dir))

from config import headers, PAGE_ID, DATABASE_ID
from create_subpage import create_subpage_with_callout

# .env 파일에서 환경 변수 로드
load_dotenv()

# Notion API 설정
NOTION_API_KEY = os.getenv('NOTION_API_KEY')
if not NOTION_API_KEY:
    raise ValueError("NOTION_API_KEY가 설정되지 않았습니다. .env 파일을 확인해주세요.")

def get_block_children(block_id):
    url = f"https://api.notion.com/v1/blocks/{block_id}/children"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()["results"]
    return []

def find_callout_block(blocks, target_id="1f6111a5-7788-80b8-8ab2-faf5d9fab77c"):
    for block in blocks:
        if block.get("type") == "callout" and block.get("id") == target_id:
            # 콜아웃 블록의 하위 블록들 가져오기
            children = get_block_children(target_id)
            if children:
                # 하위 블록들의 내용과 서식을 포함하여 반환
                return {
                    "children": children,
                    "icon": block["callout"].get("icon", {"type": "emoji", "emoji": "⚠️"}),
                    "color": block["callout"].get("color", "gray_background")
                }
            return {
                "children": [],
                "icon": block["callout"].get("icon", {"type": "emoji", "emoji": "⚠️"}),
                "color": block["callout"].get("color", "gray_background")
            }
    return None

def create_page(parent_id, title, callout_data):
    url = "https://api.notion.com/v1/pages"
    
    payload = {
        "parent": {
            "page_id": parent_id
        },
        "properties": {
            "title": {
                "title": [
                    {
                        "text": {
                            "content": title
                        }
                    }
                ]
            }
        }
    }
    
    max_retries = 3
    retry_delay = 1  # 초 단위
    
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=payload)
            if response.status_code == 200:
                page_id = response.json()["id"]
                
                # 페이지 생성 후 잠시 대기
                time.sleep(retry_delay)
                
                # 먼저 콜아웃 블록만 추가
                callout_url = f"https://api.notion.com/v1/blocks/{page_id}/children"
                callout_block = {
                    "object": "block",
                    "type": "callout",
                    "callout": {
                        "rich_text": [],
                        "icon": callout_data["icon"],
                        "color": callout_data["color"]
                    }
                }
                
                callout_payload = {
                    "children": [callout_block]
                }
                
                # 콜아웃 블록 추가
                callout_response = requests.patch(callout_url, headers=headers, json=callout_payload)
                if callout_response.status_code != 200:
                    print(f"콜아웃 블록 추가 중 오류 발생: {callout_response.status_code}")
                    print(callout_response.text)
                    return page_id
                
                # 콜아웃 블록의 ID 가져오기
                callout_id = callout_response.json()["results"][0]["id"]
                
                # 잠시 대기
                time.sleep(retry_delay)
                
                # 콜아웃 블록의 하위 블록으로 내용 추가
                children_url = f"https://api.notion.com/v1/blocks/{callout_id}/children"
                
                # 하위 블록들 생성
                children_blocks = []
                for child in callout_data["children"]:
                    # 필요한 속성만 복사
                    new_block = {
                        "object": "block",
                        "type": child["type"]
                    }
                    
                    # 블록 타입에 따라 적절한 속성 복사
                    if child["type"] == "paragraph":
                        new_block["paragraph"] = {
                            "rich_text": child["paragraph"]["rich_text"]
                        }
                    elif child["type"] == "heading_1":
                        new_block["heading_1"] = {
                            "rich_text": child["heading_1"]["rich_text"]
                        }
                    elif child["type"] == "heading_2":
                        new_block["heading_2"] = {
                            "rich_text": child["heading_2"]["rich_text"]
                        }
                    elif child["type"] == "heading_3":
                        new_block["heading_3"] = {
                            "rich_text": child["heading_3"]["rich_text"]
                        }
                    elif child["type"] == "bulleted_list_item":
                        new_block["bulleted_list_item"] = {
                            "rich_text": child["bulleted_list_item"]["rich_text"]
                        }
                    elif child["type"] == "numbered_list_item":
                        new_block["numbered_list_item"] = {
                            "rich_text": child["numbered_list_item"]["rich_text"]
                        }
                    elif child["type"] == "to_do":
                        new_block["to_do"] = {
                            "rich_text": child["to_do"]["rich_text"],
                            "checked": child["to_do"]["checked"]
                        }
                    elif child["type"] == "toggle":
                        new_block["toggle"] = {
                            "rich_text": child["toggle"]["rich_text"]
                        }
                    elif child["type"] == "code":
                        new_block["code"] = {
                            "rich_text": child["code"]["rich_text"],
                            "language": child["code"]["language"]
                        }
                    
                    children_blocks.append(new_block)
                
                # 하위 블록 추가
                children_payload = {
                    "children": children_blocks
                }
                
                # 하위 블록 추가 시도
                for children_attempt in range(max_retries):
                    children_response = requests.patch(children_url, headers=headers, json=children_payload)
                    if children_response.status_code == 200:
                        return page_id
                    elif children_response.status_code == 409:
                        if children_attempt < max_retries - 1:
                            time.sleep(retry_delay)
                            continue
                    print(f"하위 블록 추가 중 오류 발생: {children_response.status_code}")
                    print(children_response.text)
                    break
                
                return page_id
            elif response.status_code == 409 and attempt < max_retries - 1:
                print(f"페이지 생성 시도 {attempt + 1}/{max_retries} 실패. 재시도 중...")
                time.sleep(retry_delay)
                continue
            else:
                print(f"페이지 생성 중 오류 발생: {response.status_code}")
                print(response.text)
                return None
        except Exception as e:
            print(f"예상치 못한 오류 발생: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return None
    
    return None

def get_page_blocks(page_id):
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        blocks = response.json()["results"]
        
        # 엑셀에 저장할 데이터를 담을 리스트
        blocks_data = []
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        for block in blocks:
            block_type = block.get("type", "unknown")
            block_id = block.get("id", "unknown")
            created_time = block.get("created_time", "")
            last_edited_time = block.get("last_edited_time", "")
            
            # 텍스트 내용이 있는 경우 가져오기
            content = ""
            if block_type == "paragraph" and "paragraph" in block:
                rich_text = block["paragraph"].get("rich_text", [])
                if rich_text:
                    content = rich_text[0].get("text", {}).get("content", "")
            
            # 엑셀 데이터 추가
            blocks_data.append({
                "조회 시간": current_time,
                "블록 ID": block_id,
                "타입": block_type,
                "내용": content,
                "생성 시간": created_time,
                "마지막 수정 시간": last_edited_time
            })
        
        # 현재 스크립트의 디렉토리 경로 가져오기
        current_dir = os.path.dirname(os.path.abspath(__file__))
        excel_filename = "notion_blocks_history.xlsx"
        excel_path = os.path.join(current_dir, excel_filename)
        
        print(f"\n엑셀 파일 저장을 시작합니다...")
        print(f"저장 경로: {excel_path}")
        
        try:
            # 기존 파일이 있는지 확인
            if os.path.exists(excel_path):
                print(f"기존 파일이 존재합니다. 크기: {os.path.getsize(excel_path)} bytes")
                
            # 새로운 데이터로 DataFrame 생성
            df = pd.DataFrame(blocks_data)
            print(f"저장할 데이터 수: {len(df)} 행")
            
            # 파일 저장
            df.to_excel(excel_path, index=False, engine='openpyxl')
            
            # 저장 후 파일 존재 확인
            if os.path.exists(excel_path):
                print(f"파일이 성공적으로 저장되었습니다. 새로운 크기: {os.path.getsize(excel_path)} bytes")
            else:
                print("경고: 파일 저장 후에도 파일이 존재하지 않습니다.")
            
            print(f"\n블록 목록이 {excel_filename} 파일에 저장되었습니다.")
            print(f"총 {len(df)} 개의 블록이 저장되어 있습니다.")
            
        except Exception as e:
            print(f"파일 저장 중 오류 발생: {str(e)}")
            raise e
        
        return blocks
    else:
        print(f"블록 조회 중 오류 발생: {response.status_code}")
        print(response.text)
        return []

def add_text_block_and_subpage():
    # 먼저 텍스트 블록 추가
    print("\n새로운 블록 추가 중...")
    url = f"https://api.notion.com/v1/blocks/{PAGE_ID}/children"
    
    payload = {
        "children": [
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [
                        {
                            "type": "text",
                            "text": {
                                "content": "테스트입니다"
                            }
                        }
                    ]
                }
            }
        ]
    }
    
    response = requests.patch(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        print("텍스트 블록이 성공적으로 추가되었습니다.")
        
        # 하위 페이지 생성
        create_subpage_with_callout()
    else:
        print(f"오류 발생: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    add_text_block_and_subpage()
    try:
        # 블록 정보 업데이트
        blocks = get_page_blocks(PAGE_ID)
        if not blocks:
            print("블록 정보를 가져오는데 실패했습니다.")
    except Exception as e:
        print(f"엑셀 파일 업데이트 중 오류 발생: {str(e)}")
