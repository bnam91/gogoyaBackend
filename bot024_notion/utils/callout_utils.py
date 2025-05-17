import requests
import time
import sys
from pathlib import Path

# 상위 디렉토리를 파이썬 경로에 추가
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from config import headers

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
                return {
                    "children": children,
                    "icon": block["callout"].get("icon", {"type": "emoji", "emoji": "⚠️"}),
                    "color": block["callout"].get("color", "gray_background"),
                    "rich_text": block["callout"].get("rich_text", [])
                }
            return {
                "children": [],
                "icon": block["callout"].get("icon", {"type": "emoji", "emoji": "⚠️"}),
                "color": block["callout"].get("color", "gray_background"),
                "rich_text": block["callout"].get("rich_text", [])
            }
    return None

def create_page_with_callout(parent_id, title, callout_data):
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
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, json=payload)
            if response.status_code == 200:
                page_id = response.json()["id"]
                
                # 페이지 생성 후 잠시 대기
                time.sleep(retry_delay)
                
                # 먼저 콜아웃 블록만 추가
                callout_url = f"https://api.notion.com/v1/blocks/{page_id}/children"
                
                # 기본 콜아웃 블록 생성
                callout_block = {
                    "object": "block",
                    "type": "callout",
                    "callout": {
                        "rich_text": [],
                        "icon": callout_data["icon"],
                        "color": callout_data["color"]
                    }
                }
                
                # 첫 번째 heading_3 블록의 내용을 찾아서 콜아웃의 rich_text로 사용
                remaining_children = []
                if callout_data.get("children"):
                    for child in callout_data["children"]:
                        if child.get("type") == "heading_3" and not callout_block["callout"]["rich_text"]:
                            # heading_3의 내용과 서식 정보를 그대로 콜아웃의 rich_text로 사용
                            heading_block = child[child["type"]]
                            
                            # rich_text는 그대로 복사
                            callout_block["callout"]["rich_text"] = heading_block["rich_text"]
                            
                            # 제목 서식 유지를 위해 지원되는 속성만 사용
                            for text_item in callout_block["callout"]["rich_text"]:
                                # 기존 annotations 유지하면서 텍스트를 굵게만 설정
                                if "annotations" in text_item:
                                    text_item["annotations"]["bold"] = True  # 제목 효과를 내기 위해 굵게 설정
                            
                            # 배경색 gray_background 유지
                            callout_block["callout"]["color"] = "gray_background"
                        else:
                            remaining_children.append(child)
                
                callout_payload = {
                    "children": [callout_block]
                }
                
                # 콜아웃 블록 추가
                callout_response = requests.patch(callout_url, headers=headers, json=callout_payload)
                if callout_response.status_code == 200:
                    # 콜아웃 블록의 ID 가져오기
                    callout_id = callout_response.json()["results"][0]["id"]
                    
                    # 잠시 대기
                    time.sleep(retry_delay)
                    
                    # 콜아웃 블록의 하위 블록으로 내용 추가 (heading_3 제외)
                    children_url = f"https://api.notion.com/v1/blocks/{callout_id}/children"
                    
                    # 하위 블록들 생성 (heading_3 제외)
                    children_blocks = []
                    for child in remaining_children:
                        # 필요한 속성만 복사
                        new_block = {
                            "object": "block",
                            "type": child["type"]
                        }
                        
                        # 블록 타입에 따라 적절한 속성 복사 (서식 정보 포함)
                        block_type = child["type"]
                        if block_type in ["paragraph", "heading_1", "heading_2", "heading_3", 
                                        "bulleted_list_item", "numbered_list_item", "to_do", 
                                        "toggle", "code"]:
                            # 원본 블록의 모든 속성을 그대로 복사
                            new_block[block_type] = child[block_type].copy()
                            
                            # to_do 타입의 경우 checked 속성도 복사
                            if block_type == "to_do":
                                new_block[block_type]["checked"] = child[block_type]["checked"]
                            
                            # code 타입의 경우 language 속성도 복사
                            if block_type == "code":
                                new_block[block_type]["language"] = child[block_type]["language"]
                        
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
                elif callout_response.status_code == 409 and attempt < max_retries - 1:
                    print(f"콜아웃 블록 추가 시도 {attempt + 1}/{max_retries} 실패. 재시도 중...")
                    time.sleep(retry_delay)
                    continue
                else:
                    print(f"콜아웃 블록 추가 중 오류 발생: {callout_response.status_code}")
                    print(callout_response.text)
                    return None
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