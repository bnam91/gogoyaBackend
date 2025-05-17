import requests
import time
import sys
import json
from pathlib import Path
from collections import OrderedDict

# 상위 디렉토리를 파이썬 경로에 추가
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from config import headers

def update_database_properties_order(database_id):
    """
    데이터베이스 속성의 순서를 변경하는 함수
    
    Args:
        database_id (str): 데이터베이스 ID
        
    Returns:
        bool: 성공 여부
    """
    url = f"https://api.notion.com/v1/databases/{database_id}"
    
    # 원하는 순서로 속성을 재정의
    payload = {
        "properties": {
            "브랜드": {
                "type": "title",
                "title": {}
            },
            "item": {
                "type": "rich_text",
                "rich_text": {}
            },
            "URL": {
                "type": "url",
                "url": {}
            }
        }
    }
    
    try:
        custom_headers = headers.copy()
        custom_headers["Notion-Version"] = "2022-06-28"
        
        response = requests.patch(url, headers=custom_headers, json=payload)
        if response.status_code == 200:
            print("데이터베이스 속성 순서가 성공적으로 업데이트되었습니다.")
            return True
        else:
            print(f"데이터베이스 속성 순서 업데이트 실패: {response.status_code}")
            print(response.text)
            return False
    except Exception as e:
        print(f"데이터베이스 업데이트 중 오류 발생: {str(e)}")
        return False

def create_database(parent_id):
    """
    하위 페이지에 데이터베이스를 생성하는 함수
    
    Args:
        parent_id (str): 상위 페이지 ID
        
    Returns:
        str: 생성된 데이터베이스의 ID 또는 None (실패 시)
    """
    url = "https://api.notion.com/v1/databases"
    
    payload = {
        "parent": {
            "type": "page_id",
            "page_id": parent_id
        },
        "title": [
            {
                "type": "text",
                "text": {
                    "content": "소싱 리스트"
                }
            }
        ],
        "is_inline": True,
        "properties": {
            "브랜드": {
                "type": "title",
                "title": {}
            },
            "item": {
                "type": "rich_text",
                "rich_text": {}
            },
            "URL": {
                "type": "url",
                "url": {}
            }
        }
    }
    
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            # API 버전을 명시적으로 지정
            custom_headers = headers.copy()
            custom_headers["Notion-Version"] = "2022-06-28"
            
            response = requests.post(
                url, 
                headers=custom_headers,
                json=payload
            )
            
            if response.status_code == 200:
                database_id = response.json()["id"]
                print(f"데이터베이스가 성공적으로 생성되었습니다. (ID: {database_id})")
                
                # 데이터베이스 속성 순서 업데이트
                if update_database_properties_order(database_id):
                    print("데이터베이스 설정이 완료되었습니다.")
                else:
                    print("데이터베이스는 생성되었으나 속성 순서 변경에 실패했습니다.")
                
                return database_id
            elif response.status_code == 409 and attempt < max_retries - 1:
                print(f"데이터베이스 생성 시도 {attempt + 1}/{max_retries} 실패. 재시도 중...")
                time.sleep(retry_delay)
                continue
            else:
                print(f"데이터베이스 생성 중 오류 발생: {response.status_code}")
                print(response.text)
                return None
        except Exception as e:
            print(f"예상치 못한 오류 발생: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            return None
    
    return None

def duplicate_database(parent_id, source_database_id="1f6111a5778880d4b757d487033f182e"):
    """
    기존 데이터베이스를 복제하는 함수
    
    Args:
        parent_id (str): 새로운 데이터베이스가 생성될 페이지 ID
        source_database_id (str): 복사할 원본 데이터베이스 ID
        
    Returns:
        str: 생성된 데이터베이스의 ID 또는 None (실패 시)
    """
    # 먼저 원본 데이터베이스의 정보를 가져옴
    get_url = f"https://api.notion.com/v1/databases/{source_database_id}"
    
    try:
        custom_headers = headers.copy()
        custom_headers["Notion-Version"] = "2022-06-28"
        
        get_response = requests.get(get_url, headers=custom_headers)
        if get_response.status_code != 200:
            print(f"원본 데이터베이스 정보 가져오기 실패: {get_response.status_code}")
            print(get_response.text)
            return None
            
        source_db = get_response.json()
        
        # 원본 데이터베이스의 속성 정보 출력
        print("\n=== 원본 데이터베이스 속성 정보 ===")
        for prop_name, prop_info in source_db["properties"].items():
            print(f"속성명: {prop_name}")
            print(f"타입: {prop_info.get('type')}")
            print(f"설정: {prop_info}")
            print("---")
        
        # 새로운 데이터베이스 생성
        create_url = "https://api.notion.com/v1/databases"
        
        # 원본 데이터베이스의 속성을 그대로 사용
        create_payload = {
            "parent": {
                "type": "page_id",
                "page_id": parent_id
            },
            "title": source_db["title"],
            "is_inline": True,
            "properties": source_db["properties"]  # 원본 속성을 그대로 사용
        }
        
        # 생성 요청의 payload 출력
        print("\n=== 생성 요청 Payload ===")
        print(json.dumps(create_payload, indent=2, ensure_ascii=False))
        
        create_response = requests.post(
            create_url,
            headers=custom_headers,
            json=create_payload
        )
        
        if create_response.status_code == 200:
            database_id = create_response.json()["id"]
            
            # 생성된 데이터베이스의 속성 정보 출력
            print("\n=== 생성된 데이터베이스 속성 정보 ===")
            created_db = create_response.json()
            for prop_name, prop_info in created_db["properties"].items():
                print(f"속성명: {prop_name}")
                print(f"타입: {prop_info.get('type')}")
                print(f"설정: {prop_info}")
                print("---")
            
            print(f"데이터베이스가 성공적으로 복제되었습니다. (ID: {database_id})")
            return database_id
        else:
            print(f"데이터베이스 복제 중 오류 발생: {create_response.status_code}")
            print(create_response.text)
            return None
            
    except Exception as e:
        print(f"데이터베이스 복제 중 오류 발생: {str(e)}")
        return None

def add_items_to_database(database_id, items):
    """
    데이터베이스에 여러 아이템을 추가하는 함수
    
    Args:
        database_id (str): 데이터베이스 ID
        items (list): 추가할 아이템 리스트
    """
    url = "https://api.notion.com/v1/pages"
    
    # 먼저 데이터베이스의 현재 속성 구조를 가져옵니다
    db_url = f"https://api.notion.com/v1/databases/{database_id}"
    try:
        db_response = requests.get(db_url, headers=headers)
        if db_response.status_code != 200:
            print(f"데이터베이스 정보 가져오기 실패: {db_response.status_code}")
            return
        
        db_properties = db_response.json()["properties"]
        
        for item in items:
            properties = {}
            
            # 각 속성에 대해 적절한 형식으로 데이터를 변환
            for prop_name, prop_info in db_properties.items():
                prop_type = prop_info["type"]
                
                # JSON 데이터의 키 이름과 데이터베이스 속성 이름이 다를 수 있으므로 매핑
                json_key = prop_name
                if prop_name == "아이템":
                    json_key = "item"
                
                if json_key not in item:
                    continue
                
                if prop_type == "title":
                    properties[prop_name] = {
                        "title": [{"text": {"content": str(item[json_key])}}]
                    }
                elif prop_type == "rich_text":
                    properties[prop_name] = {
                        "rich_text": [{"text": {"content": str(item[json_key])}}]
                    }
                elif prop_type == "url":
                    properties[prop_name] = {"url": item[json_key]}
                elif prop_type == "select":
                    properties[prop_name] = {"select": {"name": str(item[json_key])}}
                elif prop_type == "multi_select":
                    if isinstance(item[json_key], list):
                        properties[prop_name] = {
                            "multi_select": [{"name": str(value)} for value in item[json_key]]
                        }
                    else:
                        properties[prop_name] = {
                            "multi_select": [{"name": str(item[json_key])}]
                        }
            
            payload = {
                "parent": {"database_id": database_id},
                "properties": properties
            }
            
            try:
                response = requests.post(url, headers=headers, json=payload)
                if response.status_code == 200:
                    print(f"아이템 추가 성공: {item.get('브랜드', '')} - {item.get('item', '')}")
                else:
                    print(f"아이템 추가 실패: {item.get('브랜드', '')} - {item.get('item', '')}")
                    print(f"오류 코드: {response.status_code}")
                    print(response.text)
                
                # API 속도 제한을 피하기 위한 대기
                time.sleep(0.5)
                
            except Exception as e:
                print(f"예상치 못한 오류 발생: {str(e)}")
                continue
                
    except Exception as e:
        print(f"데이터베이스 정보 가져오기 중 오류 발생: {str(e)}")
        return

def get_page_url(page_id):
    """
    페이지의 공유 링크를 가져오는 함수
    
    Args:
        page_id (str): 페이지 ID
        
    Returns:
        str: 페이지의 공유 링크
    """
    url = f"https://api.notion.com/v1/pages/{page_id}"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            page_url = response.json().get("url")
            print(f"페이지 공유 링크: {page_url}")
            return page_url
        else:
            print(f"페이지 정보 가져오기 실패: {response.status_code}")
            print(response.text)
            return None
    except Exception as e:
        print(f"페이지 정보 가져오기 중 오류 발생: {str(e)}")
        return None 