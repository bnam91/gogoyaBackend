"""
이 모듈은 브랜드 정보와 관련된 MongoDB 데이터베이스 작업을 위한 유틸리티 함수들을 제공합니다.

주요 기능:
1. get_brand_names_from_mongodb: MongoDB에서 브랜드명 목록을 조회합니다.
2. save_contact_info_to_mongodb: 브랜드의 연락처 정보를 MongoDB에 저장합니다.

이 모듈은 브랜드 관련 정보를 관리하는 MongoDB 컬렉션과 상호작용하여
브랜드명을 검색하고, 브랜드 연락처 정보(웹사이트, 고객센터 번호, 주소 등)를 저장합니다.
"""

def get_brand_names_from_mongodb(collection, only_unprocessed=True):
    """MongoDB에서 브랜드명 가져오기"""
    try:
        # 처리되지 않은 브랜드만 필터링하기 위한 쿼리
        query = {}
        if only_unprocessed:
            # level 필드가 없거나 빈 값인 문서만 선택
            query = {"$or": [{"level": {"$exists": False}}, {"level": ""}]}
        
        # 쿼리에 맞는 브랜드 찾기
        brands = collection.find(query, {"name": 1})
        brand_names = [brand.get("name") for brand in brands if brand.get("name")]
        
        if brand_names:
            print(f"MongoDB에서 불러온 브랜드 수: {len(brand_names)}")
            print(f"첫 5개 브랜드: {brand_names[:5] if len(brand_names) >= 5 else brand_names}")
            return brand_names
        else:
            print("MongoDB에서 처리할 브랜드명을 찾을 수 없습니다.")
            return ["브랜든"]  # 기본값
    except Exception as e:
        print(f"MongoDB 브랜드 정보 조회 오류: {e}")
        return ["브랜든"]  # 기본값

def save_contact_info_to_mongodb(brand_name, extracted_info, write_collection, read_collection=None):
    """브랜드 연락처 정보를 MongoDB에 저장하는 함수"""
    
    # 디버깅용 로그 추가
    print(f"\n======= MongoDB 저장 시도 =======")
    print(f"브랜드명: {brand_name}")
    print(f"쓰기 컬렉션 이름: {write_collection.name}")
    print(f"처리된 데이터 필드 수: {len(extracted_info)}")
    
    # 결과 필드는 저장하지 않기 위해 복사본 생성
    extracted_info_copy = extracted_info.copy()
    if "결과" in extracted_info_copy:
        print(f"'결과' 필드({extracted_info_copy['결과']})는 MongoDB에 저장하지 않습니다.")
        del extracted_info_copy["결과"]
    
    # 필드 매핑 (한글 키 -> 영문 키)
    field_mapping = {
        "브랜드명": "brand_name",
        "공식 홈페이지 URL": "official_website_url",
        "실제 도메인 주소": "actual_domain_url",
        "검색 URL": "search_url",
        "도메인유형": "domain_type",
        "상호명": "company_name",
        "고객센터 번호": "customer_service_number",
        "사업장소재지": "business_address",
        "이메일주소": "email",
        "페이지 스크린샷": "screenshot"
    }
    
    # 추출된 정보에서 필드 매핑에 따라 값 설정
    contact = {field_mapping[k]: v for k, v in extracted_info_copy.items() if k in field_mapping and v}
    
    # 필수 필드에 기본값 설정 (비어있는 경우)
    default_fields = {
        "official_website_url": "",
        "actual_domain_url": "",
        "domain_type": "",
        "company_name": "",
        "customer_service_number": "",
        "business_address": "",
        "email": "",
        "screenshot": ""
    }
    
    # 기본값으로 빈 필드 채우기
    for key, default_value in default_fields.items():
        if key not in contact:
            contact[key] = default_value
    
    # name 필드 추가 (upsert를 위한 기본 필드)
    contact["name"] = brand_name
    
    # 필수 추가 필드 설정
    contact["is_verified"] = "yet"
    contact["sourcing_status"] = ""
    
    # read_collection에서 aliases 필드 가져오기
    if read_collection is not None:
        try:
            brand_doc = read_collection.find_one({"name": brand_name})
            if brand_doc and "aliases" in brand_doc:
                contact["aliases"] = brand_doc["aliases"]
            else:
                contact["aliases"] = []  # aliases가 없으면 빈 배열 설정
        except Exception as e:
            print(f"aliases 필드 가져오기 실패: {e}")
            contact["aliases"] = []  # 오류 발생 시 빈 배열로 설정
    
    # 저장될 데이터 출력
    print(f"저장할 데이터: {contact}")
    
    # MongoDB에 저장 또는 업데이트 - name 필드로 검색
    query = {"name": brand_name}
    update = {"$set": contact}
    
    try:
        # 직접 insert_one 시도
        try:
            # 먼저 기존 문서 삭제
            write_collection.delete_one(query)
            # 새로운 문서 삽입
            result = write_collection.insert_one(contact)
            print(f"브랜드 '{brand_name}' 문서 생성됨: {result.inserted_id}")
            return True
        except Exception as e:
            print(f"직접 삽입 중 오류 발생: {e}")
            
            # update_one으로 대체 시도
            result = write_collection.update_one(query, update, upsert=True)
            print(f"업데이트 결과: modified={result.modified_count}, upserted_id={result.upserted_id}")
            
            if result.modified_count > 0 or result.upserted_id:
                print(f"브랜드 '{brand_name}'의 연락처 정보가 업데이트되었습니다.")
                return True
            else:
                print(f"브랜드 '{brand_name}'의 정보가 변경되지 않았습니다.")
                return False
    except Exception as e:
        print(f"MongoDB 저장 중 오류 발생: {e}")
        return False

def mark_brand_as_processed(collection, brand_name):
    """브랜드를 처리완료로 표시"""
    try:
        result = collection.update_one(
            {"name": brand_name},
            {"$set": {"level": "done"}}
        )
        if result.modified_count > 0:
            print(f"브랜드 '{brand_name}'을 처리완료로 표시했습니다.\n\n\n")  # 줄바꿈 3번 추가
            return True
        else:
            print(f"브랜드 '{brand_name}'의 상태 업데이트에 실패했습니다.\n\n\n")  # 줄바꿈 3번 추가
            return False
    except Exception as e:
        print(f"처리 상태 업데이트 중 오류 발생: {e}\n\n\n")  # 줄바꿈 3번 추가
        return False
