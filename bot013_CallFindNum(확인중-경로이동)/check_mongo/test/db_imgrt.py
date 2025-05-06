import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

def check_done_documents():
    # MongoDB 연결 설정
    uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client['insta09_database']
    copy_collection = db['08_main_brand_category_data']
    main_collection = db['08_main_brand_category_data']
    
    # level이 'done'인 문서 수 조회
    done_count = copy_collection.count_documents({'level': 'done'})
    print(f"'done' 상태인 문서의 총 개수: {done_count}")
    
    # level이 'done'인 모든 문서 조회
    done_docs = copy_collection.find({'level': 'done'})
    missing_names = []
    existing_names = []
    updated_count = 0
    skipped_count = 0
    
    # 각 문서에 대해 main 컬렉션에서 검색
    for doc in done_docs:
        name = doc.get('name')
        if name:
            main_doc = main_collection.find_one({'name': name})
            if not main_doc:
                missing_names.append(name)
            else:
                existing_names.append(name)
    
    if missing_names:
        print("\n08_main_brand_category_data에 존재하지 않는 name들:")
        for name in missing_names:
            print(f"- {name}")
        print(f"\n총 {len(missing_names)}개의 name이 main 컬렉션에 없습니다.")
    else:
        print("\n모든 name이 08_main_brand_category_data에 존재합니다.")
    
    # main 컬렉션에 존재하는 문서들에 대해 level 필드 업데이트
    if existing_names:
        print(f"\n08_main_brand_category_data에 존재하는 {len(existing_names)}개의 문서에 대해 level 필드를 업데이트합니다.")
        for i, name in enumerate(existing_names):
            user_input = input(f"\n'{name}' 문서의 level 필드를 'done'으로 업데이트하시겠습니까? (y/n/all): ")
            if user_input.lower() == 'all':
                # 나머지 모든 문서 업데이트
                remaining_count = len(existing_names) - i
                for remaining_name in existing_names[i:]:
                    main_collection.update_one(
                        {'name': remaining_name},
                        {'$set': {'level': 'done'}}
                    )
                    print(f"'{remaining_name}' 문서가 업데이트되었습니다.")
                updated_count += remaining_count
                break
            elif user_input.lower() == 'y':
                main_collection.update_one(
                    {'name': name},
                    {'$set': {'level': 'done'}}
                )
                print(f"'{name}' 문서가 업데이트되었습니다.")
                updated_count += 1
            else:
                print(f"'{name}' 문서 업데이트를 건너뛰었습니다.")
                skipped_count += 1
    else:
        print("\n업데이트할 문서가 없습니다.")
    
    # 최종 결과 출력
    print("\n=== 최종 결과 ===")
    print(f"1. 08_copy_brand_category_data의 'done' 상태 문서 수: {done_count}")
    print(f"2. 08_main_brand_category_data에 존재하지 않는 문서 수: {len(missing_names)}")
    print(f"3. 업데이트된 문서 수: {updated_count}")
    print(f"4. 건너뛴 문서 수: {skipped_count}")
    print(f"5. 08_main_brand_category_data의 현재 'done' 상태 문서 수: {main_collection.count_documents({'level': 'done'})}")

if __name__ == "__main__":
    check_done_documents()
