import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import json
from datetime import datetime

def setup_environment():
    """환경 설정 함수"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    dotenv_path = os.path.join(parent_dir, '.env')
    load_dotenv(dotenv_path)

    # MongoDB 연결 설정
    uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
    client = MongoClient(uri, server_api=ServerApi('1'))
    db = client['insta09_database']
    collection = db['gogoya_vendor_brand_info']
    
    return collection

def backup_current_values():
    """현재 값을 백업하는 함수"""
    collection = setup_environment()
    
    # 현재 디렉토리에 backup 폴더 생성
    backup_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backup')
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    
    # 현재 시간을 파일명에 포함
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(backup_dir, f'backup_{timestamp}.json')
    
    # 모든 문서의 is_verified 필드 값 백업
    backup_data = []
    for doc in collection.find({}, {'_id': 1, 'is_verified': 1}):
        backup_data.append({
            '_id': str(doc['_id']),
            'is_verified': doc.get('is_verified', '')
        })
    
    # 백업 파일 저장
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(backup_data, f, ensure_ascii=False, indent=2)
    
    print(f"백업이 완료되었습니다: {backup_file}")
    return backup_file

def restore_from_backup(backup_file):
    """백업 파일에서 값을 복원하는 함수"""
    collection = setup_environment()
    
    try:
        # 백업 파일 읽기
        with open(backup_file, 'r', encoding='utf-8') as f:
            backup_data = json.load(f)
        
        # 각 문서 복원
        for doc in backup_data:
            collection.update_one(
                {'_id': ObjectId(doc['_id'])},
                {'$set': {'is_verified': doc['is_verified']}}
            )
        
        print(f"복원이 완료되었습니다: {backup_file}")
        
    except Exception as e:
        print(f"복원 중 오류 발생: {e}")

def update_is_verified():
    """is_verified 필드를 'yet'으로 업데이트"""
    collection = setup_environment()
    
    # 현재 값 백업
    backup_file = backup_current_values()
    
    # 사용자 확인
    print("\n주의: 이 작업은 모든 문서의 is_verified 필드를 'yet'으로 변경합니다.")
    print(f"백업 파일이 생성되었습니다: {backup_file}")
    user_input = input("계속 진행하시겠습니까? (y/n): ")
    
    if user_input.lower() != 'y':
        print("작업이 취소되었습니다.")
        return
    
    # 모든 문서의 is_verified 필드를 'yet'으로 업데이트
    result = collection.update_many(
        {},  # 모든 문서에 대해
        {"$set": {"is_verified": "yet"}}  # is_verified 필드를 'yet'으로 설정
    )
    
    print(f"총 {result.modified_count}개의 문서가 업데이트되었습니다.")
    print(f"백업 파일: {backup_file}")

if __name__ == "__main__":
    import sys
    from bson import ObjectId
    
    if len(sys.argv) > 1 and sys.argv[1] == '--restore':
        # 백업 파일 목록 보여주기
        backup_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'backup')
        if os.path.exists(backup_dir):
            backup_files = sorted([f for f in os.listdir(backup_dir) if f.endswith('.json')])
            if backup_files:
                print("\n사용 가능한 백업 파일:")
                for i, file in enumerate(backup_files, 1):
                    print(f"{i}. {file}")
                
                try:
                    choice = int(input("\n복원할 백업 파일 번호를 입력하세요: "))
                    if 1 <= choice <= len(backup_files):
                        restore_from_backup(os.path.join(backup_dir, backup_files[choice-1]))
                    else:
                        print("잘못된 번호입니다.")
                except ValueError:
                    print("숫자를 입력해주세요.")
            else:
                print("사용 가능한 백업 파일이 없습니다.")
        else:
            print("백업 폴더가 없습니다.")
    else:
        update_is_verified() 