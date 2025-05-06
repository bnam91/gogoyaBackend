from pymongo import MongoClient
from pymongo.server_api import ServerApi

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))
db = client['insta09_database']
collection = db['dev_gogoya_vendor_brand_info']

# 모든 문서를 순회하며 is_verified 필드 업데이트
cursor = collection.find({})
for doc in cursor:
    if doc.get('is_verified') is True:
        collection.update_one(
            {'_id': doc['_id']},
            {'$set': {'is_verified': "true"}}
        )
        print(f"문서 ID {doc['_id']}의 is_verified 필드가 'true'로 업데이트되었습니다.")
    elif doc.get('is_verified') is False:
        collection.update_one(
            {'_id': doc['_id']},
            {'$set': {'is_verified': "false"}}
        )
        print(f"문서 ID {doc['_id']}의 is_verified 필드가 'false'로 업데이트되었습니다.")

print("모든 문서 업데이트가 완료되었습니다.")