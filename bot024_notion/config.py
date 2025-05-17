import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수 로드
load_dotenv()

# Notion API 설정
NOTION_API_KEY = os.getenv('NOTION_API_KEY')
if not NOTION_API_KEY:
    raise ValueError("NOTION_API_KEY가 설정되지 않았습니다. .env 파일을 확인해주세요.")

PAGE_ID = "1f6111a5778880e3bd03cd0a2bae843b"  # 페이지 ID
DATABASE_ID = "1f6111a5778880e3bd03cd0a2bae843b"  # 데이터베이스 ID

headers = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
} 