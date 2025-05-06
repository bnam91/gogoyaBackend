import os
from dotenv import load_dotenv
import requests
import io
import base64
import matplotlib.pyplot as plt

# .env 파일에서 환경 변수 로드
load_dotenv()

# Imgbb API를 사용한 이미지 업로드 함수
def upload_image_to_imgbb(img_base64):
    url = "https://api.imgbb.com/1/upload"
    imgbb_api_key = os.getenv("imgbb_api_key")  # 환경 변수에서 API 키 가져오기
    payload = {
        "key": imgbb_api_key,
        "image": img_base64
    }
    
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()  # HTTP 오류 발생 시 예외 발생
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"요청 중 오류 발생: {e}")  # 예외 발생

    if response.status_code == 200:
        try:
            return response.json()["data"]["url"]
        except KeyError:
            raise ValueError("예상치 못한 응답 형식입니다.")
    else:
        raise RuntimeError(f"이미지 업로드 중 오류 발생: {response.status_code}")

def save_and_upload_image(api_key):
    # 이미지를 메모리에 저장
    buf = io.BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', pad_inches=0.05, dpi=300)
    buf.seek(0)
    img_base64 = base64.b64encode(buf.getvalue()).decode('ascii')
    print("테이블 이미지가 메모리에 생성되었습니다.")

    # 이미지 업로드
    image_url = upload_image_to_imgbb(img_base64)
    
    if image_url:
        print(f"이미지가 성공적으로 업로드되었습니다. URL: {image_url}")
        return image_url
    else:
        print("이미지 업로드에 실패했습니다.")
        return None