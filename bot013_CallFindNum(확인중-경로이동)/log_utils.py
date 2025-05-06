import logging
import os
from datetime import datetime

def setup_logger(brand_name=None):
    """로거 설정 함수"""
    # 로그 저장 디렉토리 생성
    log_dir = "log"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # 로그 파일명 생성 (날짜_시간_브랜드명.log)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if brand_name:
        log_filename = f"{log_dir}/{timestamp}_{brand_name}.log"
    else:
        log_filename = f"{log_dir}/{timestamp}_process.log"
    
    # 기존 로거 초기화
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # 로거 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler()  # 콘솔에도 출력
        ]
    )
    
    # 로거 반환
    logger = logging.getLogger()
    logger.info(f"로그 파일이 생성되었습니다: {log_filename}")
    return logger 