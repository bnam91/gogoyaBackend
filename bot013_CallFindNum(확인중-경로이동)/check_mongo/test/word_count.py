import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import re
from collections import Counter
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from datetime import datetime

# NLTK 데이터 다운로드
nltk.download('punkt')
nltk.download('stopwords')

class WordCounterApp:
    def __init__(self, root):
        self.root = root
        self.root.title("URL 단어 분석기")
        self.root.geometry("800x600")
        
        # 제외할 단어 목록
        self.exclude_words = set([
            # URL 관련
            'http', 'https', 'www', 'com', 'net', 'co', 'kr',
            'html'
        ])
        
        # 제외할 URL 패턴
        self.exclude_url_patterns = [
            'store.naver.com',
            'brand.naver.com'
        ]
        
        # GUI 요소 생성
        self.create_widgets()
        
    def create_widgets(self):
        # 파일 선택 버튼
        self.select_button = tk.Button(
            self.root, 
            text="엑셀 파일 선택", 
            command=self.select_file,
            width=20,
            height=2
        )
        self.select_button.pack(pady=20)
        
        # 결과 표시 텍스트 영역
        self.result_text = tk.Text(self.root, height=30, width=80)
        self.result_text.pack(pady=20)
        
    def select_file(self):
        file_path = filedialog.askopenfilename(
            filetypes=[("Excel files", "*.xlsx *.xls")]
        )
        if file_path:
            self.analyze_excel(file_path)
            
    def analyze_excel(self, file_path):
        try:
            # 엑셀 파일 읽기
            df = pd.read_excel(file_path)
            
            # D열의 URL 가져오기
            urls = df.iloc[:, 3].dropna().tolist()
            
            # 제외할 URL 패턴 카운트
            store_count = 0
            brand_count = 0
            
            # 필터링된 URL 목록
            filtered_urls = []
            
            for url in urls:
                if 'store.naver.com' in url:
                    store_count += 1
                    continue
                if 'brand.naver.com' in url:
                    brand_count += 1
                    continue
                filtered_urls.append(url)
            
            # 모든 URL의 단어 추출
            all_words = []
            for url in filtered_urls:
                # URL 디코딩 및 소문자 변환
                url = url.lower()
                
                # URL에서 의미있는 단어 추출 (한글, 영문 3글자 이상)
                words = re.findall(r'[a-zA-Z]{3,}|[가-힣]{3,}', url)
                
                # 제외할 단어 필터링
                words = [word for word in words if word not in self.exclude_words]
                all_words.extend(words)
            
            # 단어 빈도수 계산
            word_counts = Counter(all_words)
            
            # 결과 표시
            self.result_text.delete(1.0, tk.END)
            self.result_text.insert(tk.END, f"store.naver.com URL 수: {store_count}개\n")
            self.result_text.insert(tk.END, f"brand.naver.com URL 수: {brand_count}개\n\n")
            self.result_text.insert(tk.END, "상위 50개 단어 (3글자 이상):\n\n")
            
            # 결과를 데이터프레임으로 변환
            result_data = []
            for word, count in word_counts.most_common(50):
                self.result_text.insert(tk.END, f"{word}: {count}회\n")
                result_data.append({'단어': word, '출현 횟수': count})
            
            # 결과를 엑셀로 저장
            result_df = pd.DataFrame(result_data)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f'word_count_result_{timestamp}.xlsx'
            result_df.to_excel(output_filename, index=False)
            
            self.result_text.insert(tk.END, f"\n\n분석 결과가 '{output_filename}' 파일로 저장되었습니다.")
            messagebox.showinfo("완료", f"분석이 완료되었습니다.\n결과가 '{output_filename}' 파일로 저장되었습니다.")
                
        except Exception as e:
            messagebox.showerror("오류", f"파일 분석 중 오류가 발생했습니다: {str(e)}")

if __name__ == "__main__":
    root = tk.Tk()
    app = WordCounterApp(root)
    root.mainloop()
