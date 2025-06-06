import tkinter as tk
from tkinter import ttk, messagebox, simpledialog, filedialog
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson import ObjectId
from datetime import datetime
import os
import sys
import subprocess  # 추가: 폴더 열기 기능을 위한 모듈
import webbrowser  # 웹 브라우저 열기 위한 모듈 추가

# col_to_json.py와 col_copy_bot.py가 있는 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import col_to_json

class MongoControlGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("MongoDB 컨트롤")
        self.root.geometry("1200x400")
        
        # MongoDB 연결
        self.connect_to_mongodb()
        
        # 프레임 생성
        self.main_frame = ttk.Frame(self.root, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 데이터베이스 및 컬렉션 선택 프레임
        self.selection_frame = ttk.Frame(self.main_frame)
        self.selection_frame.pack(fill=tk.X, pady=10)
        
        # 상단 버튼 프레임
        self.top_button_frame = ttk.Frame(self.selection_frame)
        self.top_button_frame.pack(fill=tk.X, pady=5)
        
        # JSON 업로드 버튼
        self.upload_json_btn = ttk.Button(self.top_button_frame, text="JSON 업로드", command=self.upload_from_json)
        self.upload_json_btn.pack(side=tk.LEFT, padx=5)
        
        # MongoDB 열기 버튼
        self.open_mongodb_btn = ttk.Button(self.top_button_frame, text="MongoDB 열기", command=self.open_mongodb_console)
        self.open_mongodb_btn.pack(side=tk.LEFT, padx=5)
        
        # 데이터베이스 및 컬렉션 선택 프레임
        self.db_collection_frame = ttk.Frame(self.selection_frame)
        self.db_collection_frame.pack(fill=tk.X, pady=5)
        
        # 데이터베이스 선택 콤보박스
        ttk.Label(self.db_collection_frame, text="데이터베이스:").pack(side=tk.LEFT, padx=5)
        self.db_combo = ttk.Combobox(self.db_collection_frame, width=20, height=20)
        self.db_combo.pack(side=tk.LEFT, padx=5)
        self.db_combo.bind("<<ComboboxSelected>>", self.update_collections)
        
        # 컬렉션 선택 콤보박스
        ttk.Label(self.db_collection_frame, text="컬렉션:").pack(side=tk.LEFT, padx=5)
        self.collection_combo = ttk.Combobox(self.db_collection_frame, width=20, height=20)
        self.collection_combo.pack(side=tk.LEFT, padx=5)
        self.collection_combo.bind("<<ComboboxSelected>>", self.on_collection_selected)
        
        # 최근 도큐먼트 표시 프레임
        self.recent_doc_frame = ttk.LabelFrame(self.main_frame, text="최근 도큐먼트", padding="10")
        self.recent_doc_frame.pack(fill=tk.X, pady=5, padx=10)

        # 최근 도큐먼트 텍스트 영역
        self.recent_doc_text = tk.Text(self.recent_doc_frame, height=5, width=50)
        self.recent_doc_text.pack(fill=tk.X, pady=5)
        self.recent_doc_text.config(state='disabled')  # 읽기 전용으로 설정
        
        # 하단 버튼 프레임
        self.bottom_button_frame = ttk.Frame(self.selection_frame)
        self.bottom_button_frame.pack(fill=tk.X, pady=5)
        
        # JSON 저장 버튼
        self.save_json_btn = ttk.Button(self.bottom_button_frame, text="JSON 저장", command=self.save_to_json)
        self.save_json_btn.pack(side=tk.LEFT, padx=5)
        
        # 콜렉션 복사 버튼
        self.copy_collection_btn = ttk.Button(self.bottom_button_frame, text="콜렉션 복사", command=self.copy_collection)
        self.copy_collection_btn.pack(side=tk.LEFT, padx=5)
        
        # 콜렉션 삭제 버튼
        self.delete_collection_btn = ttk.Button(self.bottom_button_frame, text="콜렉션 삭제", command=self.delete_collection)
        self.delete_collection_btn.pack(side=tk.LEFT, padx=5)
        
        # 콜렉션 이름 변경 버튼
        self.rename_collection_btn = ttk.Button(self.bottom_button_frame, text="이름 변경", command=self.rename_collection)
        self.rename_collection_btn.pack(side=tk.LEFT, padx=5)
        
        # 도큐먼트 삭제 버튼
        self.delete_documents_btn = ttk.Button(self.bottom_button_frame, text="도큐 전체삭제", command=self.delete_documents)
        self.delete_documents_btn.pack(side=tk.LEFT, padx=5)
        
        # 폴더 열기 버튼
        self.open_folder_btn = ttk.Button(self.bottom_button_frame, text="폴더 열기", command=self.open_col_data_folder)
        self.open_folder_btn.pack(side=tk.LEFT, padx=5)
        
        # 버튼 프레임 생성
        self.button_frame = ttk.Frame(self.main_frame)
        self.button_frame.pack(pady=20)
        
        # 버튼 생성
        self.button1 = ttk.Button(self.button_frame, text="버튼 1")
        self.button1.pack(side=tk.LEFT, padx=10)
        
        self.button2 = ttk.Button(self.button_frame, text="버튼 2")
        self.button2.pack(side=tk.LEFT, padx=10)
        
        self.button3 = ttk.Button(self.button_frame, text="버튼 3")
        self.button3.pack(side=tk.LEFT, padx=10)

        # 도큐먼트 추가 프레임
        self.document_frame = ttk.LabelFrame(self.main_frame, text="도큐먼트 추가", padding="10")
        self.document_frame.pack(fill=tk.X, pady=10, padx=10)

        # 키-값 입력 프레임
        self.key_value_frame = ttk.Frame(self.document_frame)
        self.key_value_frame.pack(fill=tk.X, pady=5)

        # 키 입력
        ttk.Label(self.key_value_frame, text="키:").pack(side=tk.LEFT, padx=5)
        self.key_entry = ttk.Entry(self.key_value_frame, width=20)
        self.key_entry.pack(side=tk.LEFT, padx=5)

        # 값 입력
        ttk.Label(self.key_value_frame, text="값:").pack(side=tk.LEFT, padx=5)
        self.value_entry = ttk.Entry(self.key_value_frame, width=20)
        self.value_entry.pack(side=tk.LEFT, padx=5)

        # 키-값 쌍 추가 버튼
        self.add_pair_btn = ttk.Button(self.key_value_frame, text="키-값 추가", command=self.add_key_value_pair)
        self.add_pair_btn.pack(side=tk.LEFT, padx=5)

        # 키-값 쌍 표시 리스트박스
        self.pairs_listbox = tk.Listbox(self.document_frame, height=5, width=50)
        self.pairs_listbox.pack(fill=tk.X, pady=5)

        # 리스트박스 선택 이벤트 바인딩
        self.pairs_listbox.bind('<<ListboxSelect>>', self.on_select_pair)

        # 도큐먼트 추가 버튼
        self.add_document_btn = ttk.Button(self.document_frame, text="도큐먼트 추가", command=self.add_document)
        self.add_document_btn.pack(pady=5)

        # 저장된 키-값 쌍
        self.document_data = {}

        # 데이터베이스 목록 로드
        self.load_databases()

    def connect_to_mongodb(self):
        uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        self.client = MongoClient(uri, server_api=ServerApi('1'))
        try:
            # 연결 확인
            self.client.admin.command('ping')
            print("MongoDB 연결 성공!")
        except Exception as e:
            print(f"MongoDB 연결 실패: {e}")

    def load_databases(self):
        try:
            # 시스템 데이터베이스 제외하고 사용자 데이터베이스만 표시
            db_list = [db for db in self.client.list_database_names() 
                      if db not in ['admin', 'local', 'config']]
            self.db_combo['values'] = db_list
            
            if db_list:
                self.db_combo.current(0)  # 첫 번째 데이터베이스 선택
                self.update_collections(None)  # 컬렉션 목록 업데이트
        except Exception as e:
            print(f"데이터베이스 로드 오류: {e}")

    def update_collections(self, event):
        try:
            selected_db = self.db_combo.get()
            if selected_db:
                db = self.client[selected_db]
                collection_list = db.list_collection_names()
                # 컬렉션 이름을 알파벳 순으로 정렬
                collection_list = sorted(collection_list)
                self.collection_combo['values'] = collection_list
                
                if collection_list:
                    self.collection_combo.current(0)  # 첫 번째 컬렉션 선택
        except Exception as e:
            print(f"컬렉션 로드 오류: {e}")

    def save_to_json(self):
        selected_db = self.db_combo.get()
        selected_collection = self.collection_combo.get()
        
        if not selected_db or not selected_collection:
            print("데이터베이스와 컬렉션을 모두 선택해주세요.")
            messagebox.showwarning("경고", "데이터베이스와 컬렉션을 모두 선택해주세요.")
            return
            
        try:
            # col_to_json 모듈의 함수를 사용하여 JSON으로 저장
            result = col_to_json.save_collection_to_json(self.client, selected_db, selected_collection)
            
            if result:
                # 현재 스크립트 파일의 디렉토리 경로 가져오기
                script_dir = os.path.dirname(os.path.abspath(__file__))
                # col_data 경로
                col_data_dir = os.path.join(script_dir, "col_data")
                # 저장된 파일 경로
                json_file_path = os.path.join(col_data_dir, f"{selected_collection}.json")
                
                # GUI 알림 표시
                messagebox.showinfo("저장 완료", 
                    f"컬렉션 '{selected_collection}'의 데이터가\n'{json_file_path}'에 저장되었습니다.")
                
        except Exception as e:
            error_msg = f"JSON 저장 오류: {e}"
            print(error_msg)
            messagebox.showerror("오류", error_msg)

    def open_col_data_folder(self):
        try:
            # 현재 스크립트 파일의 디렉토리 경로 가져오기
            script_dir = os.path.dirname(os.path.abspath(__file__))
            # col_data 경로
            col_data_dir = os.path.join(script_dir, "col_data")
            
            # col_data 디렉토리가 없으면 생성
            if not os.path.exists(col_data_dir):
                os.makedirs(col_data_dir)
                
            # 운영 체제에 따라 적절한 명령어로 폴더 열기
            if os.name == 'nt':  # Windows
                os.startfile(col_data_dir)
            elif os.name == 'posix':  # macOS, Linux
                if sys.platform == 'darwin':  # macOS
                    subprocess.call(['open', col_data_dir])
                else:  # Linux
                    subprocess.call(['xdg-open', col_data_dir])
            
        except Exception as e:
            error_msg = f"폴더 열기 오류: {e}"
            print(error_msg)
            messagebox.showerror("오류", error_msg)

    def copy_collection(self):
        selected_db = self.db_combo.get()
        selected_collection = self.collection_combo.get()
        
        if not selected_db or not selected_collection:
            messagebox.showwarning("경고", "데이터베이스와 컬렉션을 모두 선택해주세요.")
            return
            
        # 새 컬렉션 이름 입력 받기 - 원본 컬렉션 이름에 "_copy" 접미사를 기본값으로 설정
        default_name = f"{selected_collection}_copy"
        backup_collection_name = simpledialog.askstring("컬렉션 복사", 
                                                        f"'{selected_collection}'을(를) 복사할 새 컬렉션 이름을 입력하세요:",
                                                        parent=self.root,
                                                        initialvalue=default_name)
        
        # 입력 검증
        if not backup_collection_name:
            return  # 사용자가 취소하거나 빈 이름을 입력한 경우
        
        if backup_collection_name == selected_collection:
            messagebox.showwarning("경고", "새 컬렉션 이름이 원본 컬렉션과 동일합니다.")
            return
            
        try:
            db = self.client[selected_db]
            source_collection = db[selected_collection]
            backup_collection = db[backup_collection_name]
            
            # 데이터 가져오기
            all_documents = list(source_collection.find({}))
            
            # _id 필드 제거 (새 컬렉션에 삽입 시 자동 생성됨)
            for doc in all_documents:
                if '_id' in doc:
                    doc.pop('_id')
            
            # 백업 컬렉션에 데이터 삽입
            if all_documents:
                backup_collection.insert_many(all_documents)
                
                # 인덱스 복사
                indexes = source_collection.index_information()
                for index_name, index_info in indexes.items():
                    if index_name != '_id_':  # 기본 _id 인덱스는 자동 생성되므로 제외
                        backup_collection.create_index(
                            index_info['key'], 
                            **{k: v for k, v in index_info.items() if k != 'key'}
                        )
                
                # 성공 메시지
                messagebox.showinfo("복사 완료", 
                                   f"컬렉션 '{selected_collection}'의 데이터가\n'{backup_collection_name}'으로 복사되었습니다.\n({len(all_documents)}개 문서)")
                
                # 컬렉션 목록 갱신
                self.update_collections(None)
            else:
                messagebox.showinfo("알림", f"컬렉션 '{selected_collection}'에는 복사할 데이터가 없습니다.")
                
        except Exception as e:
            error_msg = f"컬렉션 복사 오류: {e}"
            print(error_msg)
            messagebox.showerror("오류", error_msg)

    def delete_collection(self):
        selected_db = self.db_combo.get()
        selected_collection = self.collection_combo.get()
        
        if not selected_db or not selected_collection:
            messagebox.showwarning("경고", "데이터베이스와 컬렉션을 모두 선택해주세요.")
            return
            
        # 비밀번호 입력 요청
        password = simpledialog.askstring("비밀번호 확인", 
                                          "컬렉션을 삭제하려면 비밀번호를 입력하세요:",
                                          show='*')
        
        # 비밀번호 확인
        if password != "0000":
            messagebox.showerror("오류", "비밀번호가 올바르지 않습니다.")
            return
            
        # 비밀번호가 맞으면 계속 진행
        # 사용자에게 삭제 확인 요청
        confirm = messagebox.askyesno("삭제 확인", 
                                     f"정말로 '{selected_db}' 데이터베이스의 '{selected_collection}' 컬렉션을 삭제하시겠습니까?\n\n이 작업은 되돌릴 수 없습니다!")
        
        if not confirm:
            return  # 사용자가 취소함
            
        try:
            db = self.client[selected_db]
            # 컬렉션 삭제
            db.drop_collection(selected_collection)
            
            messagebox.showinfo("삭제 완료", f"'{selected_collection}' 컬렉션이 성공적으로 삭제되었습니다.")
            
            # 컬렉션 목록 갱신
            self.update_collections(None)
            
        except Exception as e:
            error_msg = f"컬렉션 삭제 오류: {e}"
            print(error_msg)
            messagebox.showerror("오류", error_msg)

    def open_mongodb_console(self):
        # MongoDB Atlas 콘솔 URL 열기
        mongodb_url = "https://cloud.mongodb.com/v2/67b53ffbba89e066f00516de#/metrics/replicaSet/67b540e4a89b973ad2a8cd52/explorer/insta09_database"
        try:
            webbrowser.open(mongodb_url)
        except Exception as e:
            error_msg = f"MongoDB 콘솔 열기 오류: {e}"
            print(error_msg)
            messagebox.showerror("오류", error_msg)

    def upload_from_json(self):
        # JSON 업로드 다이얼로그 창 생성
        upload_dialog = JSONUploadDialog(self.root, self.client, self.db_combo.get())
        
        # 다이얼로그가 완료되면 컬렉션 목록 갱신
        if upload_dialog.result:
            self.update_collections(None)

    def rename_collection(self):
        selected_db = self.db_combo.get()
        selected_collection = self.collection_combo.get()
        
        if not selected_db or not selected_collection:
            messagebox.showwarning("경고", "데이터베이스와 컬렉션을 모두 선택해주세요.")
            return
            
        # 새 컬렉션 이름 입력 받기
        new_collection_name = simpledialog.askstring("컬렉션 이름 변경", 
                                                    f"'{selected_collection}'의 새 이름을 입력하세요:",
                                                    parent=self.root,
                                                    initialvalue=selected_collection)
        
        # 입력 검증
        if not new_collection_name:
            return  # 사용자가 취소하거나 빈 이름을 입력한 경우
        
        if new_collection_name == selected_collection:
            messagebox.showinfo("알림", "새 이름이 기존 이름과 동일합니다.")
            return
        
        # 데이터베이스에 새 이름의 컬렉션이 이미 존재하는지 확인
        db = self.client[selected_db]
        if new_collection_name in db.list_collection_names():
            messagebox.showerror("오류", f"'{new_collection_name}' 컬렉션이 이미 존재합니다.")
            return
            
        try:
            # MongoDB에서는 컬렉션 이름을 직접 변경하는 방법이 없으므로
            # 새 컬렉션을 만들고 데이터를 복사한 다음 원본을 삭제함
            
            # 원본 컬렉션
            source_collection = db[selected_collection]
            # 새 컬렉션
            target_collection = db[new_collection_name]
            
            # 모든 문서 가져오기
            all_documents = list(source_collection.find({}))
            
            # 문서가 있는 경우 복사
            if all_documents:
                # _id 필드 유지하여 삽입 (원본 ID 유지)
                target_collection.insert_many(all_documents)
                
                # 인덱스 복사
                indexes = source_collection.index_information()
                for index_name, index_info in indexes.items():
                    if index_name != '_id_':  # 기본 _id 인덱스는 자동 생성되므로 제외
                        target_collection.create_index(
                            index_info['key'], 
                            **{k: v for k, v in index_info.items() if k != 'key'}
                        )
            
            # 원본 컬렉션 삭제
            db.drop_collection(selected_collection)
            
            # 성공 메시지
            messagebox.showinfo("이름 변경 완료", 
                               f"컬렉션 이름이 '{selected_collection}'에서 '{new_collection_name}'으로 변경되었습니다.")
            
            # 컬렉션 목록 갱신
            self.update_collections(None)
            
            # 새 컬렉션 선택
            if new_collection_name in self.collection_combo['values']:
                self.collection_combo.set(new_collection_name)
            
        except Exception as e:
            error_msg = f"컬렉션 이름 변경 오류: {e}"
            print(error_msg)
            messagebox.showerror("오류", error_msg)

    def delete_documents(self):
        selected_db = self.db_combo.get()
        selected_collection = self.collection_combo.get()
        
        if not selected_db or not selected_collection:
            messagebox.showwarning("경고", "데이터베이스와 컬렉션을 모두 선택해주세요.")
            return
            
        # 비밀번호 입력 요청
        password = simpledialog.askstring("비밀번호 확인", 
                                          "도큐먼트를 삭제하려면 비밀번호를 입력하세요:",
                                          show='*')
        
        # 비밀번호 확인
        if password != "0000":
            messagebox.showerror("오류", "비밀번호가 올바르지 않습니다.")
            return
            
        # 사용자에게 삭제 확인 요청
        confirm = messagebox.askyesno("삭제 확인", 
                                     f"정말로 '{selected_db}' 데이터베이스의 '{selected_collection}' 컬렉션의 모든 도큐먼트를 삭제하시겠습니까?\n\n이 작업은 되돌릴 수 없습니다!")
        
        if not confirm:
            return  # 사용자가 취소함
            
        try:
            db = self.client[selected_db]
            collection = db[selected_collection]
            
            # 컬렉션의 모든 도큐먼트 삭제
            result = collection.delete_many({})
            
            messagebox.showinfo("삭제 완료", f"'{selected_collection}' 컬렉션에서 {result.deleted_count}개의 도큐먼트가 삭제되었습니다.")
            
        except Exception as e:
            error_msg = f"도큐먼트 삭제 오류: {e}"
            print(error_msg)
            messagebox.showerror("오류", error_msg)

    def parse_text(self):
        text = self.text_input.get("1.0", tk.END).strip()
        if not text:
            return

        # 기존 데이터 초기화
        self.document_data = {}
        self.pairs_listbox.delete(0, tk.END)

        lines = text.split('\n')
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            if not line:
                i += 1
                continue

            # 키 라인
            if not line.startswith('Array') and not line.startswith('String'):
                key = line
                value = None
                i += 1

                # 다음 라인이 있고 값이 있는 경우
                if i < len(lines):
                    next_line = lines[i].strip()
                    
                    # String 타입인 경우
                    if next_line == 'String':
                        i += 1  # String 라인 건너뛰기
                        if i < len(lines):
                            value = lines[i].strip()
                            if not value:  # 빈 값인 경우
                                value = " "
                            self.document_data[key] = value
                            self.pairs_listbox.insert(tk.END, f'{key}: "{value}"')
                    
                    # Array 타입인 경우
                    elif next_line.startswith('Array'):
                        array_size = next_line.split('(')[1].split(')')[0].strip()
                        array_values = []
                        i += 1  # Array 라인 건너뛰기
                        
                        # Array 값 읽기
                        while i < len(lines):
                            next_value = lines[i].strip()
                            if not next_value or next_value == 'Array':
                                break
                            if not next_value.startswith('Array'):
                                array_values.append(next_value)
                            i += 1
                            
                        self.document_data[key] = array_values
                        if array_values:
                            array_str = ', '.join(f'"{v}"' for v in array_values)
                            self.pairs_listbox.insert(tk.END, f'{key}: Array ({len(array_values)}) [{array_str}]')
                        else:
                            self.pairs_listbox.insert(tk.END, f"{key}: Array ({array_size})")
                    
                    # 직접 값이 있는 경우
                    else:
                        value = next_line
                        if not value:  # 빈 값인 경우
                            value = " "
                        self.document_data[key] = value
                        self.pairs_listbox.insert(tk.END, f'{key}: "{value}"')

            i += 1

    def on_select_pair(self, event):
        selection = self.pairs_listbox.curselection()
        if not selection:
            return

        selected_text = self.pairs_listbox.get(selection[0])
        key, value = selected_text.split(':', 1)
        key = key.strip()
        value = value.strip()

        self.key_entry.delete(0, tk.END)
        self.value_entry.delete(0, tk.END)
        self.key_entry.insert(0, key)
        self.value_entry.insert(0, value)

    def add_key_value_pair(self):
        key = self.key_entry.get().strip()
        value = self.value_entry.get().strip()

        if not key:
            messagebox.showwarning("경고", "키를 입력해주세요.")
            return

        # 기존 키가 있으면 업데이트
        if key in self.document_data:
            self.document_data[key] = value
            # 리스트박스에서 해당 항목 찾아 업데이트
            for i in range(self.pairs_listbox.size()):
                if self.pairs_listbox.get(i).startswith(key + ':'):
                    self.pairs_listbox.delete(i)
                    self.pairs_listbox.insert(i, f"{key}: {value}")
                    break
        else:
            # 새로운 키-값 쌍 추가
            self.document_data[key] = value
            self.pairs_listbox.insert(tk.END, f"{key}: {value}")

        # 입력 필드 초기화
        self.key_entry.delete(0, tk.END)
        self.value_entry.delete(0, tk.END)

    def add_document(self):
        selected_db = self.db_combo.get()
        selected_collection = self.collection_combo.get()

        if not selected_db or not selected_collection:
            messagebox.showwarning("경고", "데이터베이스와 컬렉션을 선택해주세요.")
            return

        if not self.document_data:
            messagebox.showwarning("경고", "추가할 키-값 쌍이 없습니다.")
            return

        try:
            db = self.client[selected_db]
            collection = db[selected_collection]

            # 도큐먼트 추가
            result = collection.insert_one(self.document_data)
            
            # 성공 메시지
            messagebox.showinfo("성공", f"도큐먼트가 성공적으로 추가되었습니다.\nID: {result.inserted_id}")

            # 데이터 초기화
            self.document_data = {}
            self.pairs_listbox.delete(0, tk.END)

        except Exception as e:
            error_msg = f"도큐먼트 추가 오류: {e}"
            print(error_msg)
            messagebox.showerror("오류", error_msg)

    def on_collection_selected(self, event):
        self.update_recent_document()

    def update_recent_document(self):
        # 텍스트 영역 초기화
        self.recent_doc_text.config(state='normal')
        self.recent_doc_text.delete(1.0, tk.END)

        try:
            selected_db = self.db_combo.get()
            selected_collection = self.collection_combo.get()

            if selected_db and selected_collection:
                db = self.client[selected_db]
                collection = db[selected_collection]

                # 가장 최근 도큐먼트 가져오기
                recent_doc = collection.find_one(
                    sort=[('_id', -1)]  # _id를 기준으로 내림차순 정렬
                )

                if recent_doc:
                    # ObjectId를 문자열로 변환
                    recent_doc['_id'] = str(recent_doc['_id'])
                    # JSON 형식으로 포맷팅하여 표시
                    formatted_json = json.dumps(recent_doc, indent=2, ensure_ascii=False)
                    self.recent_doc_text.insert(tk.END, formatted_json)
                else:
                    self.recent_doc_text.insert(tk.END, "컬렉션이 비어있습니다.")

        except Exception as e:
            self.recent_doc_text.insert(tk.END, f"도큐먼트 로드 오류: {e}")

        self.recent_doc_text.config(state='disabled')  # 다시 읽기 전용으로 설정

class JSONUploadDialog:
    def __init__(self, parent, mongo_client, selected_db=None):
        self.result = False
        self.parent = parent
        self.client = mongo_client
        self.file_path = ""
        
        # 다이얼로그 창 생성
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("JSON 파일 업로드")
        self.dialog.geometry("450x220")
        self.dialog.resizable(False, False)
        self.dialog.transient(parent)  # 부모 창에 종속
        self.dialog.grab_set()  # 모달 다이얼로그로 설정
        
        # 프레임 생성
        self.frame = ttk.Frame(self.dialog, padding="20")
        self.frame.pack(fill=tk.BOTH, expand=True)
        
        # 파일 경로 입력 필드 및 찾아보기 버튼
        ttk.Label(self.frame, text="JSON 파일:").grid(row=0, column=0, sticky=tk.W, pady=10)
        self.path_var = tk.StringVar()
        self.path_entry = ttk.Entry(self.frame, textvariable=self.path_var, width=30)
        self.path_entry.grid(row=0, column=1, padx=5, pady=10, sticky=tk.W+tk.E)
        
        self.browse_btn = ttk.Button(self.frame, text="찾아보기", command=self.browse_file)
        self.browse_btn.grid(row=0, column=2, padx=5, pady=10)
        
        # 데이터베이스 선택 (콤보박스)
        ttk.Label(self.frame, text="데이터베이스:").grid(row=1, column=0, sticky=tk.W, pady=10)
        self.db_var = tk.StringVar(value=selected_db if selected_db else "")
        db_list = [db for db in self.client.list_database_names() 
                  if db not in ['admin', 'local', 'config']]
        self.db_combo = ttk.Combobox(self.frame, textvariable=self.db_var, width=28, values=db_list)
        self.db_combo.grid(row=1, column=1, padx=5, pady=10, sticky=tk.W+tk.E)
        
        # 컬렉션 이름 입력 필드
        ttk.Label(self.frame, text="컬렉션 이름:").grid(row=2, column=0, sticky=tk.W, pady=10)
        self.collection_var = tk.StringVar()
        self.collection_entry = ttk.Entry(self.frame, textvariable=self.collection_var, width=30)
        self.collection_entry.grid(row=2, column=1, padx=5, pady=10, sticky=tk.W+tk.E)
        
        # 버튼 프레임
        self.btn_frame = ttk.Frame(self.frame)
        self.btn_frame.grid(row=3, column=0, columnspan=3, pady=20)
        
        # 업로드 및 취소 버튼
        self.upload_btn = ttk.Button(self.btn_frame, text="업로드", command=self.upload_data, width=10)
        self.upload_btn.pack(side=tk.LEFT, padx=10)
        
        self.cancel_btn = ttk.Button(self.btn_frame, text="취소", command=self.cancel, width=10)
        self.cancel_btn.pack(side=tk.LEFT, padx=10)
        
        # 다이얼로그가 닫힐 때까지 대기
        self.dialog.wait_window()
    
    def browse_file(self):
        file_path = filedialog.askopenfilename(
            title="업로드할 JSON 파일을 선택하세요",
            filetypes=(("JSON 파일", "*.json"), ("모든 파일", "*.*")),
            parent=self.dialog
        )
        if file_path:
            self.path_var.set(file_path)
            self.file_path = file_path
            
            # 파일 이름에서 확장자를 제외한 부분을 컬렉션 이름으로 제안
            file_name = os.path.basename(file_path)
            collection_name = os.path.splitext(file_name)[0]
            self.collection_var.set(collection_name)
    
    def upload_data(self):
        # 입력값 검증
        file_path = self.path_var.get().strip()
        db_name = self.db_var.get().strip()
        collection_name = self.collection_var.get().strip()
        
        if not file_path:
            messagebox.showwarning("경고", "JSON 파일을 선택해주세요.", parent=self.dialog)
            return
            
        if not db_name:
            messagebox.showwarning("경고", "데이터베이스를 선택해주세요.", parent=self.dialog)
            return
            
        if not collection_name:
            messagebox.showwarning("경고", "컬렉션 이름을 입력해주세요.", parent=self.dialog)
            return
            
        try:
            # 데이터베이스 선택
            db = self.client[db_name]
            
            # 컬렉션 선택
            collection = db[collection_name]
            
            # JSON 파일 읽기
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # 데이터 타입 확인 및 삽입
            if isinstance(data, list):
                # 리스트인 경우 insert_many 사용
                result = collection.insert_many(data)
                message = f"{len(result.inserted_ids)}개의 문서가 {collection_name} 컬렉션에 삽입되었습니다."
            else:
                # 딕셔너리인 경우 insert_one 사용
                result = collection.insert_one(data)
                message = f"문서가 {collection_name} 컬렉션에 삽입되었습니다."
            
            print(message)
            messagebox.showinfo("업로드 완료", message, parent=self.dialog)
            self.result = True
            self.dialog.destroy()
            
        except Exception as e:
            error_msg = f"JSON 업로드 오류: {e}"
            print(error_msg)
            messagebox.showerror("오류", error_msg, parent=self.dialog)
    
    def cancel(self):
        self.result = False
        self.dialog.destroy()

def main():
    root = tk.Tk()
    app = MongoControlGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
