from pymongo import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime, timedelta
import json
from datetime import timezone
from collections import defaultdict
import pandas as pd  # pandas 추가

# MongoDB 연결 설정
uri = "mongodb+srv://coq3820:JmbIOcaEOrvkpQo1@cluster0.qj1ty.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'), 
                    connectTimeoutMS=60000,  # 연결 타임아웃을 60초로 증가
                    socketTimeoutMS=60000)   # 소켓 타임아웃도 60초로 증가

try:
    # 연결 확인
    client.admin.command('ping')
    print("MongoDB 연결 성공!")
    
    # 데이터베이스와 컬렉션 선택
    db = client['insta09_database']
    collection = db['02_main_influencer_data']
    
    # 사용자로부터 인플루언서 username 입력 받기
    target_username = input("분석할 인플루언서의 username을 입력하세요: ")
    
    # 입력받은 username이 데이터베이스에 존재하는지 확인
    user_exists = collection.find_one({"username": target_username})
    if not user_exists:
        raise Exception(f"'{target_username}' 사용자를 찾을 수 없습니다. 올바른 username을 입력해주세요.")
    
    # 90일 전 날짜 계산 (UTC 기준)
    ninety_days_ago = datetime.now(timezone.utc) - timedelta(days=90)
    
    # 기본 쿼리로 데이터 가져오기
    user_data = collection.find_one(
        {"username": target_username},
        {"brand": 1, "clean_name": 1, "reels_views(15)": 1}
    )
    
    # clean_name 필드 출력
    user_clean_name = user_data.get('clean_name', '')
    user_reels_views = user_data.get('reels_views(15)')
    print("\nclean_name 필드:", user_clean_name)
    print("Reels Views (15):", user_reels_views if user_reels_views is not None else 'N/A')
    
    # 제외할 브랜드명 리스트
    excluded_brands = ['확인필요', 'n', 'N', 'N/A', '복합상품']
    
    # 대상 인플루언서가 진행한 브랜드 목록 수집
    target_brands = set()
    if user_data and 'brand' in user_data:
        for brand in user_data['brand']:
            brand_name = brand.get('name', '')
            clean_name = brand.get('clean_name', '')
            if (brand_name not in excluded_brands and 
                target_username not in clean_name.lower() and
                target_username not in brand_name.lower() and
                user_clean_name.lower() not in brand_name.lower()):  # 타겟 유저의 clean_name이 브랜드명에 포함되지 않은 경우만
                for product in brand.get('products', []):
                    try:
                        mentioned_date = datetime.fromisoformat(product['mentioned_date'].replace('Z', '+00:00'))
                        if mentioned_date >= ninety_days_ago:
                            target_brands.add(brand_name)
                            break
                    except ValueError:
                        continue
    
    # 결과 정리
    brand_dict = {}  # brand_name을 키로 사용하여 최신 데이터만 저장
    if user_data and 'brand' in user_data:
        for brand in user_data['brand']:
            brand_name = brand.get('name', '')
            clean_name = brand.get('clean_name', '')
            
            # 제외 조건 검사
            if (brand_name not in excluded_brands and
                target_username not in clean_name.lower() and
                target_username not in brand_name.lower() and
                user_clean_name.lower() not in brand_name.lower()):  # 타겟 유저의 clean_name이 브랜드명에 포함되지 않은 경우만
                for product in brand.get('products', []):
                    # UTC timezone을 가진 datetime으로 변환
                    mentioned_date = datetime.fromisoformat(product['mentioned_date'].replace('Z', '+00:00'))
                    if mentioned_date >= ninety_days_ago:
                        brand_info = {
                            'brand_name': brand_name,
                            'clean_name': clean_name,
                            'product_item': product['item'],
                            'product_category': product['category'],
                            'item_feed_link': product['item_feed_link'],
                            'mentioned_date': product['mentioned_date'],
                            'mentioned_date_obj': mentioned_date  # 정렬을 위해 datetime 객체 추가
                        }
                        
                        # 해당 브랜드의 기존 데이터가 없거나, 현재 데이터가 더 최신인 경우 업데이트
                        if brand_name not in brand_dict or brand_dict[brand_name]['mentioned_date_obj'] < mentioned_date:
                            brand_dict[brand_name] = brand_info
    
    # 딕셔너리를 리스트로 변환하고 mentioned_date_obj 제거
    brand_list = []
    for brand_info in brand_dict.values():
        del brand_info['mentioned_date_obj']
        brand_list.append(brand_info)
    
    # JSON 형태로 출력
    print(f"\n=== {target_username}이 진행한 브랜드 목록 ===")
    print(json.dumps(brand_list, indent=2, default=str, ensure_ascii=False))
    
    # 각 브랜드별로 진행한 다른 인플루언서 찾기
    print("\n=== 각 브랜드를 진행한 다른 인플루언서 목록 ===")
    
    # 모든 관련 인플루언서 목록 수집
    all_influencers = set()
    brand_to_influencers = defaultdict(list)  # 브랜드별 인플루언서 정보를 저장할 딕셔너리
    
    # 먼저 대상 인플루언서의 브랜드 목록 수집
    target_brands = set()
    for brand_info in brand_list:
        target_brands.add(brand_info['brand_name'])
    
    # 다른 인플루언서들의 브랜드 정보 수집
    cursor = collection.find(
        {
            "username": {"$ne": target_username},
            "brand": {
                "$elemMatch": {
                    "name": {"$in": list(target_brands)}  # 대상 인플루언서의 브랜드만 찾기
                }
            }
        }
    )
    
    for inf_data in cursor:
        username = inf_data['username']
        clean_name = inf_data.get('clean_name', '')
        reels_views = inf_data.get('reels_views(15)', 'N/A')
        
        if 'brand' in inf_data:
            for brand in inf_data['brand']:
                brand_name = brand.get('name', '')
                
                # 대상 인플루언서의 브랜드인 경우에만 처리
                if brand_name in target_brands:
                    for product in brand.get('products', []):
                        try:
                            mentioned_date = datetime.fromisoformat(product['mentioned_date'].replace('Z', '+00:00'))
                            if mentioned_date >= ninety_days_ago:
                                brand_to_influencers[brand_name].append({
                                    'username': username,
                                    'clean_name': clean_name,
                                    'product_item': product['item'],
                                    'mentioned_date': product['mentioned_date'],
                                    'item_feed_link': product.get('item_feed_link', ''),
                                    'reels_views': reels_views
                                })
                                all_influencers.add((username, clean_name))
                                break
                        except ValueError:
                            continue
    
    # 각 인플루언서별로 최근 90일 내 진행한 브랜드 출력
    print("\n=== 각 인플루언서별 최근 90일 내 진행한 브랜드 목록 ===")
    
    for username, clean_name in sorted(all_influencers):
        print(f"\n인플루언서: {username} (clean_name: {clean_name})")
        print("진행한 브랜드 목록:")
        
        # 해당 인플루언서의 브랜드 데이터 가져오기
        inf_data = collection.find_one(
            {"username": username},
            {
                "brand": 1,
                "reels_views(15)": 1
            }
        )
        
        if inf_data and 'brand' in inf_data:
            recent_brands = []
            for brand in inf_data['brand']:
                brand_name = brand.get('name', '')
                if brand_name not in excluded_brands:
                    for product in brand.get('products', []):
                        try:
                            mentioned_date = datetime.fromisoformat(product['mentioned_date'].replace('Z', '+00:00'))
                            if mentioned_date >= ninety_days_ago:
                                brand_info = {
                                    'brand_name': brand_name,
                                    'product_item': product['item'],
                                    'mentioned_date': product['mentioned_date']
                                }
                                recent_brands.append(brand_info)
                                # 브랜드별 인플루언서 정보 저장 (대상 인플루언서가 진행하지 않은 브랜드만)
                                if brand_name not in target_brands:
                                    # 인플루언서의 reels_views 가져오기
                                    reels_views = inf_data.get('reels_views(15)')
                                    brand_to_influencers[brand_name].append({
                                        'username': username,
                                        'clean_name': clean_name,
                                        'mentioned_date': product['mentioned_date'],
                                        'product_item': product['item'],
                                        'item_feed_link': product.get('item_feed_link', ''),
                                        'reels_views': reels_views if reels_views is not None else 'N/A'
                                    })
                                break  # 각 브랜드당 최신 제품 하나만 포함
                        except ValueError:
                            continue
            
            if recent_brands:
                for brand in recent_brands:
                    print(f"- {brand['brand_name']}")
                    print(f"  제품: {brand['product_item']}")
                    print(f"  언급일자: {brand['mentioned_date']}")
            else:
                print("- 최근 90일 내 진행한 브랜드가 없습니다.")
        else:
            print("- 브랜드 데이터가 없습니다.")
    
    # 브랜드별 인플루언서 목록 출력 (대상 인플루언서 제외)
    print(f"\n=== 브랜드별 진행 인플루언서 종합 정리 ({target_username} 제외) ===")
    
    # Excel 데이터를 위한 리스트 생성
    excel_data = []
    
    for brand_name, influencers in sorted(brand_to_influencers.items()):
        for inf in sorted(influencers, key=lambda x: x['mentioned_date'], reverse=True):
            # Excel 데이터에 추가
            excel_data.append({
                '브랜드명': brand_name,
                '인플루언서': inf['username'],
                'Clean Name': inf['clean_name'],
                '제품': inf['product_item'],
                '언급일자': inf['mentioned_date'],
                '링크': inf['item_feed_link'],
                'Reels Views (15)': inf['reels_views']
            })
    
    # DataFrame 생성 및 Excel 파일로 저장
    if excel_data:
        df_others = pd.DataFrame(excel_data)
        # 칼럼 순서 재정렬
        df_others = df_others[['브랜드명', '제품', 'Clean Name', '인플루언서', '언급일자', '링크', 'Reels Views (15)']]
        # 언급일자 기준으로 최신순 정렬
        df_others = df_others.sort_values('언급일자', ascending=False)
        
        # 대상 인플루언서의 브랜드 데이터를 위한 리스트 생성
        target_data = []
        target_brands_dict = {}  # 브랜드별 상세 정보를 저장하기 위한 딕셔너리
        
        if user_data and 'brand' in user_data:
            for brand in user_data['brand']:
                brand_name = brand.get('name', '')
                clean_name = brand.get('clean_name', '')
                
                # 제외 조건 검사
                if (brand_name not in excluded_brands and
                    target_username not in clean_name.lower() and
                    target_username not in brand_name.lower() and
                    user_clean_name.lower() not in brand_name.lower()):  # 타겟 유저의 clean_name이 브랜드명에 포함되지 않은 경우만
                    for product in brand.get('products', []):
                        try:
                            mentioned_date = datetime.fromisoformat(product['mentioned_date'].replace('Z', '+00:00'))
                            if mentioned_date >= ninety_days_ago:
                                brand_info = {
                                    '브랜드명': brand_name,
                                    '인플루언서': target_username,
                                    '제품': product['item'],
                                    'Clean Name': user_clean_name,
                                    '언급일자': product['mentioned_date'],
                                    '링크': product.get('item_feed_link', ''),
                                    'Reels Views (15)': user_reels_views if user_reels_views is not None else 'N/A'
                                }
                                target_data.append(brand_info)
                                target_brands_dict[brand_name] = brand_info
                                break
                        except ValueError:
                            continue
        
        df_target = pd.DataFrame(target_data)
        # 칼럼 순서 재정렬
        df_target = df_target[['브랜드명', '제품', 'Clean Name', '인플루언서', '언급일자', '링크', 'Reels Views (15)']]
        # 언급일자 기준으로 최신순 정렬
        df_target = df_target.sort_values('언급일자', ascending=False)
        
        # 겹치는 브랜드 정보 생성
        overlapping_data = []
        target_brands = set(target_brands_dict.keys())
        print(f"\n{target_username}의 브랜드 목록:", sorted(target_brands))
        
        # brand_to_influencers 내용 출력
        print("\n=== 다른 인플루언서들이 진행한 브랜드 목록 ===")
        for brand_name, influencers in brand_to_influencers.items():
            print(f"\n브랜드: {brand_name}")
            print(f"진행한 인플루언서 수: {len(influencers)}명")
            for inf in influencers:
                print(f"- {inf['username']} (clean_name: {inf['clean_name']})")
                print(f"  제품: {inf['product_item']}")
                print(f"  언급일자: {inf['mentioned_date']}")
        
        # brand_to_influencers에서 대상 인플루언서의 브랜드와 매칭되는 정보 찾기
        print(f"\n=== {target_username} 브랜드와 매칭되는 정보 찾기 ===")
        for brand_name, influencers in brand_to_influencers.items():
            if brand_name in target_brands:
                print(f"\n브랜드 '{brand_name}' 매칭됨!")
                target_info = target_brands_dict[brand_name]
                for inf in influencers:
                    print(f"- 매칭된 인플루언서: {inf['username']}")
                    overlapping_data.append({
                        '인플루언서': inf['username'],
                        '인플루언서_Clean_Name': inf['clean_name'],
                        '진행_브랜드': brand_name,
                        '인플루언서_제품': inf['product_item'],
                        '인플루언서_언급일자': inf['mentioned_date'],
                        '인플루언서_링크': inf['item_feed_link'],
                        '인플루언서_Reels_Views': inf['reels_views'],
                        f'{target_username}_제품': target_info['제품'],
                        f'{target_username}_언급일자': target_info['언급일자'],
                        f'{target_username}_링크': target_info['링크'],
                        f'{target_username}_Reels_Views': target_info['Reels Views (15)']
                    })
        
        # DataFrame 생성 및 정렬
        df_overlapping = pd.DataFrame(overlapping_data)
        if not df_overlapping.empty:
            # 인플루언서와 브랜드로 정렬
            df_overlapping = df_overlapping.sort_values(['인플루언서', '진행_브랜드'])
            print(f"\n총 {len(set(df_overlapping['인플루언서']))}명의 인플루언서가 {target_username}의 브랜드를 진행했습니다.")
        
        # Excel 파일로 저장
        excel_filename = f'brand_influencer_summary_{target_username}.xlsx'
        with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
            if not df_target.empty:
                df_target.to_excel(writer, sheet_name='Sheet0', index=False)
            # Sheet0 칼럼 너비 설정
            worksheet = writer.sheets['Sheet0']
            for column in worksheet.columns:
                worksheet.column_dimensions[column[0].column_letter].width = 25

            if not df_others.empty:
                df_others.to_excel(writer, sheet_name='Sheet1', index=False)
            # Sheet1 칼럼 너비 설정
            worksheet = writer.sheets['Sheet1']
            for column in worksheet.columns:
                worksheet.column_dimensions[column[0].column_letter].width = 25

            if not df_overlapping.empty:
                df_overlapping.to_excel(writer, sheet_name='Sheet2', index=False)
                print(f"\nSheet2에 {len(df_overlapping)}개의 브랜드-인플루언서 매칭 정보가 저장되었습니다.")
        
        print(f"\nExcel 파일이 생성되었습니다: {excel_filename}")
        print(f"Sheet0: {target_username}의 브랜드 목록")
        print("Sheet1: 다른 인플루언서들의 브랜드 목록")
        if not df_overlapping.empty:
            print(f"Sheet2: {target_username} 브랜드를 진행한 인플루언서 목록")

except Exception as e:
    print(f"오류 발생: {e}")

finally:
    client.close() 