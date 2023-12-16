import pandas as pd
import os
from datetime import datetime, timedelta

# 2020年と2021年のフォルダパス
base_folders = {
    '2020': '/Users/moritarin/Documents/学校/ソリューション開発/code/2020/在庫データ202012_202111',
    '2021': '/Users/moritarin/Documents/学校/ソリューション開発/code/2021/在庫データ202112_202211'
}

# 指定されたフォルダ内のすべてのCSVファイルを再帰的に見つける関数
def find_csv_files(folder_path):
    csv_files = []
    for subdir, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(subdir, file))
    return csv_files

# CSVファイルを読み込んで結合する関数
def read_and_combine(csv_files):
    combined_csv = pd.DataFrame()
    for file_path in csv_files:
        df = pd.read_csv(file_path, encoding='cp932')
        combined_csv = pd.concat([combined_csv, df])
    return combined_csv

# 2年分のデータを結合
combined_csv = pd.DataFrame()
for year, folder_path in base_folders.items():
    csv_files = find_csv_files(folder_path)
    combined_csv = pd.concat([combined_csv, read_and_combine(csv_files)])

# '仕入先コード'が2111のデータを抽出
filtered_data = combined_csv[combined_csv['仕入先コード'] == 2111]

# 拠点コードごとに商品コードに基づいてデータを分割して保存する関数
def save_by_product_and_base(base_code, base_name):
    base_data = filtered_data[filtered_data['拠点コード'] == base_code]
    unique_products = base_data['商品コード'].unique()
    
    for product in unique_products:
        product_data = base_data[base_data['商品コード'] == product]
        product_data = product_data.head(730)  # 最初の730行のみ
        
        # 日付列のデータを抽出
        date_column = product_data['データ作成日']
        # 整数型から文字列型に変換
        date_column = date_column.astype(str)

        min_date = datetime.strptime(date_column.min(), '%Y%m%d')
        max_date = datetime.strptime(date_column.max(), '%Y%m%d')
        
        # 欠落している日付を補完して行を追加
        current_date = min_date
        while current_date <= max_date:
            current_date_str = current_date.strftime('%Y%m%d')
            if current_date_str not in date_column.values:
                row_to_add = pd.DataFrame({
                    'データ作成日': [current_date_str],
                    '仕入先コード': [2111],
                    '拠点コード': [base_code],
                    '商品コード': [product],
                    # 他の列に必要なデータを追加
                })
                product_data = pd.concat([product_data, row_to_add])
            
            current_date += timedelta(days=1)
        
        # CSVファイルに保存
        product_data.to_csv(f'/Users/moritarin/Documents/学校/ソリューション開発/code/{base_name}_{product}.csv', index=False, encoding='utf-8')

# 茨城(801)と岩槻(1102)のデータを保存
save_by_product_and_base(801, '茨城')
save_by_product_and_base(1102, '岩槻')

