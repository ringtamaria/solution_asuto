import pandas as pd
import numpy as np
import os
from datetime import timedelta

# Define the function to find all CSV files in the specified directory
def find_csv_files(directory):
    csv_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return csv_files

# Define the function to read and combine CSV files
def read_and_combine(csv_files):
    combined_data = pd.DataFrame()
    for csv_file in csv_files:
        df = pd.read_csv(csv_file, encoding='cp932')
        combined_data = pd.concat([combined_data, df], ignore_index=True)
    return combined_data

# Define the function to process and save data for each product and base code
def process_and_save_data(data, product_code, base_code, base_name):
    product_data = data[(data['商品コード'] == product_code) & (data['拠点コード'] == base_code)]
    product_data['データ作成日'] = pd.to_datetime(product_data['データ作成日'])
    
    # Find the range of dates
    min_date = product_data['データ作成日'].min()
    max_date = product_data['データ作成日'].max()
    
    # Create a full range of dates from min to max
    all_dates = pd.date_range(start=min_date, end=max_date)
    
    # Make sure the DataFrame has all the dates in the range
    product_data.set_index('データ作成日', inplace=True)
    product_data = product_data.reindex(all_dates).reset_index()
    product_data.rename(columns={'index': 'データ作成日'}, inplace=True)
    
    # If there are fewer than 730 rows, add the missing rows
    if len(product_data) < 730:
        missing_rows = 730 - len(product_data)
        additional_dates = pd.date_range(start=max_date + timedelta(days=1), periods=missing_rows)
        additional_data = pd.DataFrame(additional_dates, columns=['データ作成日'])
        product_data = pd.concat([product_data, additional_data], ignore_index=True)
    
    # Save the DataFrame to a CSV file
    filename = f'{base_name}_{product_code}.csv'
    product_data.to_csv(filename, index=False, encoding='utf-8')

# Define the base folder paths for 2020 and 2021
base_folders = {
    '2020': '/Users/moritarin/Documents/学校/ソリューション開発/code/2020/在庫データ202012_202111',
    '2021': '/Users/moritarin/Documents/学校/ソリューション開発/code/2021/在庫データ202112_202211'
}

# Combine data from both years
combined_data = pd.DataFrame()
for year, folder in base_folders.items():
    csv_files = find_csv_files(folder)
    year_data = read_and_combine(csv_files)
    combined_data = pd.concat([combined_data, year_data], ignore_index=True)

# Filter the combined data by '仕入先コード'
filtered_data = combined_data[combined_data['仕入先コード'] == 2111]

# Process and save data for each '商品コード' and '拠点コード'
for base_code in filtered_data['拠点コード'].unique():
    base_name = '茨城' if base_code == 801 else '岩槻'
    for product_code in filtered_data[filtered_data['拠点コード'] == base_code]['商品コード'].unique():
        process_and_save_data(filtered_data, product_code, base_code, base_name)

# Return a message when all files are processed
"All CSV files have been processed and saved."
