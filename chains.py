from datetime import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import pandas as pd
import pymongo
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
#Extract Phase : Crawl Data
def crawl_data():
    options = webdriver.ChromeOptions()
    options.page_load_strategy = 'none'
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)
    driver.get('https://www.coingecko.com/en/chains') 
    # take data by navigate xpath Table
    wait.until(EC.presence_of_element_located((By.XPATH,'/html/body/div[2]/main/div[2]/div/div/table')))
    result = []
    elements = driver.find_elements(By.TAG_NAME,'tr')
    for element in elements:
     result.append(element.text)
    new_result = split_text(result)
    return new_result
#Transform Phase: Preprocessing Data
def remove_text(my_list: list):
    split_list = []  # Initialize split_list here
    try:
        my_list.remove('# Chain Top Gainers 24h 7d 30d 24h Volume TVL Dominance # of Coins Last 7 Days')
        my_list = [item.replace('Buy', '') for item in my_list]
        modified_list = [element.replace('\n\n', '\n') for element in my_list]
        split_list = [element.split('\n') for element in modified_list]
        for element in split_list:
            element[-1] = element[-1].split(" ")
    except ValueError:
        pass
    return split_list if split_list else my_list  
def split_into_field(new_result:list):
    for sublist in new_result:
     inner_list = sublist[2] 
     sublist.pop(2)
     sublist.extend(inner_list)
    return new_result
#Load Phase :  Load to DataLake mongoDB
def save_to_mongo(new_result:list):
   # Tạo DataFrame từ dữ liệu mới
   text = pd.DataFrame(new_result)
   # Đặt tên cột 
   text.columns = ['chain_market_Cap_Rank', 'chain_name','Total Value Locked_24h','Total Value Locked_change_7d', 'Total Value Locked_change_30d','24h_volume', 'Total Value Locked','Dominance','of Coin','Last 7 day']
   # Thêm cột rank làm index
   text.insert(0, 'rank', text.index + 1)
   text.index.name = 'index'
   text['timestamp'] = datetime.now()
   text['Type_market_cap'] = text['chain_name']
   mongo_url = "mongodb://root:oraiA7h6BdKQTy4hfq3vuAhD@34.138.227.225:30078/"
    # Thay thế URL trực tiếp bằng biến môi trường
   client = pymongo.MongoClient(mongo_url)
   db = client["LLM_DataLake"]  
   collection = db['Coin_gecko']  
   data_json = text.to_dict(orient='index')
   record_count_before = collection.count_documents({})
   collection.insert_many([data_json[key] for key in data_json])
   # Define DAG

# Định nghĩa các hàm xử lý dữ liệu: login, extract_data, transform_data, load_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 7),
    'retries': 1,
}

with DAG('coingecko_data_processing', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    # Define tasks
    login_task = PythonOperator(
        task_id='login',
        python_callable=login,
        provide_context=True,  # Pass context to subsequent tasks
    )

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,  # Pass context to subsequent tasks
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,  # Pass context to subsequent tasks
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # Define task dependencies
    login_task >> extract_task >> transform_task >> load_task


