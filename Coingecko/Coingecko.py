from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta
import json
import threading
import os
import requests
from dotenv import load_dotenv
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
load_dotenv()

api_key = os.environ.get("CENTIC_API_KEY")
uri = os.environ.get("MONGO_URL")

client = MongoClient(uri)
db = client["LLM_database"]
collection = db['coin_info_Gekco']


def fetch_api(page, response):
    url = 'https://pro-api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': '250',
        'page': str(page),
        'sparkline': 'false',
        'price_change_percentage': '24h,7d,14d,30d',
        'x_cg_pro_api_key': api_key
    }
    res = requests.get(url, params=params)

    if res.status_code == 200:
        coin_data = res.json()
        for item in coin_data:
            if 'fully_diluted_valuation' in item and item['fully_diluted_valuation'] is not None:
                item['fully_diluted_valuation'] /= 1000000000
            if 'market_cap' in item and item['market_cap'] is not None:
                item['market_cap'] /= 1000000000
        response.extend(coin_data)
    else:
        print(
            f"Failed to crawled data for page {page}. Status code: {res.status_code}")


def process_data_and_save():
    try:
        response = []
        threads = []
        for page in range(1, 46):
            thread = threading.Thread(target=fetch_api, args=(page, response))
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        data = []
        for coin in response:
            if coin['market_cap'] is not None and coin['market_cap'] > 0.001 and coin['total_volume'] > 50000:
                coin['is_verify'] = 1
                data.append(coin)
                continue
            coin['is_verify'] = 0
            data.append(coin)

        coin_df_raw = pd.DataFrame(data)
        coin_df_raw = coin_df_raw.rename(columns={'price_change_percentage_14d_in_currency': 'price_change_percentage_14d',
                                                  'price_change_percentage_30d_in_currency': 'price_change_percentage_30d',
                                                  'price_change_percentage_7d_in_currency': 'price_change_percentage_7d',
                                                  'high_24h': 'highest_price_24h',
                                                  'low_24h': 'lowest_price_24h'})
        coin_df_raw['market_cap_dominant'] = coin_df_raw['market_cap'] / \
            (coin_df_raw['market_cap'].sum())
        coin_df_raw['updated_at'] = datetime.now()
        coin_df_raw.drop(['price_change_percentage_24h_in_currency',
                         'roi', 'max_supply'], axis=1, inplace=True)
        coin_df_raw['market_cap_rank'].replace({'NaN': None}, inplace=True)
        coin_df_raw['symbol'] = coin_df_raw['symbol'].str.upper()
        coin_df_raw[['price_change_24h', 'price_change_percentage_24h',
                     'market_cap_change_24h', 'market_cap_change_percentage_24h']].fillna(0)
        category_file = open(
            './dags/airfow_git/Coingecko/coin_by_categories.json', 'r', encoding='utf-8')
        category_by_coin = json.load(category_file)
        category_file.close()
        category_by_coin_df = pd.DataFrame(category_by_coin)
        merged_df = pd.merge(
            coin_df_raw, category_by_coin_df, on='id', how='left')
        coin_by_category = merged_df.to_dict(orient='records')
        update_requests = [
            UpdateOne(
                {"id": result["id"]},
                {"$set": result},
                upsert=True
            )
            for result in coin_by_category
        ]
        collection.bulk_write(update_requests)
    except Exception as e:
        print(f"Error in crawl_and_remove_trash function: {e}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Định nghĩa DAG
dag = DAG(
    'coingecko-api-crawler',
    default_args=default_args,
    description='Coingecko fetch api',
    schedule_interval='@hourly',
    catchup=False
)

task = PythonOperator(
    task_id='coingecko-api-crawler',
    python_callable=process_data_and_save,
    provide_context=True,
    dag=dag,
)

task
