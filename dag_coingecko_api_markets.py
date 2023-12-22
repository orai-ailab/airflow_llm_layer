from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator 
from airflow import DAG





def call_api_trigger():
    import requests 
    url = "https://defi-lens.api.orai.io/update_data"
    payload = {}
    headers = {}
    response = requests.request("POST", url, headers=headers, data=payload)
    if (response.status_code == 200):
        print("Trigger Done")

def fetch_api(page, response):
    import requests
    from airflow.models import Variable
    
    x_cg_pro_api_key = Variable.get("x_cg_pro_api_key")
    print(x_cg_pro_api_key)
    url = 'https://pro-api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': '250',
        'page': str(page),
        'sparkline': 'false',
        'price_change_percentage': '24h,7d,14d,30d',
        'x_cg_pro_api_key': x_cg_pro_api_key
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
        print(res.text)


def process_data_and_save():
    import sys
    sys.path.append('/opt/airflow/dags/airflow_llm_layer/venv/lib/python3.10/site-packages')
    import pandas as pd
    import json
    import threading
    from pymongo import MongoClient, UpdateOne
    from airflow.models import Variable
    
    MONOGO_URL = Variable.get("MONGO_URL")
    
    client = MongoClient(MONOGO_URL)
    db = client["llm_database"]
    collection = db['coin_info_Gekco']
    
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
        if coin['market_cap'] is not None and coin['current_price'] is not None and coin['current_price'] > 0 and coin['market_cap'] > 0.00001 and coin['fully_diluted_valuation'] is not None and coin['fully_diluted_valuation'] > 0.0001:
            if coin['market_cap'] > 0.001 and coin['total_volume'] > 50000:
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
    coin_df_raw['symbol'] = coin_df_raw['symbol'].str.upper()
    coin_df_raw[['price_change_24h', 'price_change_percentage_24h',
                'market_cap_change_24h', 'market_cap_change_percentage_24h']].fillna(0)
    category_file = open(
        './dags/airflow_llm_layer/coingecko_coin_by_categories.json', 'r', encoding='utf-8')
    category_by_coin = json.load(category_file)
    category_file.close()
    category_by_coin_df = pd.DataFrame(category_by_coin)
    merged_df = pd.merge(
        coin_df_raw, category_by_coin_df, on='id', how='left')
    coin_by_category = merged_df.to_dict(orient='records')
    coin_by_category = list(map(lambda data: {key: None if pd.isna(
        value) else value for key, value in data.items()}, coin_by_category))
    update_requests = [
        UpdateOne(
            {"id": result["id"]},
            {"$set": result},
            upsert=True
        )
        for result in coin_by_category
    ]
    collection.bulk_write(update_requests)
    print("Done")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 20),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

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

trigger = PythonOperator(
    task_id='trigger-data',
    python_callable=call_api_trigger,
    provide_context=True,
    dag=dag,
)

task >> trigger
