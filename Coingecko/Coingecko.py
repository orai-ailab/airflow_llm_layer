import pymongo
from datetime import datetime, timedelta
import os 
import requests
from dotenv import load_dotenv
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
load_dotenv()
api_key = os.environ.get("CENTIC_API_KEY")
client_url = os.environ.get("MONGO_URL")
def crawl_coingecko_data():
    all_coin_data = [] 
    for page in range(1, 46):
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
        response = requests.get(url, params=params)

        if response.status_code == 200:
            coin_data = response.json()
            for item in coin_data:
                if 'fully_diluted_valuation' in item and item['fully_diluted_valuation'] is not None:
                    item['fully_diluted_valuation'] /= 1000000000  # fix len
            all_coin_data.extend(coin_data) 
            print(f"Page {page} have crawled sucessfull.")
        else:
            print(f"Failed to crawled data for page {page}. Status code: {response.status_code}")
    return all_coin_data 
def remove_trash_coin(coins):
    # Xác định index của các đồng tiền cần loại bỏ
    indexes_to_drop = []
    for idx, record in enumerate(coins):
        if record['market_cap'] is None or record['market_cap'] == 0 or record['market_cap'] < 1000000 or record['total_volume'] < 1000000:
            indexes_to_drop.append(idx)
    # Loại bỏ các đồng tiền khỏi danh sách dựa trên index đã xác định
    for idx in reversed(indexes_to_drop):
        del coins[idx]
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}
#Task1 : crawl data-> loại bỏ coin rác
def crawl_and_remove_trash():
    try:
        allcoin_info = crawl_coingecko_data()
        remove_trash_coin(allcoin_info)
        print("Remove trash coin succes")
        coin_dataframe = pd.DataFrame(allcoin_info)
        coin_dataframe.rename(columns={'price_change_percentage_14d_in_currency':'price_change_percentage_14d',
                                       'price_change_percentage_24h_in_currency':'price_change_percentage_24h',
                                       'price_change_percentage_30d_in_currency':'price_change_percentage_30d',
                                       'price_change_percentage_7d_in_currency':'price_change_percentage_7d',
                                       'high_24h':'highest_price_24h',
                                       'low_24h':'lowest_price_24h'}, inplace=True)
        coin_dataframe[['price_change_24h','price_change_percentage_24h','market_cap_change_24h','market_cap_change_percentage_24h']].fillna(0)
        return coin_dataframe
    except Exception as e:
        print(f"Error in crawl_and_remove_trash function: {e}")

#coin_dataframe.drop(columns=['roi','max_supply'], inplace=True)

#Task2  crawl 
def crawl_and_map_categories(coin_dataframe):
    try:
        list_coin_id = coin_dataframe["id"].tolist()
        coin_category = pd.read_csv("coin_category.csv")
        merged_dataframe = pd.merge(coin_dataframe, coin_category[['id', 'categories']], on='id', how='left')
        merged_dataframe = merged_dataframe.rename(columns={'categories_x': 'coin_categories', 'categories_y': 'a_categories'})
        merged_dataframe['symbol'] = merged_dataframe['symbol'].str.upper() 
        merged_dataframe[['price_change_24h','price_change_percentage_24h','market_cap_change_24h','market_cap_change_percentage_24h']].fillna(0, inplace=True)
        return merged_dataframe
    except Exception as e:
        print(f"Error in crawl_and_map_categories function: {e}")


#Task3 insert into MongoDB, 
def insert_to_mongodb(merged_dataframe):
    try:
        data_dict = merged_dataframe.to_dict(orient='records')
        client = pymongo.MongoClient(client_url)
        db = client["LLM_Datawarehouse"]
        collection = db['Coingecko_info']
        collection.delete_many({})          
        collection.insert_many(data_dict)
    except Exception as e:
        print(f"Error in insert_to_mongodb function: {e}")

dag = DAG('crawl_coingecko_data', default_args=default_args, schedule_interval=timedelta(hours=1))

task1 = PythonOperator(
    task_id='crawl_and_remove_trash',
    python_callable=crawl_and_remove_trash,
    dag=dag,
)
task2 = PythonOperator(
    task_id='crawl_and_map_categories',
    python_callable=crawl_and_map_categories,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id='insert_to_mongodb',
    python_callable=insert_to_mongodb,
    provide_context=True,
    dag=dag,
)
task1 >> task2 >> task3
