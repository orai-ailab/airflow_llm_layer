from airfow_git.lunarcrush.elasticsearch_service import connect, create_or_update, check_or_create_index
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from time import sleep
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
import json
import os
import requests
import concurrent.futures
load_dotenv()

# get env
token_lunar = os.environ.get("TOKEN_LUNAR")
es_username = os.environ.get('ES_NAME')
es_password = os.environ.get('ES_PASSWORD')
es_host = os.environ.get('ES_HOST')
es_port = os.environ.get("ES_PORT")
uri = os.environ.get("MONGO_URL")

# init mongo
client_mongo = MongoClient(uri)
db = client_mongo['LLM_database']
collection = db['lunarcrush_coin_info']

# init elastic
client = connect(es_username, es_password, es_host, es_port)
index_name = 'lunarcrush-coin-info'
check_or_create_index(index_name, client)


def process_data(data_array, **kwargs):
    def fetch_time_series(i):
        try:
            url = f"https://lunarcrush.com/api3/coins/{i['id']}/time-series"
            headers = {'authorization': f'Bearer {token_lunar}'}

            response = requests.get(url, headers=headers)
            data = response.json()

            if 'timeSeries' in data:
                time_series = data['timeSeries']
                if time_series:
                    count = len(time_series)
                    item = time_series[count - 1]
                    item.update({
                        "name": i['name'],
                        "symbol": i['symbol']
                    })
                    return item
        except Exception as e:
            os.system(
                f'python ./dags/airfow_git/utils.py --message "Request api lunarcrush errorr: {e}"')

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_time_series, i) for i in data_array]
        results = [future.result()
                   for future in concurrent.futures.as_completed(futures)]
        update_requests = [
            UpdateOne(
                {"asset_id": result["asset_id"]},
                {"$set": result},
                upsert=True
            )
            for result in results
        ]
        collection.bulk_write(update_requests)
        create_or_update(client, index_name, 'asset_id', results)
    sleep(20)


# Định nghĩa các tham số cho DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# Định nghĩa DAG
dag = DAG(
    'lunarcrush_crawl_coin_rank',
    default_args=default_args,
    description='Thu thập dữ liệu coin trên lunarcrush.com theo rank',
    schedule_interval='@hourly',
)

with open('./dags/airfow_git/lunarcrush/coins.json', 'r', encoding='utf-8') as readFile:
    data = json.load(readFile)
    total_items = len(data)
    items_per_iteration = 200
    full_iterations = total_items // items_per_iteration
    remaining_items = total_items % items_per_iteration
    iteration_count = full_iterations + \
        (1 if remaining_items > 0 else 0)

    arr_items = []
    for iteration in range(iteration_count):
        start_index = iteration * items_per_iteration
        end_index = min((iteration + 1) *
                        items_per_iteration, total_items)

        items = data[start_index:end_index]
        arr_items.append(items)

    for index, i in enumerate(arr_items):
        task_id = f'task_{index}'
        task = PythonOperator(
            task_id=task_id,
            python_callable=process_data,
            provide_context=True,
            op_kwargs={'data_array': i},
            dag=dag,
        )

        if index > 1:
            prev_task_id = f'task_{index-1}'
            dag.get_task(task_id).set_upstream(dag.get_task(prev_task_id))
