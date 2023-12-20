from airfow_llm_layer.elasticsearch_service import connect, create_or_update, check_or_create_index
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pymongo import MongoClient
import os
import requests
import threading

load_dotenv()

# get env
es_username = os.environ.get('ES_NAME')
es_password = os.environ.get('ES_PASSWORD')
es_host = os.environ.get('ES_HOST')
es_port = os.environ.get("ES_PORT")
uri = os.environ.get("MONGO_URL")

# init mongo
client_mongo = MongoClient(uri)
db = client_mongo['oraichain-transaction']
collection = db['oracle-price']

# init elastic
client = connect(es_username, es_password, es_host, es_port)
index_name = 'oraichain-oracle-price'
check_or_create_index(index_name, client)


def fetch_api(url, responses):
    response = requests.get(url)
    if response.status_code == 200:
        responses.append(response.json())


def fetch_oracle_price():
    try:
        orai_fetch_url = "https://pricefeed.oraichainlabs.org/"
        inj_fetch_url = "https://pricefeed-futures.oraichainlabs.org/inj"

        # List of API endpoints
        api_urls = [
            orai_fetch_url,
            inj_fetch_url,
        ]

        # Create and start a thread for each API request
        responses = []
        threads = []
        for url in api_urls:
            thread = threading.Thread(target=fetch_api, args=(url, responses))
            thread.start()
            threads.append(thread)

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        insert_many(client, index_name, responses)
        collection.insert_many(responses)

    except Exception as e:
        os.system(
            f'python ./dags/airfow_llm_layer/utils.py --message "Request api oracle price errorr: {e}"')


# Định nghĩa các tham số cho DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

# Định nghĩa DAG
dag = DAG(
    'oraichain-price-oracle',
    default_args=default_args,
    description='Thu thập dữ liệu price oracle mỗi 15 giây',
    schedule_interval=timedelta(seconds=15),
    catchup=False
)

task = PythonOperator(
    task_id='oraichain-price-oracle',
    python_callable=fetch_oracle_price,
    provide_context=True,
    dag=dag,
)

task
