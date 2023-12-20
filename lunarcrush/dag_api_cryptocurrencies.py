from airflow_llm_layer.elasticsearch_service import connect, create_or_update, check_or_create_index
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
import os
import requests
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
collection = db['lunarcrush_coin_info_v2']

# init elastic
client = connect(es_username, es_password, es_host, es_port)
index_name = 'lunarcrush_coin_info_v2'
check_or_create_index(index_name, client)


def fetch_api():
    try:
        url = f"https://lunarcrush.com/api3/storm/category/cryptocurrencies"
        headers = {'authorization': f'Bearer {token_lunar}'}

        response = requests.get(url, headers=headers)
        results = response.json()['data']
        update_requests = [
            UpdateOne(
                {"s": result["s"]},
                {"$set": result},
                upsert=True
            )
            for result in results
        ]
        collection.bulk_write(update_requests)
        create_or_update(client, index_name, 's', results)
    except Exception as e:
        os.system(
            f'python ./dags/airfow_llm_layer/utils.py --message "Request api lunarcrush errorr: {e}"')


# Định nghĩa các tham số cho DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

# Định nghĩa DAG
dag = DAG(
    'lunarcrush_api_cryptocurrencies',
    default_args=default_args,
    description='Thu thập dữ liệu coin trên lunarcrush.com theo rank',
    schedule_interval='*/15 * * * *',
    catchup=False
)

task = PythonOperator(
    task_id='lunarcrush-coin-info-v2',
    python_callable=fetch_api,
    provide_context=True,
    dag=dag,
)

task
