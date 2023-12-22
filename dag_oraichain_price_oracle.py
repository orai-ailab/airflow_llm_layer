from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
import threading




def fetch_api(url, responses):
    import requests
    response = requests.get(url)
    if response.status_code == 200:
        responses.append(response.json())




        
        

def fetch_oracle_price():
    import sys
    sys.path.append('/opt/airflow/dags/airflow_llm_layer/venv/lib/python3.10/site-packages')
    from pymongo import MongoClient
    from airflow.models import Variable
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk
    
    def connect(es_username, es_password, es_host, es_port):
        client = Elasticsearch("{}:{}/".format(es_host, es_port),
                            http_auth=(es_username, es_password))
        return client


    def check_or_create_index(index, client):
        if not client.indices.exists(index=index):
            client.indices.create(index=index)


    def insert_many(client, index, datas):
        print("🚀 ~ file: elasticsearch_service.py:49 ~ datas:", datas)
        insert_actions = [
            {
                # "_op_type": "insert",
                "_index": index,
                # "_id":  uuid.uuid4(),
                "_source": item
            }
            for item in datas
        ]
        try:
            success, failed = bulk(client, insert_actions,
                                index=index, raise_on_error=True)
            print(f"Successfully updated or inserted {success} documents.")
            if failed:
                print(f"Failed to update or insert {failed} documents.")

        except Exception as e:
            print(f"Error updating or inserting documents: {e}")
    
   # get env
    ES_USERNAME = Variable.get('ES_NAME')
    ES_PASSWORD = Variable.get('ES_PASSWORD')
    ES_HOST = Variable.get('ES_HOST')
    ES_PORT = Variable.get("ES_PORT")
    MONOGO_URL = Variable.get("MONGO_URL")
    
    # init elastic
    client = connect(ES_USERNAME, ES_PASSWORD, ES_HOST, ES_PORT)
    index_name = 'oraichain-oracle-price'
    check_or_create_index(index_name, client)
    
    # init mongo
    client_mongo = MongoClient(MONOGO_URL)
    db = client_mongo['oraichain-transaction']
    collection = db['oracle-price']


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
    print(responses)

    


# Định nghĩa các tham số cho DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 19),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# Định nghĩa DAG
dag = DAG(
    'oraichain-price-oracle',
    default_args=default_args,
    description='Thu thập dữ liệu price oracle mỗi 15 giây',
    schedule_interval='*/1 * * * *',
    catchup=False
)

task = PythonOperator(
    task_id='oraichain-price-oracle',
    python_callable=fetch_oracle_price,
    provide_context=True,
    dag=dag,
)

task
