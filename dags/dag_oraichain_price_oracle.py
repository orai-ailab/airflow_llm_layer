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
    
    
   # get env
    MONOGO_URL = Variable.get("MONGO_URL")
    
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
