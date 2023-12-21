from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
import requests
import threading









def fetch_api(url, responses):
    response = requests.get(url)
    if response.status_code == 200:
        responses.append(response.json())


def fetch_oracle_price():
   
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
        print(responses)
        return responses


# Định nghĩa các tham số cho DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}

dag = DAG(
    dag_id='oracle_price_crawl',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    tags=['oracle_price_crawl'],
)

task = PythonOperator(
    task_id='oracle_price_crawl',
    python_callable=fetch_oracle_price,
    dag=dag,
)

task
