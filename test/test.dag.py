from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airfow_git.lunarcrush.elasticsearch_test import connect, create_or_update, check_or_create_index
from dotenv import load_dotenv
import os
load_dotenv()

es_username = os.environ.get('ES_NAME')
es_password = os.environ.get('ES_PASSWORD')
es_host = os.environ.get('ES_HOST')
es_port = os.environ.get("ES_PORT")

# init elastic
client = connect(es_username, es_password, es_host, es_port)
index_name = 'lunarcrush-coin-info'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 4, 1),
}

dag = DAG(
    dag_id='test_eslasticsearch',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['test']
)


def test():
    check_or_create_index(index_name, client)
    print("Done")


t1 = PythonOperator(
    task_id='test',
    python_callable=test,
    dag=dag
)

t1
