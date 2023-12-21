# tạo 1 dags chạy 1 lần install các thư viện cần thiết cho các dags khác

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 4, 1),
}

dag = DAG(
    dag_id='install',
    default_args=default_args,
    schedule_interval=None,
    tags=['install'],
)

def install():
    
    print('Hello world')
    
   
        
    
    
    
    
install = PythonOperator(
    task_id='install',
    python_callable=install,
    dag=dag,
)

install
    