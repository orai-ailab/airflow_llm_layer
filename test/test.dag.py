from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airfow_git.lunarcrush.elasticsearch_service import connect, create_or_update, check_or_create_index


from datetime import datetime
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 4, 1),
}

dag = DAG(
    dag_id='test',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['test']
)


def test():
    print(1)


t1 = PythonOperator(
    task_id='test',
    python_callable=test,
    dag=dag
)

t1
