from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
import requests

# Thông tin xác thực Airflow (username/password)
username = 'duc'
password = 'yeuthuyen211'

# Thông tin của Airflow REST API endpoint
base_url = 'http://35.243.158.71:8080/api/v1'

# Xác thực và nhận token
auth_endpoint = f'{base_url}/login'
auth_response = requests.post(auth_endpoint, json={"username": username, "password": password})
if auth_response.status_code == 200:
    auth_token = auth_response.json()['id']
    print("Authentication successful. Token received:", auth_token)
else:
    print("Failed to authenticate.")

# Đọc nội dung của file DAG
with open('my_dag.py', 'r') as file:
    dag_content = file.read()

# Tạo payload để tạo DAG trên Airflow
dag_definition = {
    "dag_id": "my_dag",
    "file_content": dag_content
    # Các thuộc tính khác của DAG cần thiết có thể được thêm vào đây
}

headers = {
    "Authorization": f"Bearer {auth_token}",
    "Content-Type": "application/json"
}

create_dag_endpoint = f'{base_url}/dags'
create_dag_response = requests.post(create_dag_endpoint, headers=headers, json=dag_definition)

if create_dag_response.status_code == 200:
    print("DAG created successfully.")
else:
    print("Failed to create DAG:", create_dag_response.text)

# Định nghĩa các tham số cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Khởi tạo DAG
dag = DAG('my_dag', default_args=default_args, schedule_interval='@daily')

# Định nghĩa các tasks
start_task = DummyOperator(task_id='start_task', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

# Xác định flow của các tasks
start_task >> end_task
