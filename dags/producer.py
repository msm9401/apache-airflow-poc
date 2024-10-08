from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator


local_file = Dataset("/tmp/sample.txt")

with DAG(
    dag_id="my_producer",
    schedule="0 0 * * *",
    start_date=datetime(2024, 10, 1),
    catchup=False,
) as dag:

    task_producer = BashOperator(
        task_id="producer1",
        bash_command='echo "hello world" >> /tmp/sample.txt',
        outlets=[local_file],
    )
