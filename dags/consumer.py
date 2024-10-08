from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator


local_file = Dataset("/tmp/sample.txt")


with DAG(
    dag_id="my_consumer",
    schedule=[local_file],
    start_date=datetime(2024, 10, 1),
    catchup=False,
) as dag:

    task_consumer = BashOperator(
        task_id="consumer1", bash_command="cat /tmp/sample.txt"
    )
