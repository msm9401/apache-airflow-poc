from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "hello_world_dag_every_minute",
    default_args=default_args,
    description="A simple hello world DAG running every minute",
    schedule_interval=timedelta(minutes=1),
    catchup=False,
) as dag:

    def print_hello():
        print("Hello from Airflow every minute!")

    def print_task():
        print("I'm just Task 2")

    task1 = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )

    task2 = PythonOperator(
        task_id="print_task",
        python_callable=print_task,
    )

    task1 >> task2