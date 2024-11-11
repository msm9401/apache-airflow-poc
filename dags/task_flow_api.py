from airflow.decorators import dag, task
from datetime import datetime


# xcom 의 push, pull 없이 데코레이터로 구현 가능
@dag(
    dag_id="etl_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 11, 1),
    catch_up=False,
)
def etl_pipeline():
    @task
    def extract():
        """Simlulate data extraction"""
        data = {"orders": [100, 200, 300]}
        print("Data Extracted")
        return data

    @task
    def transform(data):
        """Simlulate data transformation"""
        transformed_data = {"total_orderes": sum(data["orders"])}
        print("Data transformed")
        return transformed_data

    @task
    def load(data):
        """Simlulate loading data to destination"""
        print(f"Data loaded: {data}")

    # Define Task Dependencies
    raw_data = extract()
    processed_data = transform(raw_data)
    load(processed_data)


# 데코레이터로 TaskFlow를 만들면 Dag도 인스턴스화
etl_dag = etl_pipeline()
