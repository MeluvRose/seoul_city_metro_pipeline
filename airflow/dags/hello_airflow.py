from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="hello_airflow",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["learning", "hello"],
)
def hello_airflow():
    @task
    def say_hello():
        print("Airflow DAG 실행 성공")

    say_hello()


hello_airflow()