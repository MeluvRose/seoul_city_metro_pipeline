from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "seoul_city_postgres"


@dag(
    dag_id="check_postgres_connection",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["learning", "postgres", "seoul-city"],
)
def check_postgres_connection():
    @task
    def run_db_health_check():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        result = hook.get_first(
            """
            SELECT
                current_database() AS database_name,
                current_user AS database_user,
                now() AS checked_at;
            """
        )

        print(
            "PostgreSQL 연결 성공 | "
            f"database={result[0]}, "
            f"user={result[1]}, "
            f"checked_at={result[2]}"
        )

    run_db_health_check()


check_postgres_connection()