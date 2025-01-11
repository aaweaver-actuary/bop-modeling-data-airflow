from airflow import DAG
from datetime import datetime
from bop_airflow import DbtRunOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG('dbt_snowflake_dag', default_args=default_args, schedule_interval='@daily') as dag:
    run_dbt = DbtRunOperator(
        task_id='run_dbt',
        dbt_project_dir='/path/to/your/dbt/project',
        profiles_dir='/path/to/your/dbt/profiles',
    )
