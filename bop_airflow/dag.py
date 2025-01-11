from airflow import DAG
from datetime import datetime
from bop_airflow import DbtRunOperator, Config, logger
from typing import Optional


def create_dbt_snowflake_dag(config_path: str) -> DAG:
    """Factory function to create an Airflow DAG for running a dbt project on Snowflake.

    Parameters
    ----------
    config_path : str
        The path to the configuration file.

    Returns
    -------
    airflow.DAG
        The constructed Airflow DAG.
    """
    logger.debug("Creating dbt Snowflake DAG")
    config = Config(config_path)
    logger.debug(f"Config loaded: {config}")

    start_date = config.get("start_date", "2023-01-01")
    dag_id = config.get("dag_id", "dbt_snowflake_dag")
    schedule = config.get("schedule", "@daily")
    task_id = config.get("task_id", "run_dbt")

    _log_config_vars(
        start_date=start_date, dag_id=dag_id, schedule=schedule, task_id=task_id
    )

    if not config.get("dbt_project_dir"):
        raise ValueError("dbt_project_dir is a required configuration parameter")
    dbt_project_dir = config.get("dbt_project_dir")
    _log_config_vars(dbt_project_dir=dbt_project_dir)

    if not config.get("profiles_dir"):
        raise ValueError("profiles_dir is a required configuration parameter")
    profiles_dir = config.get("profiles_dir")
    _log_config_vars(profiles_dir=profiles_dir)

    default_args = {
        "owner": config.get("owner", "airflow"),
        "start_date": datetime.strptime(start_date, "%Y-%m-%d"),
        "retries": config.get("retries", 1),
    }
    _log_config_vars(**default_args)

    with DAG(dag_id, default_args=default_args, schedule_interval=schedule) as dag:
        logger.debug(f"Inside DAG context manager for {dag_id}, {task_id}")
        DbtRunOperator(
            task_id=task_id,
            dbt_project_dir=dbt_project_dir,
            profiles_dir=profiles_dir,
        )
        return dag


def _log_config_vars(kwargs: Optional[dict] = None) -> None:
    for key, value in kwargs.items():
        logger.debug(f"Config variable: {key} = {value}")


# Make the DAG discoverable by Airflow
dbt_snowflake_dag = create_dbt_snowflake_dag()
