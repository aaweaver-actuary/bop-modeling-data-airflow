import os
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

class DbtRunOperator(BaseOperator):
    """Executes a dbt run command."""

    @apply_defaults
    def __init__(self, dbt_project_dir: str, profiles_dir: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dbt_project_dir = dbt_project_dir
        self.profiles_dir = profiles_dir

    def execute(self, context):
        command = f"dbt run --project-dir {self.dbt_project_dir} --profiles-dir {self.profiles_dir}"
        self.log.info("Executing command: %s", command)
        result = os.system(command)
        if result != 0:
            raise AirflowException(f"dbt command failed with exit code {result}")
