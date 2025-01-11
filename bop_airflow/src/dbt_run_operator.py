import os
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from typing import Optional
from bop_airflow import Config, logger


class DbtRunOperator(BaseOperator):
    """Executes a dbt run command.

    On initialization, the operator requires either a configuration file
    or both the dbt project and profiles directories passed as arguments.
    If both are provided, the arguments take precedence over the Values
    in the configuration file.

    Parameters
    ----------
    dbt_project_dir : Optional[str]
        The directory of the dbt project.
    profiles_dir : Optional[str]
        The directory of the dbt profiles.
    config : Optional[str]
        The configuration path.
    *args, **kwargs : list, dict
        Additional arguments and keyword arguments passed to the Airflow
        BaseOperator constructor.
    """

    @apply_defaults
    def __init__(
        self,
        dbt_project_dir: Optional[str] = None,
        profiles_dir: Optional[str] = None,
        config: Optional[str] = None,
        *args,
        **kwargs,
    ):
        logger.debug("DbtRunOperator constructor")
        self._log_before_constructor(dbt_project_dir, profiles_dir, config)

        if not self._validate_inputs(dbt_project_dir, profiles_dir, config):
            err_msg = "Either a configuration file or both project and profiles directories must be provided."
            logger.error(err_msg, exc_info=True)
            raise ValueError(err_msg)

        super().__init__(*args, **kwargs)

        config = Config(config)

        self.dbt_project_dir = dbt_project_dir or config.get("dbt_project_dir")
        self.profiles_dir = profiles_dir or config.get("profiles_dir")

        self._log_after_constructor()

    def execute(self, context: dict):
        """
        Executes the dbt run command.

        Parameters
        ----------
        context : dict
            The context dictionary.
        """
        logger.debug("Executing DbtRunOperator")

        command = f"dbt run --project-dir {self.dbt_project_dir} --profiles-dir {self.profiles_dir}"
        logger.info(f"Executing command: {command}")
        result = os.system(command)
        if result != 0:
            error_msg = f"dbt command failed with exit code {result}"
            logger.error(error_msg)
            raise AirflowException(error_msg)

    def _validate_inputs(
        self,
        dbt_project_dir: Optional[str],
        profiles_dir: Optional[str],
        config: Optional[str],
    ) -> bool:
        """
        Validates that either a configuration is provided, or both dbt_project_dir and profiles_dir are provided.

        Parameters
        ----------
        dbt_project_dir : Optional[str]
            The directory of the dbt project.
        profiles_dir : Optional[str]
            The directory of the dbt profiles.
        config : Optional[str]
            The configuration path.

        Returns
        -------
        bool
            True if the inputs are valid, False otherwise.
        """
        # Ensure either config is provided or both project and profiles directories are provided
        return (config is not None) or (
            dbt_project_dir is not None and profiles_dir is not None
        )

    def _log_before_constructor(
        self,
        dbt_project_dir: Optional[str],
        profiles_dir: Optional[str],
        config: Optional[str],
    ):
        logger.debug("Creating DbtRunOperator")
        logger.debug(f"dbt_project_dir: {dbt_project_dir}")
        logger.debug(f"profiles_dir: {profiles_dir}")
        logger.debug(f"config: {config}")

    def _log_after_constructor(self):
        logger.debug(f"dbt_project_dir: {self.dbt_project_dir}")
        logger.debug(f"profiles_dir: {self.profiles_dir}")
        logger.debug("DbtRunOperator created")
