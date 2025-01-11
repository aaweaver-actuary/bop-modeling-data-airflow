import os
import yaml
from typing import Any, Dict


class Config:
    """
    A class to parse and validate YAML configuration files for the project.

    Attributes
    ----------
    config_file : str
        Path to the YAML configuration file.
    config : Dict[str, Any]
        Parsed configuration data.
    """

    def __init__(self, config_file: str):
        """
        Initializes the ConfigParser with the provided YAML file.

        Parameters
        ----------
        config_file : str
            Path to the YAML configuration file.
        """
        self.config_file = config_file
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """
        Loads the YAML configuration file.

        Returns
        -------
        Dict[str, Any]
            The parsed configuration data.

        Raises
        ------
        FileNotFoundError
            If the configuration file does not exist.
        yaml.YAMLError
            If there is an error parsing the YAML file.
        """
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(
                f"Configuration file '{self.config_file}' not found."
            )

        with open(self.config_file, "r") as file:
            try:
                return yaml.safe_load(file) or {}
            except yaml.YAMLError as e:
                raise ValueError(f"Error parsing YAML file: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieves a value from the configuration.

        Parameters
        ----------
        key : str
            The key to retrieve from the configuration.
        default : Any
            The default value to return if the key is not found.

        Returns
        -------
        Any
            The value from the configuration, or the default if not found.
        """
        return self.config.get(key, default)

    def validate_keys(self, required_keys: list):
        """
        Validates that the required keys are present in the configuration.

        Parameters
        ----------
        required_keys : list
            A list of keys that must be present in the configuration.

        Raises
        ------
        KeyError
            If any of the required keys are missing.
        """
        missing_keys = [key for key in required_keys if key not in self.config]
        if missing_keys:
            raise KeyError(f"Missing required configuration keys: {missing_keys}")

    def __call__(self, key: str, default: Any = None) -> Any:
        """
        Retrieves a value from the configuration using the callable syntax.

        Parameters
        ----------
        key : str
            The key to retrieve from the configuration.
        default : Any
            The default value to return if the key is not found.

        Returns
        -------
        Any
            The value from the configuration, or the default if not found.
        """
        return self.get(key, default)
