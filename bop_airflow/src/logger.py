import logging
from logging import Formatter, FileHandler, getLogger
from typing import Optional, Union


class Logger:
    def __init__(self, log_file: Optional[str] = None):
        self._logger = getLogger("bop_airflow_logger")
        self._level = logging.INFO
        self._file = log_file
        self._update_file_handler()
        self._update_level()

    @property
    def logger(self):
        """The logger property."""
        return self._logger

    def _update_level(self):
        self.logger.setLevel(self._level)
        for handler in self.logger.handlers:
            handler.setLevel(self._level)

    def _update_file_handler(self):
        if self._file is not None:
            file_handler = FileHandler(self._file)
            file_handler.setFormatter(
                Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            )
            self.logger.addHandler(file_handler)

    def error(self, msg: str):
        self.logger.error(msg)

    def debug(self, msg: str):
        self.logger.debug(msg)

    def warning(self, msg: str):
        self.logger.warning(msg)

    def critical(self, msg: str):
        self.logger.critical(msg)

    def info(self, msg: str):
        self.logger.info(msg)

    def log(self, msg: str):
        self.info(msg)

    def __call__(self, msg: str):
        self.info(msg)

    @property
    def file(self):
        """The file property."""
        return self._file

    @file.setter
    def file(self, value):
        self._file = value

    @property
    def level(self) -> int:
        """The level property."""
        return self._level

    @level.setter
    def level(self, value: Union[str, int]):
        """Set the level property."""
        if isinstance(value, str):
            self._level = self._handle_level_str(value)
        elif isinstance(value, int):
            self._level = self._handle_level_int(value)
        else:
            self.warning(f"Invalid log level: {value}. Using INFO.")
            self._level = logging.INFO

        self._update_level()

    def _handle_level_str(self, value: str) -> int:
        """Handle a string level, returning the corresponding enum value."""
        match value.lower():
            case "debug":
                return logging.DEBUG
            case "d":
                return logging.DEBUG
            case "info":
                return logging.INFO
            case "i":
                return logging.INFO
            case "warning":
                return logging.WARNING
            case "w":
                return logging.WARNING
            case "error":
                return logging.ERROR
            case "e":
                return logging.ERROR
            case "critical":
                return logging.CRITICAL
            case "c":
                return logging.CRITICAL
            case _:
                self.warning(f"Invalid log level: {value}. Using INFO.")
                return logging.INFO

    def _handle_level_int(self, value: int) -> int:
        """Handle an integer level, returning the corresponding enum value."""
        if value < logging.DEBUG or value > logging.CRITICAL:
            self.warning(f"Invalid log level: {value}. Using INFO.")
            return logging.INFO

        match value:
            case logging.DEBUG:
                return logging.DEBUG
            case logging.INFO:
                return logging.INFO
            case logging.WARNING:
                return logging.WARNING
            case logging.ERROR:
                return logging.ERROR
            case logging.CRITICAL:
                return logging.CRITICAL


logger = Logger()
