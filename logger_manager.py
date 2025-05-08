# logger_manager.py

import sys
import logging
from logging.handlers import RotatingFileHandler

class LoggerManager:
    """
    Encapsulates logging configuration.

    Attributes:
        logger_name (str): Name of the logger.
        log_level (int): Global log level for the logger.
        log_file (str): File name for the file handler (default is "<logger_name>.log").
        max_bytes (int): Maximum size (in bytes) of the log file before rotating.
        backup_count (int): Number of rotated log files to keep.
        use_stderr (bool): Whether to add an additional handler that sends ERROR-level logs to sys.stderr.
    """
    def __init__(self,
                 logger_name: str = "custom_script",
                 log_level: int = logging.INFO,
                 log_file: str = None,
                 max_bytes: int = 2_000_000,
                 backup_count: int = 3,
                 use_stderr: bool = True):
        self.logger_name = logger_name
        self.log_level = log_level
        self.log_file = log_file or f"{logger_name}.log"
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.use_stderr = use_stderr
        self.logger = logging.getLogger(self.logger_name)
        self._setup_logger()

    def _setup_logger(self) -> None:
        # Clear any existing handlers to avoid duplications.
        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        # Set logger level
        self.logger.setLevel(self.log_level)

        log_format = '%(asctime)s [%(levelname)s] [%(name)s] (%(module)s:%(lineno)d, %(funcName)s): %(message)s'
        date_format = '%Y-%m-%d %H:%M:%S'
        formatter = logging.Formatter(log_format, datefmt=date_format)

        # Create a console handler for general logs (to stdout)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # Optionally add a separate error handler to stderr
        if self.use_stderr:
            error_handler = logging.StreamHandler(sys.stderr)
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(formatter)
            self.logger.addHandler(error_handler)

        # Create and add a rotating file handler.
        file_handler = RotatingFileHandler(
            filename=self.log_file,
            maxBytes=self.max_bytes,
            backupCount=self.backup_count,
            encoding="utf-8"
        )
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Prevent log propagation to ancestor loggers.
        self.logger.propagate = False

        # Optionally suppress logging from noisy external modules.
        logging.getLogger("py4j").setLevel(logging.ERROR)

    def get_logger(self) -> logging.Logger:
        """
        Returns the configured logger instance.
        """
        return self.logger

# Example usage:
if __name__ == "__main__":
    lm = LoggerManager("test_logger", log_level=logging.DEBUG)
    logger = lm.get_logger()
    logger.debug("Debug message.")
    logger.info("Info message.")
    logger.warning("Warning message.")
    logger.error("Error message.")
