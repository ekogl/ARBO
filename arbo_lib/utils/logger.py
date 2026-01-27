# arbo_lib/utils/logger.py
import logging
import sys

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent adding multiple handlers if get_logger is called repeatedly
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Ensure logs propagate to Airflow's root logger
    logger.propagate = True

    return logger