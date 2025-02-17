"""
Logging module.
"""

import sys
import logging

def get_logger(name: str) -> logging.Logger:
    """
    Get logger object.

    :param name: Logger name
    :return: (logging.Logger)
    """

    return logging.getLogger(name)

def configure_logger_globally(logging_lvl: str) -> None:
    """
    Define logging level.

    :param logging_lvl: Logging level
    :return: None
    """

    if logging_lvl.upper() not in logging.getLevelNamesMapping():
        logging_lvl = 'INFO'

    logging.basicConfig(
        stream=sys.stdout,
        encoding='utf-8',
        level=logging_lvl.upper()
    )
