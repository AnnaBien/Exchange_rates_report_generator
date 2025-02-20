import sys
import unittest
import logging
from unittest.mock import patch, Mock
from services.logging_service import get_logger, configure_logger_globally


class TestLoggingService(unittest.TestCase):

    def setUp(self):
        """
        Reset basic logging configuration.
        """

        logging.getLogger().handlers = []
        logging.getLogger().setLevel(logging.NOTSET)

    def test_get_logger_returns_logger_instance(self):
        """
        Function under test: get_logger
        Test that returned object is a logging.Logger instance
        """

        logger = get_logger("test_logger")
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual("test_logger", logger.name)

    def test_get_logger_different_names_return_different_instances(self):
        """
        Function under test: get_logger
        Test that creating loggers with different names results in different instances
        """

        logger1 = get_logger("test_logger1")
        logger2 = get_logger("test_logger2")
        self.assertIsNot(logger1, logger2)
        self.assertNotEqual(logger1.name, logger2.name)

    def test_get_logger_logging_msg(self):
        """
        Function under test: get_logger
        Test that logged message consists of logger name, logging level and message
        """

        with self.assertLogs('test_logger', 'INFO') as logs:
            logger = get_logger('test_logger')
            logger.info('Some message')
            self.assertEqual( ['INFO:test_logger:Some message'], logs.output)

    def test_configure_logger_globally_is_case_insensitive(self):
        """
        Function under test: configure_logger_globally
        Test that function is case-insensitive
        """

        configure_logger_globally('debug')
        logger = logging.getLogger()
        self.assertEqual(logging.DEBUG, logger.level, 'Logging level not set to DEBUG')

    def test_configure_logger_globally_invalid_level(self):
        """
        Function under test: configure_logger_globally
        If invalid logging level is provided function should set logging level to INFO by default.
        """

        configure_logger_globally('INVALID_LEVEL')
        logger = logging.getLogger()
        self.assertEqual(logging.INFO, logger.level, 'Logging level not set to INFO')

    def test_get_logger_message_is_not_printed(self):
        """
        Function under test: configure_logger_globally, get_logger
        Test that debug message will not be printed while INFO logging level is configured
        """

        configure_logger_globally('INFO')
        with self.assertLogs('test_logger', 'INFO') as logs:
            logger = get_logger('test_logger')
            logger.debug('Test debug message')
            logger.info('Test info message')
            self.assertEqual( ['INFO:test_logger:Test info message'], logs.output)


if __name__ == '__main__':
    unittest.main()
