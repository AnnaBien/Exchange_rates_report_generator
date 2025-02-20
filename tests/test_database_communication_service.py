
import unittest
import pandas as pd

from typing import Generator
from datetime import date
from sqlalchemy import select, Engine
from sqlalchemy.orm import Session
from unittest.mock import patch, MagicMock, Mock
from database_models.exchange_rates import ExchangeRates
from services.database_communication_service import (
    _DatabaseOp,
    get_exchange_rates_data_from_database,
    get_dates_with_records,
    save_data_into_database
)



class TestDatabaseOp(unittest.TestCase):
    @patch('services.database_communication_service.os.path')
    def setUp(self, mock_path):
        mock_path.exists.return_value = True
        mock_path.join.return_value = ":memory:"

        self.db_op = _DatabaseOp()
        _DatabaseOp._INSTANCE = None

    def test_singleton_pattern(self):
        """
        Test that _DatabaseOp implements singleton pattern correctly
        """

        instance1 = _DatabaseOp()
        instance2 = _DatabaseOp()
        self.assertIs(instance1, instance2)

    @patch('services.database_communication_service.Session')
    def test_insert(self, mock_session):
        """
        Test insert operation
        """

        mock_session_instance = Mock(spec=Session)
        mock_session.return_value.__enter__.return_value = mock_session_instance
        test_data = [
            {
                'currency_name': 'euro',
                'currency_code': 'EUR',
                'currency_rate': 1.0,
                'date': date(2024, 1, 1)
            }
        ]

        self.db_op.insert(test_data)
        mock_session_instance.execute.assert_called_once()
        mock_session_instance.commit.assert_called_once()

    @patch('services.database_communication_service.sessionmaker')
    def test_select(self, mock_sessionmaker):
        """
        Test select operation
        """

        mock_session_instance = Mock(spec=Session)
        mock_sessionmaker.return_value.__enter__.return_value = mock_session_instance
        test_query = select(ExchangeRates)

        result = self.db_op._select(test_query)
        self.assertIsInstance(result, Generator)

    @patch('services.database_communication_service.db.select')
    @patch('services.database_communication_service._DatabaseOp._select')
    def test_select_all_data_between_dates(self, mock_internal_select, mock_select):
        """
        Function under test: select_all_data_between_dates
        Validate function flow.
        """

        start_date = date(2025, 1, 1)
        end_date = date(2025, 1, 2)

        self.db_op.select_all_data_between_dates(start_date, end_date)
        mock_select.assert_called_once()
        mock_internal_select.assert_called_once()

    @patch('services.database_communication_service.db.select')
    @patch('services.database_communication_service._DatabaseOp._select')
    def test_select_specific_currency_data_between_dates(self, mock_internal_select, mock_select):
        """
        Function under test: select_specific_currency_data_between_dates
        Validate function flow.
        """

        start_date = date(2025, 1, 1)
        end_date = date(2025, 1, 2)
        currency_codes = ('EUR', 'USD')

        self.db_op.select_specific_currency_data_between_dates(start_date, end_date, currency_codes)
        mock_select.assert_called_once()
        mock_internal_select.assert_called_once()

    @patch('services.database_communication_service.db.select')
    @patch('services.database_communication_service._DatabaseOp._select')
    def test_select_distinct_dates_all_between_dates(self, mock_internal_select, mock_select):
        """
        Function under test: select_distinct_dates_for_all_between_dates
        Validate function flow.
        """

        start_date = date(2025, 1, 1)
        end_date = date(2025, 1, 2)

        self.db_op.select_distinct_dates_for_all_between_dates(start_date, end_date)
        mock_select.assert_called_once()
        mock_internal_select.assert_called_once()

    @patch('services.database_communication_service.db.select')
    @patch('services.database_communication_service._DatabaseOp._select')
    def test_select_distinct_dates_for_specific_currency_between_dates(self, mock_internal_select, mock_select):
        """
        Function under test: select_distinct_dates_for_specific_currency_between_dates
        Validate function flow.
        """

        start_date = date(2025, 1, 1)
        end_date = date(2025, 1, 2)
        currency_codes = ('EUR', 'USD')

        self.db_op.select_distinct_dates_for_specific_currency_between_dates(start_date, end_date, currency_codes)
        mock_select.assert_called_once()
        mock_internal_select.assert_called_once()




class TestInterfaceSelectQueries(unittest.TestCase):
    def setUp(self):
        self.patcher = patch('services.database_communication_service._DatabaseOp', spec=True)
        self.MockDatabaseOp = self.patcher.start()
        self.mock_db_instance = self.MockDatabaseOp()

        self.start_date = date(2024, 1, 1)
        self.end_date = date(2024, 1, 31)
        self.currency_codes = ('EUR', 'USD')

    def tearDown(self):
        self.patcher.stop()

    def test_get_exchange_rates_data_all_currencies(self):
        """
        Function under test: get_exchange_rates_data_from_database
        Test flow when currency_codes argument is not provided.
        """

        get_exchange_rates_data_from_database(self.start_date, self.end_date)
        self.mock_db_instance.select_all_data_between_dates.assert_called_once_with(
            self.start_date, self.end_date
        )

    def test_get_exchange_rates_data_specific_currencies(self):
        """
        Function under test: get_exchange_rates_data_from_database
        Test flow when currency_codes argument is provided.
        """

        get_exchange_rates_data_from_database(self.start_date, self.end_date, self.currency_codes)
        self.mock_db_instance.select_specific_currency_data_between_dates.assert_called_once_with(
            self.start_date, self.end_date, self.currency_codes
        )

    def test_get_dates_with_records_all_currencies(self):
        """
        Function under test: get_dates_with_records
        Test flow when currency_codes argument is not provided.
        """

        get_dates_with_records(self.start_date, self.end_date)
        self.mock_db_instance.select_distinct_dates_for_all_between_dates.assert_called_once_with(
            self.start_date, self.end_date
        )

    def test_get_dates_with_records_specific_currencies(self):
        """
        Function under test: get_dates_with_records
        Test flow when currency_codes argument is provided.
        """

        get_dates_with_records(self.start_date, self.end_date, self.currency_codes)
        self.mock_db_instance.select_distinct_dates_for_specific_currency_between_dates.assert_called_once_with(
            self.start_date, self.end_date, self.currency_codes
        )


class TestInterfaceInsert(unittest.TestCase):
    def setUp(self):
        self.patcher = patch('services.database_communication_service._DatabaseOp', spec=True)
        self.MockDatabaseOp = self.patcher.start()
        self.mock_db_instance = self.MockDatabaseOp()

    def tearDown(self):
        self.patcher.stop()

    def test_save_data_into_database(self):
        """
        Function under test: save_data_into_database
        Validate function flow.
        """

        test_data = [
            {
                'currency_name': 'euro',
                'currency_code': 'EUR',
                'currency_rate': 1.0,
                'date': date(2024, 1, 1)
            }
        ]

        save_data_into_database(test_data)
        self.mock_db_instance.insert.assert_called_once_with(test_data)


if __name__ == '__main__':
    unittest.main()