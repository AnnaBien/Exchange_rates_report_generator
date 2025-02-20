import unittest
import pandas as pd

from datetime import date
from unittest.mock import patch
from services.data_collection_service import (
    gather_data_for_date_range, _validate_data_gathering, _calculate_missing_date_intervals,
    _collect_missing_data_from_nbp_api, _align_json_data_with_database_model,
    _reformat_json_for_single_currency_exchange_rates, _reformat_json_for_all_currencies_exchange_rates
)


class TestJSONFormatting(unittest.TestCase):

    @patch('services.data_collection_service._reformat_json_for_single_currency_exchange_rates')
    def test_align_json_data_with_database_model_single_currency(self, mock_func):
        """
        Function under test: _align_json_data_with_database_model
        Test function flow when json with single currency data is provided.
        """

        test_data = {
            'table': 'A',
            'currency': 'dolar amerykański',
            'code': 'USD',
            'rates': []
        }
        _align_json_data_with_database_model(test_data)
        mock_func.assert_called_once_with(test_data)

    @patch('services.data_collection_service._reformat_json_for_all_currencies_exchange_rates')
    def test_align_json_data_with_database_model_all_data(self, mock_func):
        """
        Function under test: _align_json_data_with_database_model
        Test function flow when json with data for all currencies is provided.
        """

        test_data = {
            'table': 'A',
            'effectiveDate': '2025-01-02',
            'rates': []
        }
        _align_json_data_with_database_model(test_data)
        mock_func.assert_called_once_with(test_data)

    def test_reformat_json_for_single_currency_exchange_rates(self):
        """
        Function under test: _reformat_json_for_single_currency_exchange_rates
        Test if items in returned lists are dictionaries with proper keys.
        """

        test_data = {
            'table': 'A',
            'currency': 'dolar amerykański',
            'code': 'USD',
            'rates': [
                {
                    'no': '001/A/NBP/2025',
                    'effectiveDate': '2025-01-02',
                    'mid': 4.1219
                },
                {
                    'no': '002/A/NBP/2025',
                    'effectiveDate': '2025-01-03',
                    'mid': 4.1512
                }
            ]
        }

        result = _reformat_json_for_single_currency_exchange_rates(test_data)
        self.assertEqual(2, len(result))
        self.assertIsInstance(result[0], dict)
        self.assertIn('currency_code', result[0])
        self.assertIn('currency_rate', result[0])
        self.assertIn('date', result[0])

    def test_reformat_json_for_all_currencies_exchange_rates(self):
        """
        Function under test: _reformat_json_for_all_currencies_exchange_rates
        Test if items in returned lists are dictionaries with proper keys.
        """

        test_data = {
            'table': 'A',
            'effectiveDate': '2025-01-02',
            'rates': [
                {
                    'currency': 'bat (Tajlandia)',
                    'code': 'THB',
                    'mid': 0.1202
                },
                {
                    'currency': 'dolar amerykański',
                    'code': 'USD',
                    'mid': 4.1219
                },
                {
                    'currency': 'dolar australijski',
                    'code': 'AUD',
                    'mid': 2.5630
                }
            ]
        }

        result = _reformat_json_for_all_currencies_exchange_rates(test_data)
        self.assertEqual(3, len(result))
        self.assertIsInstance(result[0], dict)
        self.assertIn('currency_code', result[0])
        self.assertIn('currency_rate', result[0])
        self.assertIn('date', result[0])


class TestDataGathering(unittest.TestCase):
    def test_calculate_missing_date_intervals(self):
        """
        Function under test: _calculate_missing_date_intervals
        Test that intervals are properly calculated
        """

        start_date = date(2025,1,1)
        end_date = date(2025,2,1)
        dates_with_records = pd.DataFrame({
            'date': [date(2025, 1, 4),
                     date(2025, 1, 5),
                     date(2025, 1, 8),
                     date(2025, 1, 9)],
        })
        dates_with_records_gen = [dates_with_records]

        result = _calculate_missing_date_intervals(start_date, end_date, dates_with_records_gen)

        self.assertEqual(3, len(result))
        self.assertEqual((date(2025, 1, 1), date(2025, 1, 3)), result[0])
        self.assertEqual((date(2025, 1, 6), date(2025, 1, 7)), result[1])
        self.assertEqual((date(2025, 1, 10), date(2025, 2, 1)), result[2])

    def test_calculate_missing_date_intervals_nothing_missing(self):
        """
        Function under test: _calculate_missing_date_intervals
        Test that no interval will be returned if all data is already collected.
        """

        start_date = date(2025,1,4)
        end_date = date(2025,1,7)
        dates_with_records = pd.DataFrame({
            'date': [date(2025, 1, 4),
                     date(2025, 1, 5),
                     date(2025, 1, 6),
                     date(2025, 1, 7)],
        })
        dates_with_records_gen = [dates_with_records]

        result = _calculate_missing_date_intervals(start_date, end_date, dates_with_records_gen)
        self.assertEqual(0, len(result))

    @patch('services.data_collection_service.request_data_from_nbp_database')
    def test_collect_missing_data_from_nbp_api_call_request(self, mock_func):
        """
        Function under test: _collect_missing_data_from_nbp_api
        Test that API interface function will be called once
        """

        dates_ranges = [
            (date(2025, 1, 1), date(2025, 1, 3)),
            (date(2025, 1, 6), date(2025, 1, 7))
        ]
        currency_codes = ('USD',)
        _collect_missing_data_from_nbp_api(dates_ranges, currency_codes)
        mock_func.assert_called_once_with(dates_ranges, currency_codes)

    @patch('services.data_collection_service.request_data_from_nbp_database')
    @patch('services.data_collection_service.save_data_into_database')
    def test_collect_missing_data_from_nbp_api_save_to_db(self, mock_db_func, mock_api_func):
        """
        Function under test: _collect_missing_data_from_nbp_api
        Test that DB interface function will be called twice
        """

        test_data = [
            {
                'table': 'A',
                'currency': 'dolar amerykański',
                'code': 'USD',
                'rates': [
                    {
                        'no': '001/A/NBP/2025',
                        'effectiveDate': '2025-01-02',
                        'mid': 4.1219
                    }
                ]
            },
            {
                'table': 'A',
                'currency': 'dolar amerykański',
                'code': 'USD',
                'rates': [
                    {
                        'no': '003/A/NBP/2025',
                        'effectiveDate':
                            '2025-01-07',
                        'mid': 4.077
                    }
                ]
            }
        ]
        dates_ranges = [
            (date(2025, 1, 1), date(2025, 1, 3)),
            (date(2025, 1, 6), date(2025, 1, 7))
        ]
        currency_codes = ('USD',)
        mock_api_func.return_value = test_data

        _collect_missing_data_from_nbp_api(dates_ranges, currency_codes)
        mock_db_func.AssertNumberOfCalls(2)

    @patch('services.data_collection_service._calculate_missing_date_intervals')
    def test_validate_data_gathering_raises_SystemExit(self, mock_func):
        """
        Function under test: _validate_data_gathering
        Test that function raises an exception SystemExit
        """

        start_date = date(2025, 1, 4)
        end_date = date(2025, 1, 7)
        mock_func.return_value = [(start_date, end_date)]
        with self.assertRaises(SystemExit):
            _validate_data_gathering(start_date, end_date)

if __name__ == '__main__':
    unittest.main()