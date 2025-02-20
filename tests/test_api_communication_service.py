import datetime
import itertools
import unittest

from unittest.mock import patch, Mock
from requests.exceptions import Timeout, ConnectionError, HTTPError, RequestException
from services.api_communication_service import _send_request_to_nbp_api, request_data_from_nbp_database


class TestAPICommunicationService(unittest.TestCase):

    @patch('requests.get')
    def test_send_request_success_dict(self, get_mock):
        """
        Function under test: _send_request_to_nbp_api
        Test that type of function output is a list when response type is a dictionary.
        """

        response_mock = Mock()
        response_mock.raise_for_status.return_value = None
        response_mock.json.return_value = {'currency_code': 'USD'}
        get_mock.return_value = response_mock

        result = _send_request_to_nbp_api('http://test_uri')
        self.assertEqual(result, [{'currency_code': 'USD'}])

    @patch('requests.get')
    def test_send_request_success_list(self, get_mock):
        """
        Function under test: _send_request_to_nbp_api
        Test that type of function output is a list when response type is a list.
        """

        response_mock = Mock()
        response_mock.raise_for_status.return_value = None
        response_mock.json.return_value = [{'currency_code': 'USD'}]
        get_mock.return_value = response_mock

        result = _send_request_to_nbp_api('http://test_uri')
        self.assertEqual(result, [{'currency_code': 'USD'}])

    @patch('requests.get', side_effect=Timeout)
    def test_send_request_timeout(self, *args):
        """
        Function under test: _send_request_to_nbp_api
        Test that function returns an empty list when a Timeout exception occurs.
        """

        result = _send_request_to_nbp_api('http://test_uri')
        self.assertEqual(result, [])

    @patch('requests.get', side_effect=ConnectionError)
    def test_send_request_connection_error(self, *args):
        """
        Function under test: _send_request_to_nbp_api
        Test that function returns an empty list when a ConnectionError exception occurs.
        """

        result = _send_request_to_nbp_api('http://test_uri')
        self.assertEqual(result, [])

    @patch('requests.get')
    def test_send_request_http_error(self, get_mock):
        """
        Function under test: _send_request_to_nbp_api
        Test that function returns an empty list when a HTTPError exception occurs.
        """

        response_mock = Mock()
        response_mock.status_code = 404
        response_mock.raise_for_status.side_effect = HTTPError(response=response_mock)
        get_mock.return_value = response_mock

        result = _send_request_to_nbp_api('http://test_uri')
        self.assertEqual(result, [])

    @patch('requests.get', side_effect=RequestException())
    def test_send_request_request_exception(self, *args):
        """
        Function under test: _send_request_to_nbp_api
        Test that function returns an empty list when a RequestException exception occurs.
        """

        result = _send_request_to_nbp_api('http://test_uri')
        self.assertEqual(result, [])

    @patch('services.api_communication_service._send_request_to_nbp_api')
    def test_request_data_from_nbp_database_with_currency_codes_small_range(self, send_request_func_mock):
        """
        Function under test: request_data_from_nbp_database
        Request data from date range smaller than 93 days and check whether data acquired from API are correct.
        Specific currency codes are defined.
        """

        start_date = datetime.date(2025, 1, 1)
        end_date = datetime.date(2025, 1, 10)
        dates_ranges = [(start_date, end_date)]
        currency_codes = ('USD',)
        send_request_func_mock.return_value = [{'currency_code': 'USD'}]

        result = request_data_from_nbp_database(dates_ranges, currency_codes)
        self.assertIsInstance(result, itertools.chain)
        result = list(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], {'currency_code': 'USD'})
        self.assertEqual(send_request_func_mock.call_count, 1)

    @patch('services.api_communication_service._send_request_to_nbp_api')
    def test_request_data_from_nbp_database_without_currency_codes_small_range(self, send_request_func_mock):
        """
        Function under test: request_data_from_nbp_database
        Request data from date range smaller than 93 days and check whether data acquired from API are correct.
        Specific currency codes are not defined
        """

        start_date = datetime.date(2021, 1, 1)
        end_date = datetime.date(2021, 1, 10)
        dates_ranges = [(start_date, end_date)]
        currency_codes = None
        send_request_func_mock.return_value = [{'currency_code': 'USD'}]

        result = request_data_from_nbp_database(dates_ranges, currency_codes)
        self.assertIsInstance(result, itertools.chain)
        result = list(result)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], {'currency_code': 'USD'})
        self.assertEqual(send_request_func_mock.call_count, 1)

    @patch('services.api_communication_service._send_request_to_nbp_api')
    def test_request_data_from_nbp_database_large_range(self, send_request_func_mock):
        """
        Function under test: request_data_from_nbp_database
        Request data from date range greater than 93 days and check whether data acquired from API are correct.
        Specific currency codes are defined
        """

        start_date = datetime.date(2025, 1, 1)
        end_date = start_date + datetime.timedelta(days=100)
        dates_ranges = [(start_date, end_date)]
        currency_codes = ('USD',)
        send_request_func_mock.return_value = [{'currency_code': 'USD'}]

        result = request_data_from_nbp_database(dates_ranges, currency_codes)
        self.assertIsInstance(result, itertools.chain)
        result = list(result)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], {'currency_code': 'USD'})
        self.assertEqual(result[1], {'currency_code': 'USD'})
        self.assertEqual(send_request_func_mock.call_count, 2)


if __name__ == '__main__':
    unittest.main()
