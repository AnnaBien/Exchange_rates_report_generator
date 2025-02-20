import unittest
from unittest.mock import patch, mock_open
import pandas as pd
from datetime import date
from collections import namedtuple
from services.report_generator_service import (
    generate_report, _validate_if_report_exists, _generate_csv_report_with_historical_data,
    _generate_json_report_with_historical_data, _get_largest_exchange_rate_increase_and_decrease,
    _find_largest_increase_and_decrease, _generate_csv_report_with_analytical_data,
    _generate_json_report_with_analytical_data
)

class TestReportGeneratorTypeAndFormat(unittest.TestCase):
    def setUp(self):
        self.start_date = date(2024, 1, 1)
        self.end_date = date(2024, 1, 2)
        self.currency_codes = ('EUR',)
        self.sample_df = pd.DataFrame({
            'date': [date(2024, 1, 1), date(2024, 1, 2)],
            'currency_code': ['EUR', 'EUR'],
            'currency_name': ['euro', 'euro'],
            'currency_rate': [1.0, 1.1]
        })

    @patch('services.report_generator_service.get_exchange_rates_data_from_database')
    @patch('services.report_generator_service._generate_csv_report_with_historical_data')
    def test_generate_historical_csv_report(self, mock_generate_csv, mock_get_data):
        """
        Test that given features of the report will be provided:
            - type: historical
            - format: .csv
        """

        mock_get_data.return_value = [self.sample_df]
        report_path = "report.csv"

        generate_report('h', self.start_date, self.end_date, self.currency_codes, report_path)

        mock_get_data.assert_called_once_with(self.start_date, self.end_date, self.currency_codes)
        mock_generate_csv.assert_called_once()

    @patch('services.report_generator_service.get_exchange_rates_data_from_database')
    @patch('services.report_generator_service._generate_json_report_with_historical_data')
    def test_generate_historical_json_report(self, mock_generate_json, mock_get_data):
        """
        Test that given features of the report will be provided:
            - type: historical
            - format: .json
        """

        mock_get_data.return_value = [self.sample_df]
        report_path = "report.json"

        generate_report('historical', self.start_date, self.end_date, self.currency_codes, report_path)

        mock_get_data.assert_called_once_with(self.start_date, self.end_date, self.currency_codes)
        mock_generate_json.assert_called_once()

    @patch('services.report_generator_service.get_exchange_rates_data_from_database')
    @patch('services.report_generator_service._generate_csv_report_with_analytical_data')
    def test_generate_analytical_csv_report(self, mock_generate_csv, mock_get_data):
        """
        Test that given features of the report will be provided:
            - type: analytical
            - format: .csv
        """

        mock_get_data.return_value = [self.sample_df]
        report_path = "report.csv"

        generate_report('a', self.start_date, self.end_date, self.currency_codes, report_path)

        mock_get_data.assert_called_once_with(self.start_date, self.end_date, self.currency_codes)
        mock_generate_csv.assert_called_once()

    @patch('services.report_generator_service.get_exchange_rates_data_from_database')
    @patch('services.report_generator_service._generate_json_report_with_analytical_data')
    def test_generate_analytical_json_report(self, mock_generate_csv, mock_get_data):
        """
        Test that given features of the report will be provided:
            - type: analytical
            - format: .json
        """
        mock_get_data.return_value = [self.sample_df]
        report_path = "report.json"

        generate_report('a', self.start_date, self.end_date, self.currency_codes, report_path)

        mock_get_data.assert_called_once_with(self.start_date, self.end_date, self.currency_codes)
        mock_generate_csv.assert_called_once()

    def test_invalid_report_type(self):
        """
        Test that SystemExit exception is raised on unsupported report type.
        """

        with self.assertRaises(SystemExit):
            generate_report('invalid', self.start_date, self.end_date, self.currency_codes, "report.csv")

    def test_invalid_file_extension(self):
        """
        Test that SystemExit exception is raised on unsupported file format.
        """

        with self.assertRaises(SystemExit):
            generate_report('h', self.start_date, self.end_date, self.currency_codes, "report.txt")

class TestHistoricalReportGeneration(unittest.TestCase):
    def setUp(self):
        self.sample_df = pd.DataFrame({
            'date': [date(2024, 1, 1), date(2024, 1, 2)],
            'currency_code': ['EUR', 'EUR'],
            'currency_name': ['euro', 'euro'],
            'currency_rate': [1.0, 1.1]
        })
        self.df_gen = [self.sample_df]

    @patch('builtins.open')
    def test_generate_csv_historical_report(self, mock_file):
        """
        Test that historical .csv report generation is called.
        """

        report_path = "./report.csv"
        _generate_csv_report_with_historical_data(self.df_gen, report_path)
        mock_file.assert_called_with(report_path, 'a', encoding='utf-8', errors='strict', newline='')

    @patch('builtins.open')
    def test_generate_json_historical_report(self, mock_file):
        """
        Test that historical .json report generation is called.
        """

        report_path = "report.json"
        _generate_json_report_with_historical_data(self.df_gen, report_path)
        mock_file.assert_called_with(report_path, mode='w', encoding='utf-8')

class TestAnalyticalReportGeneration(unittest.TestCase):
    def setUp(self):
        self.curr_map = namedtuple('currency_map', ['curr_info', 'value'])
        self.test_data = {
            'max_recorded_increase': self.curr_map(('EUR', 'euro'), 0.2),
            'max_recorded_decrease': self.curr_map(('USD', 'dolar amerykański'), 0.3)
        }

    @patch('builtins.open', new_callable=mock_open)
    def test_generate_csv_analytical_report(self, mock_file):
        """
        Test that analytica .csv report generation is called.
        """

        report_path = "report.csv"
        _generate_csv_report_with_analytical_data(self.test_data, report_path)
        mock_file.assert_called_with(report_path, mode='w', encoding='utf-8')

    @patch('builtins.open', new_callable=mock_open)
    def test_generate_json_analytical_report(self, mock_file):
        """
        Test that analytica .json report generation is called.
        """

        report_path = "report.json"
        _generate_json_report_with_analytical_data(self.test_data, report_path)
        mock_file.assert_called_with(report_path, mode='w', encoding='utf-8')

class TestGenerateAnalyticalData(unittest.TestCase):

    def test_get_largest_exchange_rate_changes(self):
        """
        Test calculation of largest exchange rate changes
        """

        sample_df = pd.DataFrame({
            'date': [date(2024, 1, 1),
                     date(2024, 1, 2),
                     date(2024, 1, 1),
                     date(2024, 1, 2)],
            'currency_code': ['EUR', 'EUR', 'USD', 'USD'],
            'currency_name': ['euro', 'euro', 'dolar amerykański', 'dolar amerykański'],
            'currency_rate': [1.0, 1.2, 2.1, 1.8]
        })
        df_gen = [sample_df]

        result = _get_largest_exchange_rate_increase_and_decrease(df_gen)
        self.assertIn('max_recorded_increase', result)
        self.assertIn('max_recorded_decrease', result)
        self.assertTrue(result['max_recorded_increase'].curr_info)
        self.assertTrue(result['max_recorded_increase'].value)
        self.assertTrue(result['max_recorded_decrease'].curr_info)
        self.assertTrue(result['max_recorded_decrease'].value)

    def test_find_largest_increase_and_decrease_only_increasing_values(self):
        """
        Test finding the largest rate increases and decreases when values are constantly increasing.
        """

        max_increase, max_decrease = _find_largest_increase_and_decrease([1.0, 1.2, 1.4, 1.6, 1.8])

        self.assertEqual(0.8, round(max_increase, 1))
        self.assertEqual(0, round(max_decrease, 1))

    def test_find_largest_increase_and_decrease_only_decreasing_values(self):
        """
        Test finding the largest rate increases and decreases when values are constantly decreasing.
        """

        max_increase, max_decrease = _find_largest_increase_and_decrease([1.8, 1.6, 1.4, 1.2, 1.0])

        self.assertEqual(0, round(max_increase, 1))
        self.assertEqual(0.8, round(max_decrease, 1))

    def test_find_largest_increase_and_decrease_constant_values(self):
        """
        Test finding the largest rate increases and decreases when values are constantly the same.
        """

        max_increase, max_decrease = _find_largest_increase_and_decrease([1.0, 1.0, 1.0, 1.0])

        self.assertEqual(0, round(max_increase, 1))
        self.assertEqual(0, round(max_decrease, 1))

    def test_find_largest_increase_and_decrease_single_value(self):
        """
        Test finding the largest rate increases and decreases when there is single value only.
        """

        max_increase, max_decrease = _find_largest_increase_and_decrease([1.0])

        self.assertEqual(0, round(max_increase, 1))
        self.assertEqual(0, round(max_decrease, 1))

    def test_find_largest_increase_and_decrease_negative_values(self):
        """
        Test finding the largest rate increases and decreases when there are negative values also.
        """

        max_increase, max_decrease = _find_largest_increase_and_decrease([-1.0, -2.0, 1.0, -3.0, 2.0])

        self.assertEqual(5.0, round(max_increase, 1))
        self.assertEqual(4.0, round(max_decrease, 1))

if __name__ == '__main__':
    unittest.main()