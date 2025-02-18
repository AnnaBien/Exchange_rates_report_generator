"""
Module used to generate reports.
"""

import json
import os.path
import pandas as pd

from pathlib import Path
from typing import Iterator

from services.database_communication_service import get_exchange_rates_data_from_database
from services.logging_service import get_logger
from services.variables import curr_map

logger = get_logger(__name__)


def generate_report(report_type: str, start_date, end_date, currency_codes: tuple, report_path: str) -> None:
    """
    Interface for report generation.
    Determines which report should be generated:
        - historical: A report with historical data.
        - analytical: A report with currencies that had the greatest rise and fall
                      in the exchange rate during the given time period
    Determines the report format:
        - .csv
        - .json

    :param report_type: str('h', 'historical', 'a', 'analytical') Define the type of report
    :param start_date: (datetime.date) Start date of user specified range
    :param end_date: (datetime.date) End date of user specified range
    :param currency_codes: (tuple) Single currency code or multiple values
    :param report_path: (str) Target report path
    :return: None
    """

    extension = Path(report_path).suffix
    df_iter = get_exchange_rates_data_from_database(start_date, end_date, currency_codes)
    match report_type:
        case 'h' | 'historical':
            logger.debug('Generating a report with historical data.')
            match extension:
                case '.json':
                    _generate_json_report_with_historical_data(df_iter, report_path)
                case '.csv':
                    _generate_csv_report_with_historical_data(df_iter, report_path)
                case _:
                    logger.error(f'Unsupported file extension type for report generation: {extension}')
                    raise SystemExit(1)
        case 'a' | 'analytical':
            logger.debug('Generating a report containing the currencies whose rates have risen and fallen the most.')
            analytical_data = _get_largest_exchange_rate_increase_and_decrease(df_iter)
            match extension:
                case '.json':
                    _generate_json_report_with_analytical_data(analytical_data, report_path)
                case '.csv':
                    _generate_csv_report_with_analytical_data(analytical_data, report_path)
                case _:
                    logger.error(f'Unsupported file extension type for report generation: {extension}')
                    raise SystemExit(1)
        case _:
            logger.error(f'Unsupported report type: {report_type}')
            raise SystemExit(1)

    _validate_if_report_exists(report_path)

def _validate_if_report_exists(report_path: str) -> None:
    """
    Log a message.

    :param report_path: (str) Target report path
    :return: None
    """

    if os.path.exists(report_path):
        logger.info(f'Report generated successfully at {report_path}')
    else:
        logger.warning('Report was not generated. If the problem persists please contact the maintainer ;)')

def _generate_csv_report_with_historical_data(df_iter: Iterator, report_path: str) -> None:
    """
    Generate a report in .csv format.
    Report will include all available historical data for requested currencies and date range.

    Note: The data extracted from the database is grouped into chunks.
    If the data of a currency has been divided into separate chunks, they need to be grouped again.

    :param df_iter: (Iterator[pandas.core.frame.DataFrame]) Data retrieved from the database in chunks
    :param report_path: (str) Target report path
    :return: None
    """

    last_currency_code = ''
    for df_chunk in df_iter:
        for currency_code, df in df_chunk.groupby([df_chunk['currency_code']]):
            currency_exchange_rates = df[['date', 'currency_rate']]

            with open(report_path, mode='a', encoding='utf-8') as csv_report:
                if currency_code != last_currency_code:
                    if last_currency_code:
                        csv_report.write('\n\n')
                    csv_report.write(f'{str(currency_code[0])}\n')
                    csv_report.write(f'Date,Exchange rate\n')
            currency_exchange_rates.to_csv(report_path, index=False, header=False, mode='a')
            last_currency_code = currency_code


def _generate_json_report_with_historical_data(df_iter: Iterator, report_path: str) -> None:
    """
    Generate a report in .json format.
    Report will include all available historical data for requested currencies and date range.

    Note: The data extracted from the database is grouped into chunks.
    If the data of a currency has been divided into separate chunks, they need to be grouped again.

    :param df_iter: (Iterator[pandas.core.frame.DataFrame]) Data retrieved from the database in chunks
    :param report_path: (str) Target report path
    :return: None
    """

    json_report = []
    for df_chunk in df_iter:

        first_iter = True
        for currency_code, df in df_chunk.groupby([df_chunk['currency_code']]):
            currency_exchange_rates = df[['date', 'currency_rate']]
            currency_exchange_rates_list = [{'date': str(record['date']), 'rate': record['currency_rate']}
                                            for record in currency_exchange_rates.to_dict('records')]
            if first_iter:
                first_iter = False
                if json_report and currency_code == json_report[-1]['currency_code']:
                    json_report[-1]['rates'].extend(currency_exchange_rates_list)
                    continue

            currency_report = {
                'currency_code': currency_code[0],
                'rates': currency_exchange_rates_list
            }
            json_report.append(currency_report)

    with open(report_path, mode='w', encoding='utf-8') as json_file:
        json_file.write(json.dumps(json_report, indent=4, ensure_ascii=False))


def _get_largest_exchange_rate_increase_and_decrease(df_iter: Iterator) -> dict:
    """
    Calculate which currencies have experienced the greatest increase or decrease in the given time frame.

    Note: The data extracted from the database is grouped into chunks.
    If the data of a currency has been divided into separate chunks, they need to be grouped again.

    :param df_iter: (Iterator[pandas.core.frame.DataFrame]) Data retrieved from the database in chunks
        already limited with user defined time frame.
    :return: (dict) {'max_recorded_increase': namedtuple('curr_info', 'value'),
                     'max_recorded_decrease': namedtuple('curr_info', 'value')}
    """

    curr_max_inc = curr_max_dec = curr_map(None, float('-inf'))
    last_chunk = curr_map(None, None)

    for df_chunk in df_iter:
        grouped_chunks = df_chunk.groupby([df_chunk['currency_code']])
        for currency_code, df in grouped_chunks:
            if currency_code == last_chunk.curr_code:
                df = pd.concat([last_chunk.value, df])

            max_inc, max_dec = _find_largest_increase_and_decrease(df['currency_rate'])
            if max_inc > curr_max_inc.value:
                curr_max_inc = curr_map(currency_code, max_inc)
            if max_dec > curr_max_dec.value:
                curr_max_dec = curr_map(currency_code, max_dec)

        last_row = grouped_chunks.last().iloc[-1]
        last_chunk = curr_map(last_row.name, grouped_chunks.get_group((last_row.name,)))

    return {'max_recorded_increase': curr_max_inc, 'max_recorded_decrease': curr_max_dec}


def _find_largest_increase_and_decrease(pd_series: pd.Series) -> (float, float):
    """
    Algorithm that finds the biggest differences between consecutive numbers in an iterable object.

    Approach:
     - For max increase: keep track of the smallest value and calculate subsequent differences.
     - For max decrease: keep track of the largest value and calculate subsequent differences.

    :param pd_series: (pandas.core.series.Series)
    :return: (float, float) max_increase, max_decrease
    """

    inc_min, dec_max = float('inf'), float('-inf')
    max_increase, max_decrease = 0, 0
    for rate in pd_series:
        inc_min = min(inc_min, rate)
        max_increase = max(max_increase, rate - inc_min)
        dec_max = max(dec_max, rate)
        max_decrease = max(max_decrease, dec_max - rate)
    return max_increase, max_decrease


def _generate_csv_report_with_analytical_data(analytical_data: dict, report_path: str) -> None:
    """
    Generate a report in .csv format.
    The report will provide information on which currencies have experienced the greatest increase and decrease
    in the exchange rate considering the given time period.

    :param analytical_data: (dict) e.g.
        {'max_recorded_increase': namedtuple('curr_info', 'value'),
        'max_recorded_decrease': namedtuple('curr_info', 'value')}
    :param report_path: (str) Target report path
    :return:
    """

    with open(report_path, mode='w', encoding='utf-8') as csv_report:
        for change_type in analytical_data:
            if analytical_data[change_type].value and analytical_data[change_type].curr_code:
                csv_report.write(f'{change_type}, {analytical_data[change_type].value}\n')
                csv_report.write(f'currency_code, {analytical_data[change_type].curr_code[0]}\n\n')
            else:
                csv_report.write(f'{change_type}, {None}\n')


def _generate_json_report_with_analytical_data(analytical_data: dict, report_path: str) -> None:
    """
    Generate a report in .json format.
    The report will provide information on which currencies have experienced the greatest increase and decrease
    in the exchange rate considering the given time period.

    :param analytical_data: (dict) {'max_recorded_increase': namedtuple('curr_info', 'value'),
                                    'max_recorded_decrease': namedtuple('curr_info', 'value')}
    :param report_path: (str) Target report path
    :return:
    """

    json_report = []
    for change_type in analytical_data:
        if analytical_data[change_type].value and analytical_data[change_type].curr_code:
            currency_report = {
                change_type: analytical_data[change_type].value,
                'currency_code': analytical_data[change_type].curr_code[0]
            }
        else:
            currency_report = {
                change_type: None
            }
        json_report.append(currency_report)
    with open(report_path, mode='w', encoding='utf-8') as json_file:
        json_file.write(json.dumps(json_report, indent=4, ensure_ascii=False))
