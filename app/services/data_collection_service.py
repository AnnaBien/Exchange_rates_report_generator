"""
Module used to collect data either from NBP API or from local SQLite database.
Data newly collected from NBP API are saved in local SQLite database.
"""

from typing import Iterator
from datetime import datetime, timedelta

from database_models.exchange_rates import ApiDatabaseKeysMapping
from services.database_communication_service import save_data_into_database, get_dates_with_records
from services.api_communication_service import request_data_from_nbp_database
from services.logging_service import get_logger

logger = get_logger(__name__)

def gather_data_for_date_range(start_date: datetime.date, end_date: datetime.date,
                               currency_codes: tuple = None) -> None:
    """
    Function used for collection of the data from user specified data range.
    Firstly check if data is already present in the database.
    If any part of data is missing download it with use of NBP API.

    :param start_date: (datetime.date) Start date of user specified range
    :param end_date: (datetime.date) End date of user specified range
    :param currency_codes: (tuple) Single currency code or multiple values
    :return: None
    """

    logger.info(f'Start gathering data for date range {start_date} - {end_date} '
                f'{f'for currencies: {', '.join(currency_codes)}' if currency_codes else ''}')
    dates_with_records_iter = get_dates_with_records(start_date, end_date, currency_codes)
    missing_dates_ranges = _calculate_missing_date_intervals(start_date, end_date, dates_with_records_iter)
    if missing_dates_ranges:
        is_data_collected = _collect_missing_data_from_nbp_api(missing_dates_ranges, currency_codes)
        if not is_data_collected and missing_dates_ranges == [(start_date, end_date)]:
            raise SystemExit('Data for the requested time range is not available on the NBP server')


def _calculate_missing_date_intervals(start_date: datetime.date, end_date: datetime.date,
                                      dates_with_records_iter: Iterator) -> list:
    """
    Based od requested date ranges (strat_date, end_date) -> set A
    and dates retrieved from the database -> set B
    calculate for which date ranges data is missing in the database. -> set A - set B

    :param start_date: (datetime.date) Start date of user specified range
    :param end_date: (datetime.date) End date of user specified range
    :param dates_with_records_iter: (Iterator[pandas.core.frame.DataFrame]) Table with 'date' column only
        (services.database_service.get_dates_with_records function output)
    :return: List of intervals (tuples) with date ranges
    """

    missing_date_intervals = []
    single_day, prev_date, = timedelta(days=1), None
    for data_chunk in dates_with_records_iter:
        if data_chunk.empty and not missing_date_intervals:
            return [(start_date, end_date)]

        for date in data_chunk['date']:
            if prev_date and (date - single_day) > prev_date:
                missing_date_intervals.append((prev_date + single_day, date - single_day))
            elif not prev_date and (date > start_date):
                missing_date_intervals.append((start_date, date - single_day))
            prev_date = date
    if prev_date < end_date:
        missing_date_intervals.append((prev_date + single_day, end_date))
    logger.debug(f'Calculated date ranges for which records are missing from the database: '
                 f'{[f'{d1} - {d2}' for (d1, d2) in missing_date_intervals]}')
    return missing_date_intervals


def _collect_missing_data_from_nbp_api(dates_ranges: list, currency_codes: tuple = None) -> bool:
    """
    Collect data from NBP API and save in the database.

    :param dates_ranges: (list) List of intervals (tuples) with date ranges
    :param currency_codes: (tuple) Single currency code or multiple values
    :return: (bool) Is any data collected
    """

    logger.debug(f'Start collecting missing data from NBP database...')
    response_it = request_data_from_nbp_database(dates_ranges, currency_codes)
    data_collected = False
    for response in response_it:
        data_collected = True
        save_data_into_database(_align_json_data_with_database_model(response))
    return data_collected


def _align_json_data_with_database_model(response_json: dict) -> list:
    """
    Function helper. Prepare data to being inserted into the database.
    Align data acquired via API response with local database model.

    :param response_json: (dict) Single response in json format
    :return: List[dict]
    """

    if response_json.get('code', None):
        return _reformat_json_for_single_currency_exchange_rates(response_json)
    else:
        return _reformat_json_for_all_currencies_exchange_rates(response_json)


def _reformat_json_for_single_currency_exchange_rates(response_json: dict) -> list:
    """
    Convert a dictionary of exchange rates of a single currency into a list of dictionaries.
    The returned list contains dictionaries with key names adapted to the database model column names.

    :param response_json: (dict) Single response in json format
    :return: List[dict] [{'currency_code': ..., 'currency_rate': ..., 'date': ...},
                         {'currency_code': ..., 'currency_rate': ..., 'date': ...}, ...]
    """

    response_data = []
    currency_day_report = {
        ApiDatabaseKeysMapping.currency.value: response_json['code']
    }
    for exchange_rate in response_json['rates']:
        currency_day_report.update({
            ApiDatabaseKeysMapping.mid.value: exchange_rate['mid'],
            ApiDatabaseKeysMapping.effectiveDate.value:
                datetime.strptime(exchange_rate['effectiveDate'], '%Y-%m-%d').date(),
        })
        response_data.append(currency_day_report.copy())
    return response_data


def _reformat_json_for_all_currencies_exchange_rates(response_json: dict) -> list:
    """
    Convert a list of exchange rates of all available currencies into a list of dictionaries.
    The returned list contains dictionaries with key names adapted to the database model column names.

    :param response_json: (dict) Single response in json format
    :return: List[dict] [{'currency_code': ..., 'currency_rate': ..., 'date': ...},
                         {'currency_code': ..., 'currency_rate': ..., 'date': ...}, ...]
    """

    response_data = []
    currency_day_report = {
        ApiDatabaseKeysMapping.effectiveDate.value:
            datetime.strptime(response_json['effectiveDate'], '%Y-%m-%d').date(),
    }
    for exchange_rate in response_json['rates']:
        currency_day_report.update({
            ApiDatabaseKeysMapping.currency.value: exchange_rate['code'],
            ApiDatabaseKeysMapping.mid.value: exchange_rate['mid']
        })
        response_data.append(currency_day_report.copy())
    return response_data
