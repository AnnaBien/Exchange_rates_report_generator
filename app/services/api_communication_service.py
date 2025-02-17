"""
Module for NBP API interaction.
"""

import requests

from math import ceil
from itertools import chain
from datetime import timedelta
from typing import Iterator
from requests.exceptions import Timeout, ConnectionError, HTTPError, RequestException

from services.logging_service import get_logger

logger = get_logger(__name__)

def request_data_from_nbp_database(dates_ranges: list, currency_codes: tuple = None, table: str = 'A') -> Iterator:
    """
    Send HTTPS requests to NPB API in order to collect exchange rates data.

    :param dates_ranges: List of intervals (tuples) with date ranges\
    :param currency_codes: (str) Currency code
    :param table: (str) Available API tables: A, B or C.
        Currently sending requests to A table only are supported.

    :return: (itertools.chain) Iterator over received responses
    """

    def split_into_chunks_and_send_requests(uri: str) -> Iterator:
        """
        Function helper; split too large ranges into chunks with length of max 93 days. (NBP API limitation)

        :param uri: (str) Part of an uri without dates defined
        :return: (itertools.chain) Iterator over received responses
        """

        _response = []
        for start_date, end_date in dates_ranges:
            if (end_date - start_date) <= timedelta(days=max_len):
                _response = chain(
                    _response,
                    _send_request_to_nbp_api(f'{uri}/{start_date}/{end_date}/')
                )
            else:
                date_chunks_num = ceil((end_date - start_date) / timedelta(days=max_len))
                for i in range(date_chunks_num):
                    chunk_start_date = start_date + timedelta(days=max_len * i)
                    chunk_end_date = start_date + timedelta(days=max_len * (i + 1) - 1)
                    chunk_end_date = chunk_end_date if chunk_end_date < end_date else end_date
                    _response = chain(
                        _response,
                        _send_request_to_nbp_api(f'{uri}/{chunk_start_date}/{chunk_end_date}/')
                    )
        return _response

    max_len, response = 93, []
    if currency_codes:
        rates_uri = f'https://api.nbp.pl/api/exchangerates/rates/{table}'
        for currency_code in currency_codes:
            single_currency_uri = rates_uri + f'/{currency_code}'
            response = chain(response, split_into_chunks_and_send_requests(single_currency_uri))
    else:
        tables_uri = f'https://api.nbp.pl/api/exchangerates/tables/{table}'
        response = chain(response, split_into_chunks_and_send_requests(tables_uri))
    return response


def _send_request_to_nbp_api(uri: str) -> list:
    """
    Send single HTTPS request to NBP API in order to collect exchange rates data.

    :param uri: (str) API endpoint URL
    :return: (list) Response data in json format
    """

    try:
        response = requests.get(uri, headers={'Accept': 'application/json'})
        response.raise_for_status()
        if isinstance((response_json := response.json()), dict):
            return [response_json]
        return response_json
    except Timeout:
        logger.warning('The request timed out.')
    except ConnectionError:
        logger.warning('Failed to connect to the server.')
    except HTTPError as http_err:
        logger.debug(f'HTTP error occurred: {http_err}')
        if http_err.response.status_code == 404:
            logger.debug(f'The data requested for the date range '
                         f'{' - '.join(uri.split('/')[-3:-1])} are not available on the server')
    except RequestException as req_err:
        logger.warning(f'Request error occurred: {req_err}')

    return []
