"""
Application entrypoint.
"""

import argparse
import os
import pathlib
from datetime import datetime, date, timedelta

from services.logging_service import get_logger, configure_logger_globally
from services.data_collection_service import gather_data_for_date_range
from services.report_generator_service import generate_report

SUPPORTED_FORMATS = ['csv', 'json']

def convert_date_type(arg: str) -> datetime.date:
    """
    Convert user input date type from string into datetime.date.
    Raise an exception if provided input is invalid.

    :param arg: (str) Date represented in format YYYY-MM-DD
    :return: datetime.date
    """

    try:
        datetime_arg = datetime.strptime(arg, '%Y-%m-%d').date()
    except ValueError:
        raise SystemExit(f'Provided date arg ({str(arg)}) is incorrect, acceptable format is YYYY-MM-DD')
    return datetime_arg

def validate_and_convert_date_args():
    """
    Convert dates and check whether provided date range is correct.

    :return: None
    """

    args.start_date = convert_date_type(args.start_date)
    args.end_date = convert_date_type(args.end_date)

    if args.start_date > args.end_date:
        raise SystemExit('ArgumentTypeError: Incorrect date range')

    if args.start_date < (first_available_date := date(2002, 1, 2)):
        logger.warning(f'Report can only be generated for archive data starting from 2002-01-02. '
                       f'Converting start date into {str(first_available_date)}')
        args.start_date = first_available_date
    if args.end_date > (today := datetime.now().date()):
        logger.warning(f"Only archive data is available. Converting end date into {str(today)}")
        args.end_date = today

def validate_and_convert_path_args():
    """
    Check whether provided arguments that make up the full path to the report are correct.
    If format was not provided either in format or in filename arguments set default value (which is .csv).

    :return: None
    """

    if not os.path.isabs(args.dir_path):
        args.dir_path = os.path.join(os.getcwd(), args.dir_path)

    if not os.path.isdir(args.dir_path):
        raise SystemExit(f'ERROR: Defined directory does not exist: {args.dir_path}. Provide valid path.')

    if '.' in args.filename:
        filename_stem, filename_suffix = args.filename.split('.', 1)
        if args.format:
            args.filename = filename_stem + '.' + args.format
        elif filename_suffix not in SUPPORTED_FORMATS:
            args.filename = filename_stem + '.csv'
            logger.warning(f'Provided not supported format. Replaced with .csv')
    else:
        if args.format:
            args.filename = args.filename + '.' + args.format
        else:
            args.filename = args.filename + '.csv'

    args.full_path = os.path.join(args.dir_path, args.filename)
    logger.debug(f'Report target path set to {args.full_path}')
    check_if_target_report_filepath_already_exists(args.full_path)

def check_if_target_report_filepath_already_exists(path):
    """
    Ask user whether to override an existing report.

    :param path: (str) Full path to target report file
    :return:
    """

    while os.path.exists(path):
        ans = input(f"Given file's path {path} already exists. Do you want to override it? [y/n]")
        match ans.lower():
            case 'y' | 'yes':
                os.remove(path)
            case 'n' | 'no':
                raise SystemExit('Report generation skipped')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Generate a report based on currency rates; '
                    'Note: Report will not be generated if data for requested period is not available'
    )
    parser.add_argument(
        '-d', '--debug',
        action='store_true',
        help='enable debug logs')
    parser.add_argument(
        '-r', '--report-type',
        default='h',
        metavar='TYPE',
        choices=['h', 'historical', 'a', 'analytical'],
        help='Options: "h" or "historical" - generate a report with historical data; \n'
             'Options: "a" or "analytical" - generate a report with currencies that had the greatest rise and fall '
             'in the exchange rate during the given time period, Default: h' )
    parser.add_argument(
        '-c', '--currency',
        metavar='CURRENCY CODE',
        help='generates report for specified currencies, specify single value or comma separated values (ISO 4217)')
    parser.add_argument(
        '-s', '--start-date',
        default=str(datetime.now().date()),
        help='define the start date included in report YYYY-MM-DD (ISO 8601), Default: today')
    parser.add_argument(
        '-e', '--end-date',
        default=str(datetime.now().date()),
        help='define the end date included in report YYYY-MM-DD (ISO 8601), Default: today')
    parser.add_argument(
        '-p', '--dir-path',
        default=os.getcwd(),
        type=pathlib.Path,
        help='the path to the existing directory of the target report file, Default: current working directory')
    parser.add_argument(
        '-n', '--filename',
        default='exchange_rates_report.csv',
        help='the filename of the target report file, Default: exchange_rates_report.csv')
    parser.add_argument(
        '-f', '--format',
        metavar='EXTENSION',
        choices=SUPPORTED_FORMATS,
        help='the report extension: json or csv; Overrides the extension given in filename. '
             'Default: csv')

    args = parser.parse_args()

    if args.debug:
        configure_logger_globally('debug')
    else:
        configure_logger_globally('info')
    logger = get_logger('main')

    if args.currency:
        args.currency = tuple(args.currency.split(','))

    validate_and_convert_date_args()
    validate_and_convert_path_args()

    gather_data_for_date_range(args.start_date, args.end_date, args.currency)
    generate_report(args.report_type, args.start_date, args.end_date, args.currency, args.full_path)
