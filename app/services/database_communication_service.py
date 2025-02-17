"""
Module for database interaction.
"""

import os
import pandas
import sqlalchemy as db

from typing import Iterator
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import SingletonThreadPool
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from database_models.exchange_rates import ExchangeRates, Base
from services.logging_service import get_logger
from services.variables import AVAILABLE_CURRENCIES

logger = get_logger(__name__)

class _DatabaseOp:

    _INSTANCE = None

    def __new__(cls):
        if not cls._INSTANCE:
            cls._INSTANCE = super().__new__(cls)
        return cls._INSTANCE

    def __init__(self):
        self._exchange_rates_table = ExchangeRates
        self._db_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), f'{self._exchange_rates_table.__tablename__}.db'
        )
        self._engine = db.create_engine(url=f'sqlite:///{self._db_path}', poolclass=SingletonThreadPool)
        self._create_database_if_not_exist()


    def _create_database_if_not_exist(self):
        """
        Create a SQLite database in a temporary file.
        """

        if not os.path.exists(self._db_path):
            Base.metadata.create_all(bind=self._engine)

    def insert(self, input_data: list):
        """
        Execute Insert command into ExchangeRates table. Ignore input data that already exists in the database

        :param input_data: List[dict] e.g.
            [{'currency_code': ..., 'currency_rate': ..., 'date': ...},
             {'currency_code': ..., 'currency_rate': ..., 'date': ...}, ...]
        :return: None
        """

        insert_cmd = (
            sqlite_insert(self._exchange_rates_table)
            .values(input_data)
            .on_conflict_do_nothing()
        )
        with Session(self._engine) as session:
            session.execute(insert_cmd)
            session.commit()

    def _select(self, query: db.Select) -> Iterator:
        """
        Send a select query to the ExchangeRates table. Returns output as pandas DataFrame iterator.

        :param query: (sqlalchemy.sql.selectable.Select) Object returned from db.select()
        :return: Iterator[pandas.core.frame.DataFrame]
        """

        session = sessionmaker(bind=self._engine)()
        df_iter = pandas.read_sql(
            sql=session.query(query.subquery()).statement,
            con=self._engine,
            chunksize=100
        )
        return df_iter

    def select_all_data_between_dates(self, start_date, end_date) -> Iterator:
        """
        Construct a query to collect exchange rates data for
        all available currencies within a given date range and execute it.

        :param start_date: (datetime.date) Start date of user specified range
        :param end_date: (datetime.date) End date of user specified range
        :return: Iterator[pandas.core.frame.DataFrame]
        """

        query = (
            db.select(self._exchange_rates_table)
            .where(self._exchange_rates_table.date.between(start_date, end_date))
            .order_by(self._exchange_rates_table.currency_code, self._exchange_rates_table.date)
        )
        logger.debug(f'Select all data between dates {start_date} - {end_date}')
        return self._select(query)

    def select_specific_currency_data_between_dates(self, start_date, end_date, currency_codes) -> Iterator:
        """
        Construct a query to collect exchange rates data for
        specified currencies within a given date range and execute it.

        :param start_date: (datetime.date) Start date of user specified range
        :param end_date: (datetime.date) End date of user specified range
        :param currency_codes: (tuple) Currency codes
        :return: Iterator[pandas.core.frame.DataFrame]
        """

        query = (
            db.select(self._exchange_rates_table)
            .where(self._exchange_rates_table.date.between(start_date, end_date))
            .filter(self._exchange_rates_table.currency_code.in_(currency_codes))
            .order_by(self._exchange_rates_table.currency_code, self._exchange_rates_table.date)
        )
        logger.debug(f'Select all data between dates {start_date} - {end_date} for currencies {currency_codes}')
        return self._select(query)

    def select_distinct_dates_for_all_between_dates(self, start_date, end_date) -> Iterator:
        """
        Construct a query to collect distinct date values that stores records for
        all available currencies within a given date range and execute it.

        Note: If number of distinct currencies records for a particular date is less than a known number
            of available currencies, data stored for this date is considered as incomplete.

        :param start_date: (datetime.date) Start date of user specified range
        :param end_date: (datetime.date) End date of user specified range
        :return: Iterator[pandas.core.frame.DataFrame]
        """

        query = (
            db.select(self._exchange_rates_table.date)
            .where(self._exchange_rates_table.date.between(start_date, end_date))
            .group_by(self._exchange_rates_table.date)
            .order_by(self._exchange_rates_table.date)
            .having(db.func.count(self._exchange_rates_table.currency_code) >= len(AVAILABLE_CURRENCIES))
        )
        logger.debug(f'Select dates that contains records for time frame {start_date} - {end_date}')
        return self._select(query)


    def select_distinct_dates_for_specific_currency_between_dates(self, start_date, end_date,
                                                                  currency_codes) -> Iterator:
        """
        Construct a query to collect distinct date values that stores records for
        all specified currency codes within a given date range and execute it.

        :param start_date: (datetime.date) Start date of user specified range
        :param end_date: (datetime.date) End date of user specified range
        :param currency_codes: (tuple) Currency codes
        :return: Iterator[pandas.core.frame.DataFrame]
        """

        query = (
            db.select(self._exchange_rates_table.date)
            .where(self._exchange_rates_table.date.between(start_date, end_date))
            .filter(self._exchange_rates_table.currency_code.in_(currency_codes))
            .group_by(self._exchange_rates_table.date)
            .order_by(self._exchange_rates_table.date)
            .having(db.func.count(self._exchange_rates_table.currency_code) >= len(currency_codes))
        )
        logger.debug(f'Select dates that contains records for time frame  {start_date} - {end_date} '
                     f'for currencies {currency_codes}')
        return self._select(query)


def get_exchange_rates_data_from_database(start_date, end_date, currency_codes=None) -> Iterator:
    """
    Interface for data collection.
    Allows to collect exchange rates data for
    specified or all currency codes within a given date range.

    :param start_date: (datetime.date) Start date of user specified range
    :param end_date: (datetime.date) End date of user specified range
    :param currency_codes: (tuple) Currency codes
    :return: Iterator[pandas.core.frame.DataFrame]
    """

    db_instance = _DatabaseOp()
    if currency_codes:
        return db_instance.select_specific_currency_data_between_dates(
            start_date, end_date, currency_codes
        )
    else:
        return db_instance.select_all_data_between_dates(
            start_date, end_date
        )


def get_dates_with_records(start_date, end_date, currency_codes=None) -> Iterator:
    """
    Interface for data collection.
    Allows to collect only distinct date values that stores records for
    specified or all currency codes within a given date range.

    :param start_date: (datetime.date) Start date of user specified range
    :param end_date: (datetime.date) End date of user specified range
    :param currency_codes: (tuple) Currency codes
    :return: Iterator[pandas.core.frame.DataFrame]
    """

    db_instance = _DatabaseOp()
    if currency_codes:
        return db_instance.select_distinct_dates_for_specific_currency_between_dates(
            start_date, end_date, currency_codes
        )
    else:
        return db_instance.select_distinct_dates_for_all_between_dates(
            start_date, end_date
        )

def save_data_into_database(input_data: list) -> None:
    """
    Interface for data entry.
    Insert data into self._exchange_rates_table table.

    :param input_data: List[dict] e.g.
        [{'currency_code': ..., 'currency_rate': ..., 'date': ...},
         {'currency_code': ..., 'currency_rate': ..., 'date': ...}, ...]
    :return: None
    """

    db_instance = _DatabaseOp()
    db_instance.insert(input_data)
