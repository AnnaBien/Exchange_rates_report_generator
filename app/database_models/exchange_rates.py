from enum import Enum
from datetime import datetime
from sqlalchemy import String, Float, Date
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass

class ExchangeRates(Base):

    __tablename__ = 'exchange_rates'

    date: Mapped[datetime] = mapped_column(Date, primary_key=True)
    currency_code: Mapped[str] = mapped_column(String(10), primary_key=True)
    currency_rate: Mapped[float] = mapped_column(Float, nullable=False)

    def __repr__(self):
        return (f'ExchangeRates(date={self.date}, currency_code={self.currency_code}, '
                f'currency_rate={self.currency_rate})')


class ApiDatabaseKeysMapping(Enum):
    """
    Mapping key names from API response data to database column names.
    """

    currency = 'currency_code'
    mid = 'currency_rate'
    effectiveDate = 'date'
