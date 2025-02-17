from collections import namedtuple

curr_map = namedtuple('currency_map', ['curr_code', 'value'])

AVAILABLE_CURRENCIES = [
    'THB', 'USD', 'AUD', 'HKD', 'CAD', 'NZD', 'SGD', 'EUR', 'HUF',
    'CHF', 'GBP', 'UAH', 'JPY', 'CZK', 'DKK', 'ISK', 'NOK', 'SEK',
    'RON', 'BGN', 'TRY', 'ILS', 'CLP', 'PHP', 'MXN', 'ZAR', 'BRL',
    'MYR', 'IDR', 'INR', 'KRW', 'CNY', 'XDR'
]
