# Exchange Rates report generator

This project is used to generate reports of exchange rates. There are 2 types of reports available:

### Historical (option: ```--report-type h```)

Generates report with all data available in the given time period.
It can be defined which currencies should be incorporated into the report (option ```--currency```).
By default all available currencies will be included.

### Analytical (option: ```--report-type a```)

Generated report will contain information which currencies noted 
the greatest rise and fall in the exchange rate <u>anytime</u> in the given time period.
Report will contain only two entries: 
- Currency for which exchange rate increase was the greatest and the growth value.
- Currency for which exchange rate decrease was the greatest and the drop value.


### Supported report formats:
 - csv (option: ```--format csv```)
 - json (option: ```--format json```)

## Data acquisition

The data is obtained through communication with the [NBP API](https://api.nbp.pl/#kursyWalut). 
Currently only data acquisition from table A is supported.
Downloaded data is stored in the local SQLite database.

The implemented cache-like mechanism allows the program to retrieve data from the local database if it is available there. Otherwise, program will communicate with the API to retrieve new data and store it in the database.

If data cannot be downloaded from the NBP API, a report will not be generated.

## Usage

```commandline
usage: main.py [-h] [-d] [-r TYPE] [-c CURRENCY CODE] [-s START_DATE]
               [-e END_DATE] [-p DIR_PATH] [-n FILENAME] [-f EXTENSION]

Generate a report based on currency rates; Note: Report will not be generated
if data for requested period is not available

options:
  -h, --help            show this help message and exit
  -d, --debug           enable debug logs
  -r TYPE, --report-type TYPE
                        Options: "h" or "historical" - generate a report with
                        historical data; Options: "a" or "analytical" -
                        generate a report with currencies that had the
                        greatest rise and fall in the exchange rate during the
                        given time period, Default: h
  -c CURRENCY CODE, --currency CURRENCY CODE
                        generates report for specified currencies, specify
                        single value or comma separated values (ISO 4217)
  -s START_DATE, --start-date START_DATE
                        define the start date included in report YYYY-MM-DD
                        (ISO 8601), Default: today
  -e END_DATE, --end-date END_DATE
                        define the end date included in report YYYY-MM-DD (ISO
                        8601), Default: today
  -p DIR_PATH, --dir-path DIR_PATH
                        the path to the existing directory of the target
                        report file, Default: current working directory
  -n FILENAME, --filename FILENAME
                        the filename of the target report file, Default:
                        exchange_rates_report.csv
  -f EXTENSION, --format EXTENSION
                        the report extension: json or csv; Overrides the
                        extension given in filename. Default: csv
```


## How to run the project

Make sure to use python 3.12.8 version or newer.

Open the project in a virtual environment and go to the main directory.

Install required packages:
```commandline
pip install pipenv
python -m pipenv sync
```

Use command line to run the program:
```commandline
python app/main.py --help
```