# pip install yfinance
# pip install hdfs
import datetime
from io import BytesIO

import numpy as np
import yfinance as yf
from hdfs import InsecureClient

from hive_load import load_staging_to_hive


def update_hadoop_yahoo_chart_data(ticker_list=('MSFT', 'GOOG'),
                                   # Default to end today, start yesterday
                                   days_ago_start=1,
                                   days_ago_end=0):
    # First let's pull down some data from yahoo, store it in memory, and then write it to a staging area in hdfs

    # The yfinance package has some convenience functions for this to download multiple tickers
    #  and group them, but this way we replicate what it would look like to actually
    #  hit the endpoint ourselves so it's easier if we want to change to that in the future
    buffer = BytesIO()

    # If we want to do a historical pull for the tickers, we can set this to 365d or however long we want
    today_dt = datetime.datetime.today()
    start_dt = today_dt - datetime.timedelta(days=days_ago_start)

    for days_after_start in range(days_ago_start - days_ago_end):
        cur_start = (start_dt + datetime.timedelta(days=days_after_start)).strftime("%Y-%m-%d")
        cur_end = (start_dt + datetime.timedelta(days=days_after_start) +
                   datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"Processing data for trading_date={cur_start}")

        for ticker in ticker_list:
            print(f"\tticker={ticker}")
            cur_ticker = yf.Ticker(ticker)
            hist = cur_ticker.history(period="max", interval="1h", start=cur_start, end=cur_end)

            if len(hist):
                print(f"No data found for date range: {cur_start} -> {cur_end}")
                continue

            # Add the ticker column into the data
            hist.insert(0, 'Symbol', ticker)

            # Convert timestamps to unix ts
            hist.index = hist.index.view(np.int64)

            # Make sure to append here!
            hist.to_csv(buffer, header=False, mode='a')

        print("\tWriting to hdfs ...")
        client = InsecureClient('http://localhost:50070')
        with client.write(f'/tmp/yahoo_chart_staging/trading_day={cur_start}.csv', overwrite=True) as writer:
            writer.write(buffer.getvalue())

        # Load the data to hive
        print("\tLoading into Hive ...")
        load_staging_to_hive(file_name=f'trading_day={cur_start}.csv',
                             table_name='yahoo_finance.chart',
                             part_col_name='trading_day',
                             part_col_val=cur_start,
                             staging_folder='yahoo_chart_staging')


def update_hadoop_yahoo_options_data(ticker_list=('MSFT', 'GOOG'),
                                     # Default to end today, start yesterday
                                     days_ago_start=1,
                                     days_ago_end=0):
    # First let's pull down some data from yahoo, store it in memory, and then write it to a staging area in hdfs

    # The yfinance package has some convenience functions for this to download multiple tickers
    #  and group them, but this way we replicate what it would look like to actually
    #  hit the endpoint ourselves so it's easier if we want to change to that in the future
    buffer = BytesIO()

    # This gets all the valid date options for the tickers in the list
    all_exp_dates = dict()
    for ticker in ticker_list:
        # We can get a date list of options expirations from the ticker object
        cur_ticker = yf.Ticker(ticker)
        option_exp_dates = cur_ticker.options

        # Populate the dict
        for date in option_exp_dates:
            if not all_exp_dates.get(date):
                all_exp_dates[date] = set()

            all_exp_dates[date].add(ticker)

    for date in all_exp_dates.keys():
        print(f"\texpiration_date={date}")

        for ticker in all_exp_dates[date]:
            print(f"\tticker={ticker}")

            cur_ticker = yf.Ticker(ticker)
            hist = cur_ticker.option_chain(date=date)

            calls = hist.calls

            # Add the ticker column into the data
            calls.insert(0, 'Symbol', ticker)
            calls.insert(1, 'optionType', 'CALLS')

            puts = hist.puts

            # Add the ticker column into the data
            puts.insert(0, 'Symbol', ticker)
            puts.insert(1, 'optionType', 'CALLS')

            # Make sure to append here!
            calls.to_csv(buffer, header=False, mode='a')
            puts.to_csv(buffer, header=False, mode='a')

        print("\tWriting to hdfs ...")
        client = InsecureClient('http://localhost:50070')
        with client.write(f'/tmp/yahoo_options_staging/expiration_date={date}.csv', overwrite=True) as writer:
            writer.write(buffer.getvalue())

        # Load the data to hive
        print("\tLoading into Hive ...")
        load_staging_to_hive(file_name=f'expiration_date={date}.csv',
                             table_name='yahoo_finance.options',
                             part_col_name='expiration_date',
                             part_col_val=date,
                             staging_folder='yahoo_options_staging')


if __name__ == '__main__':
    # Test options load
    update_hadoop_yahoo_options_data()
