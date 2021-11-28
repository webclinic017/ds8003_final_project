# pip install yfinance
# pip install hdfs
import datetime
from io import BytesIO

import paramiko
import pandas as pd
import numpy as np
import yfinance as yf
from hdfs import InsecureClient

from hive_load import load_staging_to_hive


# Examples:
# Top 3 healthcare tickers by market cap:
#   get_ticker_data(sector="Health Care", limit=3, order="ticker_data.market_cap", order_ascending=False)
#
# Top 5 Finance tickers by volume:
#   get_ticker_data(sector="Finance", limit=5, order="ticker_data.volume", order_ascending=False)
def get_ticker_data(sector=None, limit=None, order=None, order_ascending=False):
    ssh = paramiko.SSHClient()

    # This is super not cool, but again, dev env so we'd do it properly in prod
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    ssh.connect('sandbox-hdp.hortonworks.com', username='root', password='hdpsandbox', port=2222)

    hive_load = f"""hive -e "SET hive.cli.print.header=true; select * from yahoo_finance.ticker_data;" """
    _, ssh_stdout, ssh_stderr = ssh.exec_command(hive_load)

    # This is sooooo hacky but windows is a pain to develop on and I'm not doing this in the sandbox
    df = pd.read_csv(ssh_stdout, sep='\t')

    if len(df) == 0:
        print("No ticker data found! Have you set up a ticker_data table?")
        raise

    # Now just slice as required and return a list of tickers
    if sector:
        df = df[df['ticker_data.sector'] == sector]

    if order:
        df = df.sort_values(order, ascending=order_ascending)

    if limit:
        df = df.iloc[:limit]

    tickers = df['ticker_data.ticker'].values.tolist()

    return tickers


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
            hist = cur_ticker.history(period="max", interval="1m", start=cur_start, end=cur_end)

            if len(hist) == 0:
                print(f"No data found for date range: {cur_start} -> {cur_end}")
                continue

            # Add the ticker column into the data
            hist.insert(0, 'Symbol', ticker)

            # Convert timestamps to unix ts
            hist.index = hist.index.view(np.int64)

            # Make sure to append here!
            hist.to_csv(buffer, header=False, mode='a')

        print("\tWriting to hdfs ...")
        client = InsecureClient('http://sandbox-hdp.hortonworks.com:50070')
        with client.write(f'/tmp/yahoo_chart_staging/trading_day={cur_start}.csv', overwrite=True) as writer:
            writer.write(buffer.getvalue())

        # Load the data to hive
        print("\tLoading into Hive ...")
        load_staging_to_hive(file_name=f'trading_day={cur_start}.csv',
                             table_name='yahoo_finance.chart',
                             part_col_name='trading_day',
                             part_col_val=cur_start,
                             staging_folder='yahoo_chart_staging')


def update_hadoop_yahoo_news(ticker_list=('MSFT', 'GOOG')):
    buffer = BytesIO()

    extract_date = datetime.datetime.today().strftime("%Y-%m-%d")

    for ticker in ticker_list:
        print(f"\tticker={ticker}")
        cur_ticker = yf.Ticker(ticker)
        news = cur_ticker.news

        if len(news) == 0:
            print(f"No data found for ticker: {ticker}")
            continue

        news_df = pd.DataFrame.from_dict(news, orient='columns')

        # Add the ticker column into the data
        news_df.insert(0, 'Symbol', ticker)

        # Make sure to append here!
        news_df.to_csv(buffer, header=False, index=False, mode='a')

    print("\tWriting to hdfs ...")
    client = InsecureClient('http://sandbox-hdp.hortonworks.com:50070')
    with client.write(f'/tmp/yahoo_news_staging/extract_date={extract_date}.csv', overwrite=True) as writer:
        writer.write(buffer.getvalue())

    # Load the data to hive
    # NOTE: We don't partiton this, meaning we can get dupes. Easy to dedupe since we have uuids
    print("\tLoading into Hive ...")
    load_staging_to_hive(file_name=f'extract_date={extract_date}.csv',
                         table_name='yahoo_finance.news',
                         staging_folder='yahoo_news_staging')


def update_hadoop_sustainability(ticker_list=('MSFT', 'GOOG')):
    # NOTE: We could group these by date like we did with the options chains, but that's for future work
    buffer = BytesIO()

    # These are the current columns in the sustainability report. If more get added we will need to alter the
    #  hive table to support them, so we need to manually maintain this for now
    columns = ['palmOil', 'controversialWeapons', 'gambling', 'socialScore', 'nuclear', 'furLeather', 'alcoholic',
               'gmo', 'catholic', 'socialPercentile', 'peerCount', 'governanceScore', 'environmentPercentile',
               'animalTesting', 'tobacco', 'totalEsg', 'highestControversy', 'esgPerformance', 'coal', 'pesticides',
               'adult', 'percentile', 'peerGroup', 'smallArms', 'environmentScore', 'governancePercentile',
               'militaryContract']

    for ticker in ticker_list:
        print(f"\tticker={ticker}")
        cur_ticker = yf.Ticker(ticker)
        sustainability = cur_ticker.sustainability

        if type(sustainability) != pd.core.frame.DataFrame:
            print(f"No data found for ticker: {ticker}")
            continue

        # Keep only known cols
        sustainability = sustainability.loc[columns, :]

        # Grab the report date
        report_date = sustainability.index.name

        sustainability = sustainability.transpose()

        # Add the ticker column into the data
        sustainability.insert(0, 'Symbol', ticker)

        # Make sure to append here!
        sustainability.to_csv(buffer, header=False, index=False, mode='a')

        print("\tWriting to hdfs ...")
        client = InsecureClient('http://sandbox-hdp.hortonworks.com:50070')
        with client.write(f'/tmp/yahoo_sustainability_staging/ticker_report_date={ticker}_{report_date}.csv',
                          overwrite=True) as writer:
            writer.write(buffer.getvalue())

        # Load the data to hive
        # NOTE: We don't partiton this, meaning we can get dupes. Easy to dedupe since we have uuids
        print("\tLoading into Hive ...")
        load_staging_to_hive(file_name=f'ticker_report_date={ticker}_{report_date}.csv',
                             table_name='yahoo_finance.sustainability',
                             part_col_name='ticker_report_date',
                             part_col_val=f"{ticker}_{report_date}",
                             staging_folder='yahoo_sustainability_staging')


def update_hadoop_yahoo_options_data(ticker_list=('MSFT', 'GOOG')):
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
        client = InsecureClient('http://sandbox-hdp.hortonworks.com:50070')
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
    # update_hadoop_yahoo_options_data()

    update_hadoop_yahoo_news()
