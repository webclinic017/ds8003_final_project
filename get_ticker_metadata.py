# We need to grab a list of interesting tickers, and some metadata about them. Then store it in a hive table
#  to integrate into our analysis
# The easiest way is to go to this URL: https://www.nasdaq.com/market-activity/stocks/screener

# Manually copy the ticker data into the tmp folder, hdfs breaks on copying (weird, 500kb limit?)
# Hive command:
# LOAD DATA INPATH '/tmp/ticker_data/nasdaq_screener.csv' OVERWRITE INTO TABLE yahoo_finance.ticker_data;