from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.sensors.weekday import DayOfWeekSensor


def pull_top_5_finance_data_yesterday_and_write_to_hive():
    import yahoo_to_hadoop as y2h

    # Grab some tickers of interest
    top_5_finance_by_market_cap = y2h.get_ticker_data(sector="Finance",
                                                      limit=5,
                                                      order="ticker_data.market_cap",
                                                      order_ascending=False)

    # Now pull down the chart and options data
    y2h.update_hadoop_yahoo_chart_data(ticker_list=top_5_finance_by_market_cap)
    y2h.update_hadoop_yahoo_options_data(ticker_list=top_5_finance_by_market_cap)

    return "Done!"


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}
with DAG(
        'yahoo_finance_refresh',
        default_args=default_args,
        description='Refresh the Yahoo finance API data',
        schedule_interval='0 3 * * 2-6',  # Mon -> Sat at 3am
        start_date=datetime(2021, 1, 1),
        catchup=False,
) as dag:
    # Let's pull the data and write it to HDFS
    t1 = PythonVirtualenvOperator(
        task_id='pull_finance_data_from_yahoo',
        python_callable=pull_top_5_finance_data_yesterday_and_write_to_hive,
        requirements=['yfinance==0.1.66', 'hdfs==2.6.0', 'paramiko==2.8.0', 'numpy==1.21.4', 'pandas==1.3.4'],
    )

    # We can add notification task here?
