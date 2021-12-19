import sys
sys.path.append('T:\Projects_Code\_My_Work\_Strategy_Analysis\qstrader')

import os
import pandas as pd
import pytz
import pytest

from qstrader.asset.asset_mc.equity_mc import Equity_MC
from qstrader.asset.asset_mc.cash_mc import Cash_MC
from qstrader.asset.universe_mc.static_mc import StaticUniverse_MC
from qstrader.data.backtest_data_handler import BacktestDataHandler
from qstrader.data.daily_bar_equity_csv import CSVDailyBarEquityDataSource
from qstrader.data.daily_bar_fx_csv import CSVDailyBarFxDataSource

csv_dir = 'T:/Projects_Code/_My_Work/_Strategy_Analysis/qstrader/tests_tc/unit/data/test_data'

def test_bar_data_csv():

    strategy_symbols = [('EQ:SPY','FX:USD'), ('EQ:MSE','FX:EUR')]

    multi_currency_universe = StaticUniverse_MC(strategy_symbols)

    data_source_equity = CSVDailyBarEquityDataSource(csv_dir, Equity_MC, csv_symbols=['SPY','MSE'])
    data_source_fx = CSVDailyBarFxDataSource(csv_dir, Cash_MC, csv_symbols=['EUR'])

    data_handler = BacktestDataHandler(multi_currency_universe, data_sources=[data_source_equity, data_source_fx])

    dt = pd.Timestamp('2018-01-02 14:30:00', tz=pytz.UTC)
    mid_price_spy = data_handler.get_asset_latest_mid_price(dt, 'SPY')
    mid_price_mse = data_handler.get_asset_latest_mid_price(dt, 'MSE')
    mid_price_eur = data_handler.get_asset_latest_mid_price(dt, 'EUR')

    assert mid_price_spy == 250.78027270535725
    assert mid_price_mse == 34.085
    assert mid_price_eur == 1.2012

    dt = pd.Timestamp('2018-01-02 21:00:00', tz=pytz.UTC)
    mid_price_spy = data_handler.get_asset_latest_mid_price(dt, 'SPY')
    mid_price_mse = data_handler.get_asset_latest_mid_price(dt, 'MSE')
    mid_price_eur = data_handler.get_asset_latest_mid_price(dt, 'EUR')

    assert mid_price_spy == 251.651031
    assert mid_price_mse == 33.96
    assert mid_price_eur == 1.2059


    dt = pd.Timestamp('2018-01-02 23:00:00', tz=pytz.UTC)  ##Note time not available
    mid_price_spy = data_handler.get_asset_latest_mid_price(dt, 'SPY')
    mid_price_mse = data_handler.get_asset_latest_mid_price(dt, 'MSE')
    mid_price_eur = data_handler.get_asset_latest_mid_price(dt, 'EUR')

    assert mid_price_spy == 251.651031
    assert mid_price_mse == 33.96
    assert mid_price_eur == 1.2059