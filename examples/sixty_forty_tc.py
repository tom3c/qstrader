import sys
sys.path.append('T:\Projects_Code\_My_Work\_Strategy_Analysis\qstrader')

import os
import pandas as pd
import pytz

from qstrader.alpha_model.fixed_signals import FixedSignalsAlphaModel
#from qstrader.asset.equity import Equity
#from qstrader.asset.universe.static import StaticUniverse
from qstrader.data.backtest_data_handler import BacktestDataHandler
#from qstrader.data.daily_bar_csv import CSVDailyBarDataSource
from qstrader.statistics.tearsheet import TearsheetStatistics
from qstrader.trading.backtest_mc import BacktestTradingSession_MC

from qstrader.asset.universe_mc.static_mc import StaticUniverse_MC
from qstrader.asset.asset_mc.equity_mc import Equity_MC
from qstrader.data.daily_bar_equity_csv import CSVDailyBarEquityDataSource

if __name__ == "__main__":
    start_dt = pd.Timestamp('2016-01-25 14:30:00', tz=pytz.UTC)
    end_dt = pd.Timestamp('2017-09-30 23:59:00', tz=pytz.UTC)

    # Construct the symbols and assets necessary for the backtest
    equity_assets = [('SPY', 'USD'), ('AGG', 'USD')]
    strategy_multi_currency_universe = StaticUniverse_MC(equity_assets)

    stock_list = ['SPY', 'AGG']
    #currency_list = ['AUD','JPY','EUR','GBP']

    # To avoid loading all CSV files in the directory, set the
    # data source to load only those provided symbols
    csv_dir = os.environ.get('QSTRADER_CSV_DATA_DIR', '.')
    
    data_source_equity = CSVDailyBarEquityDataSource(csv_dir, Equity_MC, csv_symbols=stock_list) 
    #data_source_fx = CSVDailyBarFxDataSource(csv_dir, Cash_MC, csv_symbols=['EUR'])   
    data_handler = BacktestDataHandler(strategy_multi_currency_universe, data_sources=[data_source_equity])    
    
    # Construct an Alpha Model that simply provides
    # static allocations to a universe of assets
    # In this case 60% SPY ETF, 40% AGG ETF,
    # rebalanced at the end of each month
    strategy_alpha_model = FixedSignalsAlphaModel({'SPY': 0.6, 'AGG': 0.4})
    strategy_backtest = BacktestTradingSession_MC(
        start_dt,
        end_dt,
        strategy_multi_currency_universe,
        strategy_alpha_model,
        rebalance='end_of_month',
        long_only=True,
        cash_buffer_percentage=0.01,
        data_handler=data_handler
    )
    strategy_backtest.run()

    # Construct benchmark assets (buy & hold SPY)
    benchmark_assets = [('SPY', 'USD')]
    benchmark_universe = StaticUniverse_MC(benchmark_assets)

    # Construct a benchmark Alpha Model that provides
    # 100% static allocation to the SPY ETF, with no rebalance
    benchmark_alpha_model = FixedSignalsAlphaModel({'SPY': 1.0})
    benchmark_backtest = BacktestTradingSession_MC(
        start_dt,
        end_dt,
        benchmark_universe,
        benchmark_alpha_model,
        rebalance='buy_and_hold',
        long_only=True,
        cash_buffer_percentage=0.01,
        data_handler=data_handler
    )
    benchmark_backtest.run()

    # Performance Output
    tearsheet = TearsheetStatistics(
        strategy_equity=strategy_backtest.get_equity_curve(),
        benchmark_equity=benchmark_backtest.get_equity_curve(),
        title='60/40 US Equities/Bonds'
    )
    tearsheet.plot_results()
    print ('stop')
