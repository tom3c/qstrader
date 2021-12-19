import sys
sys.path.append('T:\Projects_Code\_My_Work\_Strategy_Analysis\qstrader')

import pandas as pd
import pytest
import pytz

from qstrader.asset.universe_mc.static_mc import StaticUniverse_MC
from qstrader.broker.simulated_broker_mc import SimulatedBroker_MC
from qstrader.asset.asset_mc.equity_mc import Equity_MC
from qstrader.asset.asset_mc.cash_mc import Cash_MC
from qstrader.asset.universe_mc.static_mc import StaticUniverse_MC
from qstrader.data.backtest_data_handler import BacktestDataHandler
from qstrader.data.daily_bar_equity_csv import CSVDailyBarEquityDataSource
from qstrader.data.daily_bar_fx_csv import CSVDailyBarFxDataSource

import uuid
from qstrader.execution.order_mc import Order_MC
#import test_broker_helper as tbh - ********At some point get these working********

csv_dir = 'T:/Projects_Code/_My_Work/_Strategy_Analysis/qstrader/tests_tc/unit/broker/data'

equity_assets = [('AS51', 'AUD'), ('NKY', 'JPY'), ('DAX', 'EUR'), ('IBEX', 'EUR'), ('UKX', 'GBP'), ('SPX', 'USD'), ('CCMP', 'USD')]
stock_list = ['AS51', 'NKY', 'DAX', 'IBEX','UKX','SPX','CCMP']
currency_list = ['AUD','JPY','EUR','GBP']


def get_order_mc(order_type, dt, asset,  quantity, commission, currency, auto_fx):

    order_id = uuid.uuid4().hex
    return Order_MC(
        order_type,
        dt,
        asset,
        quantity,
        commission,
        currency,
        auto_fx,
        order_id,
    )

class ExchangeMock(object):
    def is_open_at_datetime(self, dt):
        return True

def test_broker_trading_run_auto_fx():

    start_dt = pd.Timestamp('2020-10-02 21:00:00', tz=pytz.UTC)
    universe = StaticUniverse_MC(equity_assets)
    data_source_equity = CSVDailyBarEquityDataSource(csv_dir, Equity_MC, csv_symbols=stock_list)
    data_source_fx = CSVDailyBarFxDataSource(csv_dir, Cash_MC, csv_symbols=currency_list)
    data_handler = BacktestDataHandler(universe, data_sources=[data_source_equity, data_source_fx])
    exchange_mock = ExchangeMock()


    sbwp = SimulatedBroker_MC(start_dt, universe, exchange_mock, data_handler)
    sbwp.create_portfolio(portfolio_id=1234, name="My Portfolio #1")
    sbwp.subscribe_funds_to_account(7500000.0)
    sbwp.subscribe_funds_to_portfolio("1234", 7500000.00)



    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-05 14:30:00', tz=pytz.UTC), 'AS51', 90, 0.0, 'AUD', True)
    order2 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-05 14:30:00', tz=pytz.UTC), 'DAX', 80, 0.0, 'EUR', True)
    order3 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-05 14:30:00', tz=pytz.UTC), 'IBEX', 5, 0.0, 'EUR', True)
    order4 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-05 14:30:00', tz=pytz.UTC), 'SPX', 1000, 0.0, 'USD', True)


    sbwp.submit_order("1234", order1)
    sbwp.submit_order("1234", order2)
    sbwp.submit_order("1234", order3)
    sbwp.submit_order("1234", order4)
    sbwp.update(pd.Timestamp('2020-10-05 21:00:00', tz=pytz.UTC))

    port = sbwp.portfolios["1234"]
    assert port.get_position('AS51')  == 90
    assert port.get_position('DAX')  == 80
    assert port.get_position('IBEX')  == 5
    assert port.get_position('SPX')  == 1000

    assert port.get_mv_local('AS51')  == 535680.00
    assert round(port.get_mv_local('DAX'),4)  == 1020381.60
    assert port.get_mv_local('IBEX')  == 33318.00
    assert port.get_mv_local('SPX')  == 3341210.00

    assert round(port.get_mv_base('AS51'),4)  == 384671.8080
    assert port.get_mv_base('DAX')  == 1202315.63928
    assert port.get_mv_base('IBEX')  == 39258.5994
    assert port.get_mv_base('SPX')  == 3341210.00

    assert port.get_position('AUD')  == 0.0
    assert port.total_cash_value_local('AUD') == 0.0
    assert port.get_position('JPY')  == 0.0
    assert port.total_cash_value_local('JPY') == 0.0
    assert port.get_position('EUR')  == 0.0
    assert port.total_cash_value_local('EUR') == 0.0
    assert port.get_position('GBP')  == 0.0
    assert port.total_cash_value_local('GBP') == 0.0

    assert round(port.get_position('USD'),4)  == 2532543.9533
    assert round(port.total_cash_value_local('USD'),4) == 2532543.9533
    assert round(port.total_cash_value_base,4) == 2532543.9533

    assert port.total_market_value_local('AUD') == 535680.00
    assert port.total_market_value_local('JPY') == 0.0
    assert port.total_market_value_local('EUR') == 1053699.6
    assert port.total_market_value_local('GBP') == 0.0
    assert port.total_market_value_local('USD') == 3341210.00
    
    # assert port.total_market_value_base('AUD') == 384671.8080
    # assert port.total_market_value_base('JPY') == 0.0
    # assert port.total_market_value_base('EUR') == 1241574.23868
    # assert port.total_market_value_base('GBP') == 0.0
    # assert port.total_market_value_base('USD') == 3341210.00  
    
    assert port.total_market_value_base == 4967456.04668
    assert port.total_equity_base == 7500000.0


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-08 14:30:00', tz=pytz.UTC), 'CCMP', 200, 0.0, 'USD', True)
    sbwp.submit_order("1234", order1)
    sbwp.update(pd.Timestamp('2020-10-08 21:00:00', tz=pytz.UTC))


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-12 14:30:00', tz=pytz.UTC), 'NKY', 50, 0.0, 'JPY', True)
    sbwp.submit_order("1234", order1)
    sbwp.update(pd.Timestamp('2020-10-12 21:00:00', tz=pytz.UTC))


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-14 14:30:00', tz=pytz.UTC), 'IBEX', -5, 0.0, 'EUR', True)
    sbwp.submit_order("1234", order1)
    sbwp.update(pd.Timestamp('2020-10-14 21:00:00', tz=pytz.UTC))


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-16 14:30:00', tz=pytz.UTC), 'DAX', -5, 0.0, 'EUR', True)
    order2 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-16 14:30:00', tz=pytz.UTC), 'CCMP', 20, 0.0, 'USD', True)
    sbwp.submit_order("1234", order1)
    sbwp.submit_order("1234", order2)
    sbwp.update(pd.Timestamp('2020-10-16 21:00:00', tz=pytz.UTC))


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-19 14:30:00', tz=pytz.UTC), 'NKY', -50, 0.0, 'JPY', True)
    order2 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-19 14:30:00', tz=pytz.UTC), 'DAX', -15, 0.0, 'EUR', True)
    sbwp.submit_order("1234", order1)
    sbwp.submit_order("1234", order2)
    sbwp.update(pd.Timestamp('2020-10-19 21:00:00', tz=pytz.UTC))


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-20 14:30:00', tz=pytz.UTC), 'SPX', 100, 0.0, 'USD', True)
    sbwp.submit_order("1234", order1)
    sbwp.update(pd.Timestamp('2020-10-20 21:00:00', tz=pytz.UTC))


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-21 14:30:00', tz=pytz.UTC), 'IBEX', 5, 0.0, 'EUR', True)
    sbwp.submit_order("1234", order1)
    sbwp.update(pd.Timestamp('2020-10-21 21:00:00', tz=pytz.UTC))


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-22 14:30:00', tz=pytz.UTC), 'DAX', -5, 0.0, 'EUR', True)
    order2 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-22 14:30:00', tz=pytz.UTC), 'UKX', 10, 0.0, 'GBP', True)
    sbwp.submit_order("1234", order1)
    sbwp.submit_order("1234", order2)
    sbwp.update(pd.Timestamp('2020-10-22 21:00:00', tz=pytz.UTC))


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-23 14:30:00', tz=pytz.UTC), 'UKX', 10, 0.0, 'GBP', True)
    order2 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-23 14:30:00', tz=pytz.UTC), 'CCMP', -220, 0.0, 'USD', True)
    sbwp.submit_order("1234", order1)
    sbwp.submit_order("1234", order2)
    sbwp.update(pd.Timestamp('2020-10-23 21:00:00', tz=pytz.UTC))


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-27 14:30:00', tz=pytz.UTC), 'UKX', 5, 0.0, 'GBP', True)
    sbwp.submit_order("1234", order1)
    sbwp.update(pd.Timestamp('2020-10-27 21:00:00', tz=pytz.UTC))


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-28 14:30:00', tz=pytz.UTC), 'SPX', -1100, 0.0, 'USD', True)
    sbwp.submit_order("1234", order1)
    sbwp.update(pd.Timestamp('2020-10-28 21:00:00', tz=pytz.UTC))

    sbwp.update(pd.Timestamp('2020-10-29 21:00:00', tz=pytz.UTC))

    port = sbwp.portfolios["1234"]
    assert port.get_position('AS51')  == 90
    assert port.get_position('NKY')  == 0.0
    assert port.get_position('DAX')  == 55
    assert port.get_position('IBEX')  == 5
    assert port.get_position('UKX')  == 25
    assert port.get_position('SPX')  == 0.0
    assert port.get_position('CCMP')  == 0.0

    assert port.get_mv_local('AS51')  == 555030.00
    assert port.get_mv_local('NKY')  == 0.00
    assert port.get_mv_local('DAX')  == 676687.55
    assert port.get_mv_local('IBEX')  == 33989.50
    assert port.get_mv_local('UKX')  == 146507.00
    assert port.get_mv_local('SPX')  == 0.0
    assert port.get_mv_local('CCMP')  == 0.0

    assert port.get_mv_base('AS51')  == 390130.587
    assert port.get_mv_base('NKY')  == 0.00
    assert port.get_mv_base('DAX')  == 789965.04587
    assert port.get_mv_base('IBEX')  == 39679.3423
    assert round(port.get_mv_base('UKX'),4)  == 189433.5510
    assert port.get_mv_base('SPX')  == 0.0
    assert port.get_mv_base('CCMP')  == 0.0

    assert port.get_position('AUD')  == 0.0
    assert port.total_cash_value_local('AUD') == 0.0
    assert port.get_position('JPY')  == 0.0
    assert port.total_cash_value_local('JPY') == 0.0
    assert port.get_position('EUR')  == 0.0
    assert port.total_cash_value_local('EUR') == 0.0
    assert port.get_position('GBP')  == 0.0
    assert port.total_cash_value_local('GBP') == 0.0

    assert round(port.get_position('USD'),4)  == 6255589.0422
    assert round(port.total_cash_value_local('USD'),4) == 6255589.0422
    assert round(port.total_cash_value_base,4) == 6255589.0422

    assert port.total_market_value_local('AUD') == 555030.00
    assert port.total_market_value_local('JPY') == 0.0
    assert port.total_market_value_local('EUR') == 710677.05
    assert port.total_market_value_local('GBP') == 146507
    assert port.total_market_value_local('USD') == 0.0
    
    # assert port.total_market_value_base('AUD') == 384671.8080
    # assert port.total_market_value_base('JPY') == 0.0
    # assert port.total_market_value_base('EUR') == 1241574.23868
    # assert port.total_market_value_base('GBP') == 0.0
    # assert port.total_market_value_base('USD') == 3341210.00  
    
    assert port.total_market_value_base == 1409208.52617
    assert round(port.total_equity_base,4) == 7664797.5684


def test_broker_trading_run():

    start_dt = pd.Timestamp('2020-10-02 21:00:00', tz=pytz.UTC)
    universe = StaticUniverse_MC(equity_assets)
    data_source_equity = CSVDailyBarEquityDataSource(csv_dir, Equity_MC, csv_symbols=stock_list)
    data_source_fx = CSVDailyBarFxDataSource(csv_dir, Cash_MC, csv_symbols=currency_list)
    data_handler = BacktestDataHandler(universe, data_sources=[data_source_equity, data_source_fx])
    exchange_mock = ExchangeMock()


    sbwp = SimulatedBroker_MC(start_dt, universe, exchange_mock, data_handler)
    sbwp.create_portfolio(portfolio_id=1234, name="My Portfolio #1")
    sbwp.subscribe_funds_to_account(14000000.0)
    sbwp.subscribe_funds_to_portfolio("1234", 14000000.00)


    fx_order1 = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-05 14:30:00', tz=pytz.UTC), 'AUD', 1000000, 0.0, 'USD', False)
    fx_order2 = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-05 14:30:00', tz=pytz.UTC), 'EUR', 3000000, 0.0, 'USD', False)
    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-05 14:30:00', tz=pytz.UTC), 'AS51', 90, 0.0, 'AUD', False)
    order2 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-05 14:30:00', tz=pytz.UTC), 'DAX', 80, 0.0, 'EUR', False)
    order3 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-05 14:30:00', tz=pytz.UTC), 'IBEX', 5, 0.0, 'EUR', False)
    order4 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-05 14:30:00', tz=pytz.UTC), 'SPX', 1000, 0.0, 'USD', False)
    sbwp.submit_order("1234", fx_order1)
    sbwp.submit_order("1234", fx_order2)
    sbwp.submit_order("1234", order1)
    sbwp.submit_order("1234", order2)
    sbwp.submit_order("1234", order3)
    sbwp.submit_order("1234", order4)
    sbwp.update(pd.Timestamp('2020-10-05 21:00:00', tz=pytz.UTC))


    port = sbwp.portfolios["1234"]
    assert port.get_position('AS51')  == 90
    assert port.get_position('NKY')  == 0.0
    assert port.get_position('DAX')  == 80
    assert port.get_position('IBEX')  == 5
    assert port.get_position('UKX')  == 0
    assert port.get_position('SPX')  == 1000
    assert port.get_position('CCMP')  == 0.0

    assert port.get_mv_local('AS51')  == 535680.0
    assert port.get_mv_local('NKY')  == 0.00
    assert round(port.get_mv_local('DAX'),4)  == 1020381.6
    assert port.get_mv_local('IBEX')  == 33318
    assert port.get_mv_local('UKX')  == 0.0
    assert port.get_mv_local('SPX')  == 3341210.0
    assert port.get_mv_local('CCMP')  == 0.0

    assert round(port.get_mv_base('AS51'),4)  == 384671.8080
    assert port.get_mv_base('NKY')  == 0.00
    assert port.get_mv_base('DAX')  == 1202315.63928
    assert port.get_mv_base('IBEX')  == 39258.5994
    assert round(port.get_mv_base('UKX'),4)  == 0.0
    assert port.get_mv_base('SPX')  == 3341210.0
    assert port.get_mv_base('CCMP')  == 0.0

    assert port.get_position('AUD')  == 464320.0
    assert port.total_cash_value_local('AUD') == 464320.0
    assert port.get_position('JPY')  == 0.0
    assert port.total_cash_value_local('JPY') == 0.0
    assert port.get_position('EUR')  == 1946300.4
    assert port.total_cash_value_local('EUR') == 1946300.4
    assert port.get_position('GBP')  == 0.0
    assert port.total_cash_value_local('GBP') == 0.0

    assert round(port.get_position('USD'),4)  == 6405790.0000
    assert round(port.total_cash_value_local('USD'),4) == 6405790.0000
    assert round(port.total_cash_value_base,4) == 9032543.9533

    assert port.total_market_value_local('AUD') == 535680.0
    assert port.total_market_value_local('JPY') == 0.0
    assert port.total_market_value_local('EUR') == 1053699.6
    assert port.total_market_value_local('GBP') == 0.0
    assert port.total_market_value_local('USD') == 3341210.0
       
    assert port.total_market_value_base == 4967456.04668
    assert port.total_equity_base == 14000000.0


    fx_order1 = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-06 14:30:00', tz=pytz.UTC), 'EUR', -400000, 0.0, 'GBP', False)
    sbwp.submit_order("1234", fx_order1)
    sbwp.update(pd.Timestamp('2020-10-06 21:00:00', tz=pytz.UTC))


    fx_order1 = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-08 14:30:00', tz=pytz.UTC), 'AUD', 2000000, 0.0, 'EUR', False)
    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-08 14:30:00', tz=pytz.UTC), 'AS51', 150, 0.0, 'AUD', False)
    order2 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-08 14:30:00', tz=pytz.UTC), 'CCMP', 200, 0.0, 'USD', False)
    sbwp.submit_order("1234", fx_order1)
    sbwp.submit_order("1234", order1)
    sbwp.submit_order("1234", order2)
    sbwp.update(pd.Timestamp('2020-10-08 21:00:00', tz=pytz.UTC))


    sbwp.update(pd.Timestamp('2020-10-09 21:00:00', tz=pytz.UTC))

    port = sbwp.portfolios["1234"]
    assert port.get_position('AS51')  == 240
    assert port.get_position('NKY')  == 0.0
    assert port.get_position('DAX')  == 80
    assert port.get_position('IBEX')  == 5
    assert port.get_position('UKX')  == 0
    assert port.get_position('SPX')  == 1000
    assert port.get_position('CCMP')  == 200.0

    assert port.get_mv_local('AS51')  == 1425984
    assert port.get_mv_local('NKY')  == 0.00
    assert round(port.get_mv_local('DAX'),4)  == 1028717.6
    assert port.get_mv_local('IBEX')  == 34300.5
    assert port.get_mv_local('UKX')  == 0.0
    assert port.get_mv_local('SPX')  == 3408740
    assert port.get_mv_local('CCMP')  == 2262906

    assert round(port.get_mv_base('AS51'),4)  == 1032412.416
    assert port.get_mv_base('NKY')  == 0.00
    assert round(port.get_mv_base('DAX'),5)  == 1216561.43376
    assert port.get_mv_base('IBEX')  == 40563.7713
    assert round(port.get_mv_base('UKX'),4)  == 0.0
    assert port.get_mv_base('SPX')  == 3408740.0
    assert port.get_mv_base('CCMP')  == 2262906.0

    assert port.get_position('AUD')  == 1595595.0
    assert port.total_cash_value_local('AUD') == 1595595.0
    assert port.get_position('JPY')  == 0.0
    assert port.total_cash_value_local('JPY') == 0.0
    assert round(port.get_position('EUR'),5)  == 327659.35910
    assert round(port.total_cash_value_local('EUR'),5) == 327659.35910
    assert port.get_position('GBP')  == 364381.6473876252
    assert port.total_cash_value_local('GBP') == 364381.6473876252

    assert round(port.get_position('USD'),4)  == 4171968.0
    assert round(port.total_cash_value_local('USD'),4) == 4171968.0


    assert port.total_market_value_local('AUD') == 1425984.0
    assert port.total_market_value_local('JPY') == 0.0
    assert port.total_market_value_local('EUR') == 1063018.1
    assert port.total_market_value_local('GBP') == 0.0
    assert port.total_market_value_local('USD') == 5671646.0

    assert port.total_cash_value_base == 6189676.653600446      
    assert port.total_market_value_base == 7961183.621060001
    assert port.total_equity_base == 14150860.274660446



    fx_order1 = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-12 14:30:00', tz=pytz.UTC), 'JPY', 300000000, 0.0, 'USD', False)
    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-12 14:30:00', tz=pytz.UTC), 'NKY', 500, 0.0, 'JPY', False)
    sbwp.submit_order("1234", fx_order1)
    sbwp.submit_order("1234", order1)
    sbwp.update(pd.Timestamp('2020-10-12 21:00:00', tz=pytz.UTC))

    assert port.total_cash_value_base == 6074850.634027748    
    assert port.total_market_value_base == 8041006.35157856
    assert port.total_equity_base == 14115856.985606309


    fx_order1 = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-13 14:30:00', tz=pytz.UTC), 'AUD', -400000, 0.0, 'USD', False)
    fx_order2 = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-13 14:30:00', tz=pytz.UTC), 'EUR', 750000, 0.0, 'USD', False)
    sbwp.submit_order("1234", fx_order1)
    sbwp.submit_order("1234", fx_order2)
    sbwp.update(pd.Timestamp('2020-10-13 21:00:00', tz=pytz.UTC))


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-14 14:30:00', tz=pytz.UTC), 'IBEX', -5, 0.0, 'EUR', False)
    sbwp.submit_order("1234", order1)
    sbwp.update(pd.Timestamp('2020-10-14 21:00:00', tz=pytz.UTC))


    fx_order1 = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-16 14:30:00', tz=pytz.UTC), 'GBP', 1250000, 0.0, 'JPY', False)
    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-16 14:30:00', tz=pytz.UTC), 'DAX', -5, 0.0, 'EUR', False)
    order2 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-16 14:30:00', tz=pytz.UTC), 'UKX', 70, 0.0, 'GBP', False)
    order3 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-16 14:30:00', tz=pytz.UTC), 'CCMP', 20, 0.0, 'USD', False)
    sbwp.submit_order("1234", fx_order1)
    sbwp.submit_order("1234", order1)
    sbwp.submit_order("1234", order2)
    sbwp.submit_order("1234", order3)
    sbwp.update(pd.Timestamp('2020-10-16 21:00:00', tz=pytz.UTC))


    fx_order1 = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-19 14:30:00', tz=pytz.UTC), 'AUD', -600000, 0.0, 'USD', False)
    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-19 14:30:00', tz=pytz.UTC), 'NKY', -500, 0.0, 'JPY', False)
    order2 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-19 14:30:00', tz=pytz.UTC), 'DAX', -15, 0.0, 'EUR', False)
    sbwp.submit_order("1234", fx_order1)
    sbwp.submit_order("1234", order1)
    sbwp.submit_order("1234", order2)
    sbwp.update(pd.Timestamp('2020-10-19 21:00:00', tz=pytz.UTC))


    fx_order1 = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-20 14:30:00', tz=pytz.UTC), 'JPY', -30000000, 0.0, 'USD', False)
    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-20 14:30:00', tz=pytz.UTC), 'SPX', 100, 0.0, 'USD', False)
    sbwp.submit_order("1234", fx_order1)
    sbwp.submit_order("1234", order1)
    sbwp.update(pd.Timestamp('2020-10-20 21:00:00', tz=pytz.UTC))


    fx_order1 = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-21 14:30:00', tz=pytz.UTC), 'JPY', -20000000, 0.0, 'USD', False)
    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-21 14:30:00', tz=pytz.UTC), 'IBEX', 5, 0.0, 'EUR', False)
    sbwp.submit_order("1234", fx_order1)
    sbwp.submit_order("1234", order1)
    sbwp.update(pd.Timestamp('2020-10-21 21:00:00', tz=pytz.UTC))


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-22 14:30:00', tz=pytz.UTC), 'DAX', -5, 0.0, 'EUR', False)
    order2 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-22 14:30:00', tz=pytz.UTC), 'UKX', 10, 0.0, 'GBP', False)
    sbwp.submit_order("1234", order1)
    sbwp.submit_order("1234", order2)
    sbwp.update(pd.Timestamp('2020-10-22 21:00:00', tz=pytz.UTC))

    assert round(port.total_cash_value_base,7) == 5376812.3354881   
    assert port.total_market_value_base == 8981217.14899
    assert port.total_equity_base == 14358029.484478105


    fx_order1 = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-23 14:30:00', tz=pytz.UTC), 'EUR', 150000, 0.0, 'USD', False)
    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-23 14:30:00', tz=pytz.UTC), 'UKX', 10, 0.0, 'GBP', False)
    order2 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-23 14:30:00', tz=pytz.UTC), 'CCMP', -220, 0.0, 'USD', False)
    sbwp.submit_order("1234", fx_order1)
    sbwp.submit_order("1234", order1)
    sbwp.submit_order("1234", order2)
    sbwp.update(pd.Timestamp('2020-10-23 21:00:00', tz=pytz.UTC))


    fx_order1  = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-26 14:30:00', tz=pytz.UTC), 'GBP', -1000000, 0.0, 'USD', False)
    sbwp.submit_order("1234", fx_order1)
    sbwp.update(pd.Timestamp('2020-10-26 21:00:00', tz=pytz.UTC))

    assert round(port.total_cash_value_base,7) == 7829046.3614988  
    assert port.total_market_value_base == 6401704.0579699995
    assert port.total_equity_base == 14230750.41946882


    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-27 14:30:00', tz=pytz.UTC), 'UKX', 5, 0.0, 'GBP', False)
    sbwp.submit_order("1234", order1)
    sbwp.update(pd.Timestamp('2020-10-27 21:00:00', tz=pytz.UTC))

    assert round(port.total_cash_value_base,7) == 7792774.8370998 
    assert port.total_market_value_base == 6403909.93138
    assert port.total_equity_base == 14196684.768479826


    fx_order1 = get_order_mc('FX_ORDER', pd.Timestamp('2020-10-28 14:30:00', tz=pytz.UTC), 'EUR', -1500000, 0.0, 'USD', False)
    order1 = get_order_mc('STOCK_ORDER', pd.Timestamp('2020-10-28 14:30:00', tz=pytz.UTC), 'SPX', -1100, 0.0, 'USD', False)
    sbwp.submit_order("1234", fx_order1)
    sbwp.submit_order("1234", order1)
    sbwp.update(pd.Timestamp('2020-10-28 21:00:00', tz=pytz.UTC))


    port = sbwp.portfolios["1234"]
    assert port.total_cash_value_base == 11591837.370012645
    assert port.total_market_value_base == 2606107.291765
    assert port.total_equity_base == 14197944.661777645
    

    sbwp.update(pd.Timestamp('2020-10-29 21:00:00', tz=pytz.UTC))

    port = sbwp.portfolios["1234"]
    assert port.total_cash_value_base == 11588119.450463153
    assert port.total_market_value_base == 2589840.11397
    assert port.total_equity_base == 14177959.564433154