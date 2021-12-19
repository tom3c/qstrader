import sys
sys.path.append('T:\Projects_Code\_My_Work\_Strategy_Analysis\qstrader')

import pandas as pd
import pytz
import pytest

from qstrader.broker.portfolio_mc.portfolio_mc import Portfolio_MC
from qstrader.broker.portfolio_mc.portfolio_event_mc import PortfolioEvent_MC
from qstrader.broker.transaction.transaction_mc import Transaction_MC
import test_portfolio_helper as tph


def test_initial_settings_for_default_multi_currency_portfolio():
    """
    Test that the initial settings are as they should be
    for two specified portfolios.
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)

    # Test a default Portfolio
    port1 = Portfolio_MC(start_dt)
    assert port1.start_dt == start_dt
    assert port1.current_dt == start_dt
    assert port1.base_currency == "USD"
    assert port1.starting_cash == 0.0
    assert port1.portfolio_id is None
    assert port1.name is None
    assert port1.total_market_value_local('USD') == 0.0
    assert port1.total_market_value_base == 0.0
    assert port1.total_cash_value_local('USD') == 0.0
    assert port1.total_equity_base == 0.0

    # Test a Portfolio with keyword arguments
    port2 = Portfolio_MC(
        start_dt, starting_cash=1234567.56, base_currency="USD",
        portfolio_id=12345, name="My Second Test Portfolio"
    )
    assert port2.start_dt == start_dt
    assert port2.current_dt == start_dt
    assert port2.base_currency == "USD"
    assert port2.starting_cash == 1234567.56
    assert port2.portfolio_id == 12345
    assert port2.name == "My Second Test Portfolio"
    assert port2.total_market_value_base == 0.0
    assert port2.total_cash_value_base == 1234567.56
    assert port2.total_equity_base == 1234567.56


def test_multi_currency_portfolio_currency_settings():
    """
    Test that USD and GBP currencies are correctly set with
    some currency keyword arguments and that the currency
    formatter produces the correct strings.
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)

    # Test a US portfolio produces correct values
    cur1 = "USD"
    port1 = Portfolio_MC(start_dt, base_currency=cur1)
    assert port1.base_currency == "USD"

    # Test a UK portfolio produces correct values
    cur2 = "GBP"
    port2 = Portfolio_MC(start_dt, base_currency=cur2)
    assert port2.base_currency == "GBP"


def test_multi_currency_subscribe_funds_behaviour():
    """
    Test subscribe_funds raises for incorrect datetime
    Test subscribe_funds raises for negative amount
    Test subscribe_funds correctly adds positive
    amount, generates correct event and modifies time
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)
    earlier_dt = pd.Timestamp('2017-10-04 08:00:00', tz=pytz.UTC)
    later_dt = pd.Timestamp('2017-10-06 08:00:00', tz=pytz.UTC)
    pos_cash = 1000.0
    neg_cash = -1000.0
    port = Portfolio_MC(start_dt, starting_cash=2000.0)

    # Test subscribe_funds raises for incorrect datetime
    with pytest.raises(ValueError):
        port.subscribe_funds(earlier_dt, pos_cash)

    # Test subscribe_funds raises for negative amount
    with pytest.raises(ValueError):
        port.subscribe_funds(start_dt, neg_cash)

    # Test subscribe_funds correctly adds positive
    # amount, generates correct event and modifies time
    port.subscribe_funds(later_dt, pos_cash)

    assert port.total_cash_value_base == 3000.0
    assert port.total_cash_value_local('USD') == 3000.0
    assert port.total_market_value_base == 0.0
    assert port.total_market_value_local('USD') == 0.0
    assert port.total_equity_base == 3000.0
    assert port.total_equity_local('USD') == 3000.0

    pe1 = PortfolioEvent_MC(
        dt=start_dt, type='subscription',
        description="SUBSCRIPTION",
        currency= 'USD',
        debit=0.0,
        credit=2000.0, balance=2000.0
    )
    pe2 = PortfolioEvent_MC(
        dt=later_dt, type='subscription',
        description="SUBSCRIPTION",
        currency= 'USD',
        debit=0.0,
        credit=1000.0, balance=3000.0
    )

    assert port.history == [pe1, pe2]
    assert port.current_dt == later_dt


def test_multi_currency_subscribe_funds_behaviour_two_currencies():
    """
    Test subscribe_funds raises for incorrect datetime
    Test subscribe_funds raises for negative amount
    Test subscribe_funds correctly adds positive
    amount, generates correct event and modifies time
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)
    earlier_dt = pd.Timestamp('2017-10-04 08:00:00', tz=pytz.UTC)
    later_dt = pd.Timestamp('2017-10-06 08:00:00', tz=pytz.UTC)
    pos_cash = 1000.0
    neg_cash = -1000.0
    pos_cash_hkd = 10000.0
    neg_cash_hkd = -10000.0

    port = Portfolio_MC(start_dt, starting_cash=2000.0, base_currency='HKD')

    # Test subscribe_funds raises for incorrect datetime
    with pytest.raises(ValueError):
        port.subscribe_funds(earlier_dt, pos_cash)

    # Test subscribe_funds raises for negative amount
    with pytest.raises(ValueError):
        port.subscribe_funds(start_dt, neg_cash)

    # Test subscribe_funds correctly adds positive
    # amount, generates correct event and modifies time
    port.subscribe_funds(later_dt, pos_cash)

    assert port.total_cash_value_base == 3000.0
    assert port.total_cash_value_local('HKD') == 3000.0
    assert port.total_market_value_base == 0.0
    assert port.total_market_value_local('HKD') == 0.0
    assert port.total_equity_base == 3000.0
    assert port.total_equity_local('HKD') == 3000.0


    # Test subscribe_funds raises for negative amount
    with pytest.raises(ValueError):
        port.subscribe_funds(later_dt, neg_cash_hkd)

    port.subscribe_funds(later_dt, pos_cash_hkd)

    assert port.total_cash_value_base == 13000.0
    assert port.total_cash_value_local('HKD') == 13000.0
    assert port.total_market_value_base == 0.0
    assert port.total_market_value_local('HKD') == 0.0
    assert port.total_equity_base == 13000.0
    assert port.total_equity_local('HKD') == 13000.0

    pe1 = PortfolioEvent_MC(
        dt=start_dt, type='subscription',
        description="SUBSCRIPTION",
        currency='HKD',
        debit=0.0,
        credit=2000.0, balance=2000.0
    )
    pe2 = PortfolioEvent_MC(
        dt=later_dt, type='subscription',
        description="SUBSCRIPTION",
        currency='HKD',
        debit=0.0,
        credit=1000.0, balance=3000.0
    )

    pe3 = PortfolioEvent_MC(
        dt=later_dt, type='subscription',
        description="SUBSCRIPTION",
        currency='HKD',
        debit=0.0,
        credit=10000.0, balance=13000.0
    )

    assert port.history == [pe1, pe2, pe3]
    assert port.current_dt == later_dt


def test_withdraw_funds_behaviour():
    """
    Test withdraw_funds raises for incorrect datetime
    Test withdraw_funds raises for negative amount
    Test withdraw_funds raises for lack of cash
    Test withdraw_funds correctly subtracts positive
    amount, generates correct event and modifies time
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)
    earlier_dt = pd.Timestamp('2017-10-04 08:00:00', tz=pytz.UTC)
    later_dt = pd.Timestamp('2017-10-06 08:00:00', tz=pytz.UTC)
    even_later_dt = pd.Timestamp('2017-10-07 08:00:00', tz=pytz.UTC)
    pos_cash = 1000.0
    neg_cash = -1000.0
    port_raise = Portfolio_MC(start_dt)

    # Test withdraw_funds raises for incorrect datetime
    with pytest.raises(ValueError):
        port_raise.withdraw_funds(earlier_dt, pos_cash)

    # Test withdraw_funds raises for negative amount
    with pytest.raises(ValueError):
        port_raise.withdraw_funds(start_dt, neg_cash)

    # Test withdraw_funds raises for not enough cash
    port_broke = Portfolio_MC(start_dt)
    port_broke.subscribe_funds(later_dt, 1000.0)

    with pytest.raises(ValueError):
        port_broke.withdraw_funds(later_dt, 2000.0)

    # Test withdraw_funds correctly subtracts positive
    # amount, generates correct event and modifies time
    # Initial subscribe
    port_cor = Portfolio_MC(start_dt)
    port_cor.subscribe_funds(later_dt, pos_cash)
    pe_sub = PortfolioEvent_MC(
        dt=later_dt, type='subscription',
        description="SUBSCRIPTION", 
        currency='USD',
        debit=0.0,
        credit=1000.0, 
        balance=1000.0
    )

    assert port_cor.total_cash_value_base == 1000.0
    assert port_cor.total_cash_value_local('USD') == 1000.0
    assert port_cor.total_market_value_base == 0.0
    assert port_cor.total_market_value_local('USD') == 0.0
    assert port_cor.total_equity_base == 1000.0
    assert port_cor.total_equity_local('USD') == 1000.0
    assert port_cor.history == [pe_sub]
    assert port_cor.current_dt == later_dt


    # Now withdraw
    port_cor.withdraw_funds(even_later_dt, 468.0)
    pe_wdr = PortfolioEvent_MC(
        dt=even_later_dt, type='withdrawal',
        description="WITHDRAWAL",
        currency='USD',
        debit=468.0,
        credit=0.0, 
        balance=532.0
    )

    assert port_cor.total_cash_value_base == 532.0
    assert port_cor.total_cash_value_local('USD') == 532.0
    assert port_cor.total_market_value_base == 0.0
    assert port_cor.total_market_value_local('USD') == 0.0
    assert port_cor.total_equity_base == 532.0
    assert port_cor.total_equity_local('USD') == 532.0
    assert port_cor.history == [pe_sub, pe_wdr]
    assert port_cor.current_dt == even_later_dt


def test_transact_asset_behaviour():
    """
    Test transact_asset raises for incorrect time
    Test correct total_cash and total_securities_value
    for correct transaction (commission etc), correct
    portfolio event and correct time update
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)
    earlier_dt = pd.Timestamp('2017-10-04 08:00:00', tz=pytz.UTC)
    later_dt = pd.Timestamp('2017-10-06 08:00:00', tz=pytz.UTC)
    even_later_dt = pd.Timestamp('2017-10-07 08:00:00', tz=pytz.UTC)
    port = Portfolio_MC(start_dt)
    
    
    type = "STOCK_TRANSACTION"
    asset = 'EQ:AAA'

    # Test transact_asset raises for incorrect time

    tn_early = tph.get_transaction_mc(
       "STOCK_TRANSACTION",
       'EQ:AAA', 
       'USD',
       567.0,
       1.0,
       pd.Timestamp('2017-10-04 08:00:00', tz=pytz.UTC),
       100.0,
       0.0
    )

    with pytest.raises(ValueError):
        port.transact_asset(tn_early)

    # Test transact_asset raises for transaction total
    # cost exceeding total cash
    port.subscribe_funds(later_dt, 1000.0)

    assert port.total_cash_value_base == 1000.0
    assert port.total_cash_value_local('USD') == 1000.0
    assert port.total_market_value_base == 0.0
    assert port.total_market_value_local('USD') == 0.0
    assert port.total_equity_base == 1000.0
    assert port.total_equity_local('USD') == 1000.0

    pe_sub1 = PortfolioEvent_MC(
        dt=later_dt, type='subscription',
        description="SUBSCRIPTION",
        currency = 'USD',
        debit=0.0,
        credit=1000.0, 
        balance=1000.0
    )

    # Test correct total_cash and total_securities_value
    # for correct transaction (commission etc), correct
    # portfolio event and correct time update
    port.subscribe_funds(even_later_dt, 99000.0)

    assert port.total_cash_value_base == 100000.0
    assert port.total_cash_value_local('USD') == 100000.0
    assert port.total_market_value_base == 0.0
    assert port.total_market_value_local('USD') == 0.0
    assert port.total_equity_base == 100000.0
    assert port.total_equity_local('USD') == 100000.0

    pe_sub2 = PortfolioEvent_MC(
        dt=even_later_dt, type='subscription',
        description="SUBSCRIPTION",
        currency = 'USD', 
        debit=0.0,
        credit=99000.0, 
        balance=100000.0
    )

    tn_even_later = tph.get_transaction_mc(
       "STOCK_TRANSACTION",
       'EQ:AAA', 
       'USD',
       567.0,
       1.0,
       pd.Timestamp('2017-10-07 08:00:00', tz=pytz.UTC),
       100.0,
       15.78
    )

    port.transact_asset(tn_even_later)

    assert port.total_cash_value_base == 43284.22
    assert port.total_cash_value_local('USD') == 43284.22
    assert port.total_market_value_base == 56700.00
    assert port.total_market_value_local('USD') == 56700.00
    assert port.total_equity_base == 99984.22
    assert port.total_equity_local('USD') == 99984.22

    description = "LONG 100.0 EQ:AAA 567.00 USD 07/10/2017"
    pe_tn = PortfolioEvent_MC(
        dt=even_later_dt, type="asset_transaction",
        description=description, 
        currency = 'USD',
        debit=56715.78,
        credit=0.0, balance=43284.22
    )

    assert port.history == [pe_sub1, pe_sub2, pe_tn]
    assert port.current_dt == even_later_dt


def test_transact_asset_behaviour_two_different_currency_assets_no_auto_fx():

    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)
    dt_2 = pd.Timestamp('2017-10-06 08:00:00', tz=pytz.UTC)
    dt_3 = pd.Timestamp('2017-10-07 08:00:00', tz=pytz.UTC)
    dt_4 = pd.Timestamp('2017-10-07 08:00:00', tz=pytz.UTC)
    
    port = Portfolio_MC(start_dt)
    port.subscribe_funds(start_dt, 200000.0)

    assert port.total_cash_value_base == 200000.0
    assert port.total_cash_value_local('USD') == 200000.0
    assert port.total_market_value_base == 0.0
    assert port.total_market_value_local('USD') == 0.0
    assert port.total_equity_base == 200000.0
    assert port.total_equity_local('USD') == 200000.0

    pe_Usd = PortfolioEvent_MC(
        dt=start_dt, type='subscription',
        description="SUBSCRIPTION", 
        currency = 'USD',
        debit=0.0,
        credit=200000.0, balance=200000.0
    )

    #Now carry out fx
    tn_fx = tph.get_transaction_mc(
       "FX_TRANSACTION",
       'EUR', 
       'USD',
       1.24,
       1.0,
       dt_2,
       75000,
       0.0
    )

    port.transact_asset(tn_fx)

    assert port.total_cash_value_base == 200000
    assert port.total_cash_value_local('USD') == 107000.0
    assert port.total_cash_value_local('EUR') == 75000.0
    assert port.total_market_value_base == 0.0
    assert port.total_market_value_local('USD') == 0.0
    assert port.total_equity_base == 200000.0
    assert port.total_equity_local('USD') == 107000.0

    description = "LONG 75000 EUR 1.24 USD 06/10/2017"
    pe_Eur_Usd_fx = PortfolioEvent_MC(
        dt=dt_2, type="asset_transaction",
        description=description, 
        currency = 'USD',
        debit=93000.0,
        credit=0.0, 
        balance=107000.0
    )

    # Test USD Stock transaction
    tn_usd_stock = tph.get_transaction_mc(
       "STOCK_TRANSACTION",
       'EQ:AAA', 
       'USD',
       567.0,
       1.0,
       dt_3,
       100,
       15.78
    )

    port.transact_asset(tn_usd_stock)

    assert port.total_cash_value_base == 143284.22
    assert port.total_cash_value_local('USD') == 50284.22
    assert port.total_cash_value_local('EUR') == 75000.0
    assert port.total_market_value_base == 56700.0
    assert port.total_market_value_local('USD') == 56700.0
    assert port.total_equity_base == 199984.22
    assert port.total_equity_local('USD') == 106984.22
    assert port.total_equity_local('EUR') == 75000


    description = "LONG 100 EQ:AAA 567.00 USD 07/10/2017"
    pe_tn_stock_usd = PortfolioEvent_MC(
        dt=dt_3, type="asset_transaction",
        description=description,
        currency = 'USD',
        debit=56715.78,
        credit=0.0,
        balance=50284.22
    )

    tn_eur_stock = tph.get_transaction_mc(
       "STOCK_TRANSACTION",
       'EQ:MSE', 
       'EUR',
       462.3,
       1.17,
       dt_4,
       50,
       7.64
    )
    port.transact_asset(tn_eur_stock)

    assert port.total_cash_value_base == 110980.7312
    assert port.total_cash_value_local('USD') == 50284.22
    assert port.total_cash_value_local('EUR') == 51877.36
    assert port.total_market_value_base == 83744.55
    assert port.total_market_value_local('USD') == 56700.0
    assert port.total_market_value_local('EUR') == 23115.0
    assert port.total_equity_base == 194725.2812
    assert port.total_equity_local('USD') == 106984.22
    assert port.total_equity_local('EUR') == 74992.36


    description = "LONG 50 EQ:MSE 462.30 EUR 07/10/2017"
    pe_tn_stock_eur = PortfolioEvent_MC(
        dt=dt_4, type="asset_transaction",
        description=description,
        currency = 'EUR', 
        debit=23122.64,
        credit=0.0, 
        balance=51877.36
    )

    ##Check actual currency balance
    assert port.portfolio_cash_to_dict()['USD']['quantity'] == 50284.22
    assert port.portfolio_cash_to_dict()['EUR']['quantity'] == 51877.36

    assert port.history == [pe_Usd, pe_Eur_Usd_fx, pe_tn_stock_usd, pe_tn_stock_eur]
    assert port.current_dt == dt_4


def test_portfolio_to_dict_empty_portfolio():
    """
    Test 'portfolio_to_dict' method for an empty Portfolio.
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)
    port = Portfolio_MC(start_dt)
    port_dict = port.portfolio_to_dict()
    assert port_dict == {}


def test_portfolio_to_dict_empty_portfolio_with_subscription():
    """
    Test 'portfolio_to_dict' method for an empty Portfolio.
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)
    port = Portfolio_MC(start_dt)
    port.subscribe_funds(start_dt, 100000.0)
    port_dict = port.portfolio_to_dict()

    test_holdings = {       
        'USD': {
            "quantity": 100000.0,
            "market_value_local": 100000.0,
            "market_value_base": 100000.0,
            "unrealised_pnl_local": 0.0,
            "unrealised_pnl_base": 0.0,
            "realised_pnl_local": 0.0,
            "total_pnl_local": 0.0,
        }
    }

    asset = 'USD'
    for key, val in test_holdings[asset].items():
        assert port_dict[asset][key] == pytest.approx(
            test_holdings[asset][key]
        )


def test_portfolio_to_dict_for_two_holdings():
    """
    Test portfolio_to_dict for two holdings.
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)
    asset1_dt = pd.Timestamp('2017-10-06 08:00:00', tz=pytz.UTC)
    asset2_dt = pd.Timestamp('2017-10-07 08:00:00', tz=pytz.UTC)
    update_dt = pd.Timestamp('2017-10-08 08:00:00', tz=pytz.UTC)
    asset1 = 'EQ:AAA'
    asset2 = 'EQ:BBB'

    port = Portfolio_MC(start_dt, portfolio_id='1234')
    port.subscribe_funds(start_dt, 100000.0)
    
    tn_asset1 = tph.get_transaction_mc(
       "STOCK_TRANSACTION",
       asset1, 
       'USD',
       567.0,
       1.0,
       asset1_dt,
       100,
       15.78
    )
    port.transact_asset(tn_asset1)

    tn_asset2 = tph.get_transaction_mc(
       "STOCK_TRANSACTION",
       asset2, 
       'USD',
       123.0,
       1.0,
       asset1_dt,
       100,
       7.64
    )
    port.transact_asset(tn_asset2)
    port.update_market_value_of_asset(asset2, 134.0, update_dt)
    
    test_holdings = {
        asset1: {
            "quantity": 100,
            "market_value_local": 56700.0,
            "market_value_base": 56700.0,
            "unrealised_pnl_local": -15.78,
            "unrealised_pnl_base": -15.78,
            "realised_pnl_local": 0.0,
            "total_pnl_local": -15.78
        },
        asset2: {
            "quantity": 100,
            "market_value_local": 13400.0,
            "market_value_base": 13400.0,
            "unrealised_pnl_local": 1092.3600000000006,
            "unrealised_pnl_base": 1092.3600000000006,
            "realised_pnl_local": 0.0,
            "total_pnl_local": 1092.3600000000006
        },
        'USD': {
            "quantity": 30976.58,
            "market_value_local": 30976.58,
            "market_value_base": 30976.58,
            "unrealised_pnl_local": 0.0,
            "unrealised_pnl_base": 0.0,
            "realised_pnl_local": 0.0,
            "total_pnl_local": 0.0,
        }

    }
    port_holdings = port.portfolio_to_dict()

    # This is needed because we're not using Decimal
    # datatypes and have to compare slightly differing
    # floating point representations
    for asset in (asset1, asset2, 'USD'):
        for key, val in test_holdings[asset].items():
            assert port_holdings[asset][key] == pytest.approx(
                test_holdings[asset][key]
            )


def test_portfolio_to_dict_for_two_holdings_with_different_currencies():
    """
    Test portfolio_to_dict for two holdings.
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)
    fx_dt = pd.Timestamp('2017-10-06 07:00:00', tz=pytz.UTC)
    asset1_dt = pd.Timestamp('2017-10-06 08:00:00', tz=pytz.UTC)
    asset2_dt = pd.Timestamp('2017-10-07 08:00:00', tz=pytz.UTC)
    update_dt = pd.Timestamp('2017-10-08 08:00:00', tz=pytz.UTC)
    asset1 = 'EQ:AAA'
    asset2 = 'EQ:BBB'

    port = Portfolio_MC(start_dt, portfolio_id='1234')
    port.subscribe_funds(start_dt, 100000.0)
    
    #Now carry out fx
    tn_fx = tph.get_transaction_mc(
       "FX_TRANSACTION",
       'EUR', 
       'USD',
       1.24,
       1.0,
       fx_dt,
       20000,
       0.0
    )

    port.transact_asset(tn_fx)

    tn_asset1 = tph.get_transaction_mc(
       "STOCK_TRANSACTION",
       asset1, 
       'USD',
       567.0,
       1.0,
       asset1_dt,
       100,
       15.78
    )
    port.transact_asset(tn_asset1)

    tn_asset2 = tph.get_transaction_mc(
       "STOCK_TRANSACTION",
       asset2, 
       'EUR',
       123.0,
       1.24,
       asset1_dt,
       100,
       7.64
    )
    port.transact_asset(tn_asset2)
    port.update_market_value_of_asset(asset2, 134.0, update_dt)
    
    test_holdings = {
        asset1: {
            "quantity": 100,
            "market_value_local": 56700.0,
            "market_value_base": 56700.0,
            "unrealised_pnl_local": -15.78,
            "unrealised_pnl_base": -15.78,
            "realised_pnl_local": 0.0,
            "total_pnl_local": -15.78
        },
        asset2: {
            "quantity": 100,
            "market_value_local": 13400.0,
            "market_value_base": 16616.0,
            "unrealised_pnl_local": 1092.3600000000006,
            "unrealised_pnl_base": 1354.5264,
            "realised_pnl_local": 0.0,
            "total_pnl_local": 1092.3600000000006
        },
        'USD': {
            "quantity": 18484.22,
            "market_value_local": 18484.22,
            "market_value_base": 18484.22,
            "unrealised_pnl_local": 0.0,
            "unrealised_pnl_base": 0.0,
            "realised_pnl_local": 0.0,
            "total_pnl_local": 0.0,
        },
        'EUR': {
            "quantity": 7692.36,
            "market_value_local": 7692.36,
            "market_value_base": 9538.5264,
            "unrealised_pnl_local": 0.0,
            "unrealised_pnl_base": 0.0,
            "realised_pnl_local": 0.0,
            "total_pnl_local": 0.0,
        }
    }
    port_holdings = port.portfolio_to_dict()

    # This is needed because we're not using Decimal
    # datatypes and have to compare slightly differing
    # floating point representations
    for asset in (asset1, asset2, 'USD', 'EUR'):
        for key, val in test_holdings[asset].items():
            assert port_holdings[asset][key] == pytest.approx(
                test_holdings[asset][key]
            )


    port.update_market_value_of_asset(asset1, 562.0, update_dt)
    port.update_market_value_of_asset(asset2, 131, update_dt)
    port.update_fx_rate_of_asset(asset2, 1.2, update_dt)
    port.update_fx_rate('EUR', 1.2, update_dt)

    test_holdings = {
        asset1: {
            "quantity": 100,
            "market_value_local": 56200.0,
            "market_value_base": 56200.0,
            "unrealised_pnl_local": -515.78,
            "unrealised_pnl_base": -515.78,
            "realised_pnl_local": 0.0,
            "total_pnl_local": -515.78
        },
        asset2: {
            "quantity": 100,
            "market_value_local": 13100.0,
            "market_value_base": 15720, 
            "unrealised_pnl_local": 792.3600000000006,
            "unrealised_pnl_base": 950.832,
            "realised_pnl_local": 0.0,
            "total_pnl_local": 792.3600000000006
        },
        'USD': {
            "quantity": 18484.22,
            "market_value_local": 18484.22,
            "market_value_base": 18484.22,
            "unrealised_pnl_local": 0.0,
            "unrealised_pnl_base": 0.0,
            "realised_pnl_local": 0.0,
            "total_pnl_local": 0.0,
        },
        'EUR': {
            "quantity": 7692.36,
            "market_value_local": 7692.36,
            "market_value_base": 9230.832,
            "unrealised_pnl_local": 0.0,
            "unrealised_pnl_base": -307.6944,
            "realised_pnl_local": 0.0,
            "total_pnl_local": 0.0,
        }
    }
    port_holdings = port.portfolio_to_dict()

    for asset in (asset1, asset2, 'USD', 'EUR'):
        for key, val in test_holdings[asset].items():
            assert port_holdings[asset][key] == pytest.approx(
                test_holdings[asset][key]
            )

def test_update_market_value_of_asset_not_in_list():
    """
    Test update_market_value_of_asset for asset not in list.
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)
    later_dt = pd.Timestamp('2017-10-06 08:00:00', tz=pytz.UTC)
    port = Portfolio_MC(start_dt)
    asset = 'EQ:AAA'
    update = port.update_market_value_of_asset(
        asset, 54.34, later_dt
    )
    assert update is None


def test_update_market_value_of_asset_negative_price():
    """
    Test update_market_value_of_asset for
    asset with negative price.
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)
    later_dt = pd.Timestamp('2017-10-06 08:00:00', tz=pytz.UTC)
    port = Portfolio_MC(start_dt)

    asset = 'EQ:AAA'
    port.subscribe_funds(later_dt, 100000.0)

    tn_asset = tph.get_transaction_mc(
       "STOCK_TRANSACTION",
       asset, 
       'USD',
       567.0,
       1.0,
       later_dt,
       100,
       15.78
    )

    port.transact_asset(tn_asset)
    with pytest.raises(ValueError):
        port.update_market_value_of_asset(
            asset, -54.34, later_dt
        )


def test_update_market_value_of_asset_earlier_date():
    """
    Test update_market_value_of_asset for asset
    with current_trade_date in past
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)
    earlier_dt = pd.Timestamp('2017-10-04 08:00:00', tz=pytz.UTC)
    later_dt = pd.Timestamp('2017-10-06 08:00:00', tz=pytz.UTC)
    port = Portfolio_MC(start_dt, portfolio_id='1234')

    asset = 'EQ:AAA'
    port.subscribe_funds(later_dt, 100000.0)

    tn_asset = tph.get_transaction_mc(
       "STOCK_TRANSACTION",
       asset, 
       'USD',
       567.0,
       1.0,
       later_dt,
       100,
       15.78
    )

    port.transact_asset(tn_asset)
    with pytest.raises(ValueError):
        port.update_market_value_of_asset(
            asset, 50.23, earlier_dt
        )


def test_history_to_df_empty():
    """
    Test 'history_to_df' with no events.
    """
    start_dt = pd.Timestamp('2017-10-05 08:00:00', tz=pytz.UTC)
    port = Portfolio_MC(start_dt)
    hist_df = port.history_to_df()
    test_df = pd.DataFrame(
        [], columns=[
            "date", "type", "description",
            "debit", "credit", "balance"
        ]
    )
    test_df.set_index(keys=["date"], inplace=True)
    assert sorted(test_df.columns) == sorted(hist_df.columns)
    assert len(test_df) == len(hist_df)
    assert len(hist_df) == 0
