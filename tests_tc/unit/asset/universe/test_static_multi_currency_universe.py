##TC##

import sys
sys.path.append('T:\Projects_Code\_My_Work\_Strategy_Analysis\qstrader')

import pandas as pd
import pytest
import pytz

from qstrader.asset.universe.static_multi_currency import StaticMultiCurrencyUniverse


@pytest.mark.parametrize(
    'assets,dt,expected_equity, expected_cash',
    [
        (
            [('EQ:SPY','USD'), ('EQ:MSE','EUR')],
            pd.Timestamp('2019-01-01 15:00:00', tz=pytz.utc),
            ['EQ:SPY', 'EQ:MSE'],
            ['EUR','USD']
        ),
        (
            [('EQ:GLD','USD'), ('EQ:MSE','EUR'), ('EQ:TLT','USD')],
            pd.Timestamp('2020-05-01 15:00:00', tz=pytz.utc),
            ['EQ:GLD', 'EQ:MSE', 'EQ:TLT'],
            ['EUR','USD']
        )
    ]
)
def test_static_multi_currency_universe(assets, dt, expected_equity,expected_cash):
    """
    Checks that the StaticUniverse correctly returns the
    list of assets for a particular datetime.
    """
    universe = StaticMultiCurrencyUniverse(assets)
    assert universe.get_equity_assets(dt) == expected_equity
    assert universe.get_cash_assets(dt) == expected_cash


@pytest.mark.parametrize(
    'equity_assets, cash_assets, dt,expected_equity, expected_cash',
    [
        (
            [('EQ:SPY','USD'), ('EQ:MSE','EUR')],
            ['HKD','AUD'],
            pd.Timestamp('2019-01-01 15:00:00', tz=pytz.utc),
            ['EQ:SPY', 'EQ:MSE'],
            ['AUD','EUR','HKD','USD']
        ),
        (
            [('EQ:GLD','USD'), ('EQ:MSE','EUR'), ('EQ:TLT','USD')],
            ['HKD','AUD'],
            pd.Timestamp('2020-05-01 15:00:00', tz=pytz.utc),
            ['EQ:GLD', 'EQ:MSE', 'EQ:TLT'],
            ['AUD','EUR','HKD','USD']
        )
    ]
)
def test_static_multi_currency_universe_with_cash(equity_assets, cash_assets, dt, expected_equity,expected_cash):
    """
    Checks that the StaticUniverse correctly returns the
    list of assets for a particular datetime.
    """
    universe = StaticMultiCurrencyUniverse(equity_assets, cash_assets)
    assert universe.get_equity_assets(dt) == expected_equity
    assert universe.get_cash_assets(dt) == expected_cash


@pytest.mark.parametrize(
    'assets,dt',
    [
        (
            [('EQ:GLD','USD'), ('EQ:MSE','EUR'), ('EQ:TLT','USD')],
            pd.Timestamp('2020-05-01 15:00:00', tz=pytz.utc)
        )
    ]
)
def test_static_multi_currency_universe_currencies(assets, dt):
    """
    Checks that the StaticUniverse correctly returns the
    list of assets for a particular datetime.
    """
    universe = StaticMultiCurrencyUniverse(assets)
    assert universe.get_equity_asset_currency('EQ:GLD') == 'USD'
    assert universe.get_equity_asset_currency('EQ:MSE') == 'EUR'
    assert universe.get_equity_asset_currency('EQ:TLT') == 'USD'