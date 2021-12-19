
from collections import OrderedDict

import numpy as np
import pandas as pd
import pytz

from qstrader.broker.portfolio_mc.position_handler_mc import PositionHandler_MC
import test_portfolio_helper as tph

def test_transact_position_new_position():
    """
    Tests the 'transact_position' method for a transaction
    with a brand new asset and checks that all objects are
    set correctly.
    """
    # Create the PositionHandler, Transaction and
    # carry out a transaction
    ph = PositionHandler_MC()
    stock_transaction = tph.get_stock_leg_transaction(
        'EQ:AMZN',
        'USD',
        960.0,
        1.0,
        pd.Timestamp('2015-05-06 15:00:00', tz=pytz.UTC),
        100,
        26.83
    )
    
    ph.transact_position(stock_transaction)

    # Check that the position object is set correctly
    pos = ph.positions['EQ:AMZN']
    assert pos.buy_quantity == 100
    assert pos.sell_quantity == 0
    assert pos.net_quantity == 100
    assert pos.direction == 1
    assert pos.avg_price == 960.2683000000001


def test_transact_position_current_position():
    """
    Tests the 'transact_position' method for a transaction
    with a current asset and checks that all objects are
    set correctly.
    """
    # Create the PositionHandler, Transaction and
    # carry out a transaction
    ph = PositionHandler_MC()
    
    stock_transaction_1 = tph.get_stock_leg_transaction(
        'EQ:AMZN',
        'USD',
        960.0,
        1.0,
        pd.Timestamp('2015-05-06 15:00:00', tz=pytz.UTC),
        100,
        26.83
    )

    stock_transaction_2 = tph.get_stock_leg_transaction(
        'EQ:AMZN',
        'USD',
        990.0,
        1.0,
        pd.Timestamp('2015-05-06 16:00:00', tz=pytz.UTC),
        200,
        18.53
    )

    ph.transact_position(stock_transaction_1)
    ph.transact_position(stock_transaction_2)

    # Check that the position object is set correctly
    pos = ph.positions['EQ:AMZN']
    assert pos.buy_quantity == 300
    assert pos.sell_quantity == 0
    assert pos.net_quantity == 300
    assert pos.direction == 1
    assert np.isclose(pos.avg_price, 980.1512)


def test_transact_position_quantity_zero():
    """
    Tests the 'transact_position' method for a transaction
    with net zero quantity after the transaction to ensure
    deletion of the position.
    """
    # Create the PositionHandler, Transaction and
    # carry out a transaction
    ph = PositionHandler_MC()
    stock_transaction_open = tph.get_stock_leg_transaction(
        'EQ:AMZN',
        'USD',
        960.0,
        1.0,
        pd.Timestamp('2015-05-06 15:00:00', tz=pytz.UTC),
        100,
        26.83
    )

    stock_transaction_close = tph.get_stock_leg_transaction(
        'EQ:AMZN',
        'USD',
        960.0,
        1.0,
        pd.Timestamp('2015-05-06 16:00:00', tz=pytz.UTC),
        -100,
        26.83
    )

    ph.transact_position(stock_transaction_open)
    ph.transact_position(stock_transaction_close)

    # Go long and then close, then check that the
    # positions OrderedDict is empty
    assert ph.positions == OrderedDict()


def test_total_values_for_no_transactions():
    """
    Tests 'total_market_value', 'total_unrealised_pnl',
    'total_realised_pnl' and 'total_pnl' for the case
    of no transactions being carried out.
    """
    ph = PositionHandler_MC()
    assert ph.total_market_value_base() == 0.0
    assert ph.total_unrealised_pnl_base() == 0.0
    #assert ph.total_realised_pnl_base() == 0.0
    #assert ph.total_pnl_base() == 0.0


def test_total_values_for_two_separate_transactions():
    """
    Tests 'total_market_value', 'total_unrealised_pnl',
    'total_realised_pnl' and 'total_pnl' for single
    transactions in two separate assets.
    """
    ph = PositionHandler_MC()

    stock_transaction_1 = tph.get_stock_leg_transaction(
        'EQ:AMZN',
        'USD',
        483.45,
        1.0,
        pd.Timestamp('2015-05-06 15:00:00', tz=pytz.UTC),
        75,
        15.97
    )

    stock_transaction_2 = tph.get_stock_leg_transaction(
        'EQ:MSFT',
        'USD',
        142.58,
        1.0,
        pd.Timestamp('2015-05-06 15:00:00', tz=pytz.UTC),
        250,
        8.35
    )

    ph.transact_position(stock_transaction_1)
    ph.transact_position(stock_transaction_2)

    # Check all total values
    assert ph.total_market_value_base() == 71903.75
    assert np.isclose(ph.total_unrealised_pnl_base(), -24.31999999999971)
    #assert ph.total_realised_pnl_base() == 0.0
    #assert np.isclose(ph.total_pnl_base(), -24.31999999999971)
