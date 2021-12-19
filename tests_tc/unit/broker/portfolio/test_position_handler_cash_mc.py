from collections import OrderedDict

import numpy as np
import pandas as pd
import pytz

from qstrader.broker.portfolio_mc.position_handler_cash_mc import PositionHandler_Cash_MC
import test_portfolio_helper as tph

def test_transact_position_new_position():

    ph = PositionHandler_Cash_MC()
    cash_transaction = tph.get_cash_leg_transaction(
        'USD',
        1.0,
        pd.Timestamp('2015-05-06 15:00:00', tz=pytz.UTC),
        10000,
        17.4
    )
    
    ph.transact_cash_position(cash_transaction)

    # Check that the position object is set correctly
    pos = ph.positions['USD']
    assert pos.buy_quantity == 10000
    assert pos.sell_quantity == 0
    assert pos.net_quantity == 10000
    assert pos.direction == 1
    assert pos.avg_price == 1.0017399999999999


def test_transact_position_current_position():
    """
    Tests the 'transact_position' method for a transaction
    with a current asset and checks that all objects are
    set correctly.
    """
    # Create the PositionHandler, Transaction and
    # carry out a transaction
    ph = PositionHandler_Cash_MC()
    
    cash_transaction_1 = tph.get_cash_leg_transaction(
        'USD',
        1.0,
        pd.Timestamp('2015-05-06 15:00:00', tz=pytz.UTC),
        10000,
        17.4
    )

    cash_transaction_2 = tph.get_cash_leg_transaction(
        'USD',
        1.0,
        pd.Timestamp('2015-05-06 16:00:00', tz=pytz.UTC),
        5000,
        6.4
    )

    ph.transact_cash_position(cash_transaction_1)
    ph.transact_cash_position(cash_transaction_2)

    # Check that the position object is set correctly
    pos = ph.positions['USD']
    assert pos.buy_quantity == 15000
    assert pos.sell_quantity == 0
    assert pos.net_quantity == 15000
    assert pos.direction == 1
    assert np.isclose(pos.avg_price, 1.0015866666666666)


def test_transact_position_quantity_zero():
    """
    Tests the 'transact_position' method for a transaction
    with net zero quantity after the transaction to ensure
    deletion of the position.
    """
    # Create the PositionHandler, Transaction and
    # carry out a transaction
    ph = PositionHandler_Cash_MC()
    cash_transaction_open = tph.get_cash_leg_transaction(
        'USD',
        1.0,
        pd.Timestamp('2015-05-06 15:00:00', tz=pytz.UTC),
        10000,
        17.4
    )

    cash_transaction_close = tph.get_cash_leg_transaction(
        'USD',
        1.0,
        pd.Timestamp('2015-05-06 15:00:00', tz=pytz.UTC),
        -10000,
        15.2
    )

    ph.transact_cash_position(cash_transaction_open)
    ph.transact_cash_position(cash_transaction_close)

    # Go long and then close, then check that the
    # positions OrderedDict is empty
    assert ph.positions == OrderedDict()


def test_total_values_for_no_transactions():
    """
    Tests 'total_market_value', 'total_unrealised_pnl',
    'total_realised_pnl' and 'total_pnl' for the case
    of no transactions being carried out.
    """
    ph = PositionHandler_Cash_MC()
    assert ph.total_cash_market_value_base() == 0.0
    assert ph.total_cash_unrealised_pnl_base() == 0.0
    #assert ph.total_realised_pnl_local() == 0.0
    #assert ph.total_pnl_local() == 0.0


## Need to fix this.  issue with commissions for fx transactions, should they apply as local or base, realised or unrealised
## Fix this when you fix financing charges etc too
def test_total_values_for_two_separate_transactions():
    """
    Tests 'total_market_value', 'total_unrealised_pnl',
    'total_realised_pnl' and 'total_pnl' for single
    transactions in two separate assets.
    """
    ph = PositionHandler_Cash_MC()

    cash_transaction_1 = tph.get_cash_leg_transaction(
        'USD',
        1.0,
        pd.Timestamp('2015-05-06 15:00:00', tz=pytz.UTC),
        10000,
        17.4
    )

    cash_transaction_2 = tph.get_cash_leg_transaction(
        'GBP',
        1.45,
        pd.Timestamp('2015-05-06 15:00:00', tz=pytz.UTC),
        22000,
        22.4
    )

    ph.transact_cash_position(cash_transaction_1)
    ph.transact_cash_position(cash_transaction_2)

    # Check all total values
    assert ph.total_cash_market_value_local('GBP') == 22000
    assert ph.total_cash_market_value_local('USD') == 10000
    assert ph.total_cash_market_value_base() == 41900.0
    assert np.isclose(ph.total_cash_unrealised_pnl_base(), -39.8) #49.88 is fx'ing GBP unrealised. 39.8 is not i.e the commissions
    assert ph.total_cash_realised_pnl_local('GBP') == 0.0
    assert ph.total_cash_realised_pnl_local('USD') == 0.0
    assert np.isclose(ph.total_cash_pnl_local('GBP'), 0.0)
    assert np.isclose(ph.total_cash_pnl_local('USD'), 0.0)