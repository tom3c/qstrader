import numpy as np
import pandas as pd
import pytest
import pytz

from qstrader.broker.portfolio_mc.position_mc import Position_MC
from qstrader.broker.transaction.transaction_mc import Transaction_MC


def test_basic_long_equities_position():
    """
    Tests that the properties on the Position
    are calculated for a simple long equities position.
    """
    # Initial long details
    asset = 'EQ:MSFT'
    type = "STOCK_TRANSACTION"
    quantity = 100
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 193.74
    currency = 'USD'
    fx_rate = 1.0
    order_id = 123
    commission = 1.0

    # Create the initial transaction and position
    transaction = Transaction_MC(
        type,
        asset,
        quantity,
        dt,
        price,
        currency,
        fx_rate,
        order_id,
        commission
    )
    position = Position_MC.open_from_transaction(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    # Update the market price
    new_market_price = 192.80
    new_dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    position.update_current_price(new_market_price, new_dt)

    assert position.current_price == new_market_price
    assert position.current_dt == new_dt

    assert position.buy_quantity == 100
    assert position.sell_quantity == 0
    assert position.avg_price_bought == 193.74
    assert position.avg_price_sold == 0.0
    assert position.commission_local == 1.0

    assert position.direction == 1
    assert position.market_value_local == 19280.0
    assert position.market_value_base == 19280.0
    assert position.avg_price == 193.75
    assert position.net_quantity == 100
    assert position.total_bought_local == 19374.0
    assert position.total_sold_local == 0.0
    assert position.net_total_local == -19374.0
    assert position.net_incl_commission_local == -19375.0
    assert np.isclose(position.unrealised_pnl_local, -94.99999999999886)
    assert np.isclose(position.unrealised_pnl_base, -94.99999999999886)
    assert np.isclose(position.realised_pnl_local, 0.0)




def test_basic_long_equities_position_with_fx():
    """
    Tests that the properties on the Position
    are calculated for a simple long equities position.
    """
    # Initial long details
    type = "STOCK_TRANSACTION"
    asset = 'EQ:MSFT'
    quantity = 100
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 193.74
    fx_rate = 0.75
    order_id = 123
    commission = 1.0

    # Create the initial transaction and position
    transaction = Transaction_MC(
        type,
        asset,
        quantity,
        dt,
        price,
        currency='USD',
        fx_rate = fx_rate,
        order_id = order_id,
        commission=commission
    )
    position = Position_MC.open_from_transaction(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    # Update the market price
    new_market_price = 192.80
    new_dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    position.update_current_price(new_market_price, new_dt)

    assert position.current_price == new_market_price
    assert position.current_dt == new_dt

    assert position.buy_quantity == 100
    assert position.sell_quantity == 0
    assert position.avg_price_bought == 193.74
    assert position.avg_price_sold == 0.0
    assert position.commission_local == 1.0

    assert position.direction == 1
    assert position.market_value_local == 19280.0
    assert position.market_value_base == 14460.0
    assert position.avg_price == 193.75
    assert position.net_quantity == 100
    assert position.total_bought_local == 19374.0
    assert position.total_sold_local == 0.0
    assert position.net_total_local == -19374.0
    assert position.net_incl_commission_local == -19375.0
    assert np.isclose(position.unrealised_pnl_local, -94.99999999999886)
    assert np.isclose(position.unrealised_pnl_base, -71.2499999999991)
    assert np.isclose(position.realised_pnl_local, 0.0)


def test_position_long_twice():
    """
    Tests that the properties on the Position
    are calculated for two consective long trades
    with differing quantities and market prices.
    """
    # Initial long details
    type = "STOCK_TRANSACTION"
    asset = 'EQ:MSFT'
    quantity = 100
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 193.74
    order_id = 123
    commission = 1.0

    # Create the initial transaction and position
    first_transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'EUR',
        fx_rate = 1.0,
        order_id=order_id,
        commission=commission
    )
    position = Position_MC.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    # Second long
    second_quantity = 60
    second_dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    second_price = 193.79
    second_order_id = 234
    second_commission = 1.0
    second_transaction = Transaction_MC(
        type,
        asset,
        quantity=second_quantity,
        dt=second_dt,
        price=second_price,
        currency = 'EUR',
        fx_rate = 1.0,
        order_id=second_order_id,
        commission=second_commission
    )
    position.transact(second_transaction)

    assert position.current_price == second_price
    assert position.current_dt == second_dt

    assert position.buy_quantity == 160
    assert position.sell_quantity == 0
    assert np.isclose(position.avg_price_bought, 193.75875)
    assert position.avg_price_sold == 0.0
    assert position.commission_local == 2.0

    assert position.direction == 1
    assert np.isclose(position.market_value_base, 31006.40)
    assert np.isclose(position.market_value_local, 31006.40)
    assert position.avg_price == 193.77125
    assert position.net_quantity == 160
    assert position.total_bought_local == 31001.40
    assert position.total_sold_local == 0.0
    assert position.net_total_local == -31001.40
    assert position.net_incl_commission_local == -31003.40
    assert np.isclose(position.unrealised_pnl_local, 3.0)
    assert np.isclose(position.unrealised_pnl_base, 3.0)
    assert np.isclose(position.realised_pnl_local, 0.0)



def test_position_long_twice_with_fx():
    """
    Tests that the properties on the Position
    are calculated for two consective long trades
    with differing quantities and market prices.
    """
    # Initial long details
    type = "STOCK_TRANSACTION"
    asset = 'EQ:MSFT'
    quantity = 100
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 193.74
    fx_rate = 0.75
    order_id = 123
    commission = 1.0

    # Create the initial transaction and position
    first_transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        order_id=order_id,
        currency = 'EUR',
        fx_rate = fx_rate,
        commission=commission
    )
    position = Position_MC.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    # Second long
    second_quantity = 60
    second_dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    second_price = 193.79
    second_order_id = 234
    second_fx_rate = 0.7
    second_commission = 1.0
    second_transaction = Transaction_MC(
        type,
        asset,
        quantity=second_quantity,
        dt=second_dt,
        price=second_price,
        order_id=second_order_id,
        currency = 'EUR',
        fx_rate = second_fx_rate,
        commission=second_commission
    )
    position.transact(second_transaction)

    assert position.current_price == second_price
    assert position.current_dt == second_dt

    assert position.buy_quantity == 160
    assert position.sell_quantity == 0
    assert np.isclose(position.avg_price_bought, 193.75875)
    assert position.avg_price_sold == 0.0
    assert position.commission_local == 2.0

    assert position.direction == 1
    assert np.isclose(position.market_value_local, 31006.40)
    assert np.isclose(position.market_value_base, 21704.479999999996)
    assert position.avg_price == 193.77125
    assert position.net_quantity == 160
    assert position.total_bought_local == 31001.40
    assert position.total_sold_local == 0.0
    assert position.net_total_local == -31001.40
    assert position.net_incl_commission_local == -31003.40
    assert np.isclose(position.unrealised_pnl_local, 3.0)
    assert np.isclose(position.unrealised_pnl_base, 2.1)
    assert np.isclose(position.realised_pnl_local, 0.0)


def test_position_long_close():
    """
    Tests that the properties on the Position
    are calculated for a long opening trade and
    subsequent closing trade.
    """
    # Initial long details
    type = "STOCK_TRANSACTION"
    asset = 'EQ:AMZN'
    quantity = 100
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 2615.27
    order_id = 123
    commission = 1.0

    # Create the initial transaction and position
    first_transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'EUR',
        fx_rate = 1.0,
        order_id=order_id,
        commission=commission
    )
    position = Position_MC.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    # Closing trade
    second_quantity = -100
    second_dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    second_price = 2622.0
    second_order_id = 234
    second_commission = 6.81
    second_transaction = Transaction_MC(
        type,
        asset,
        quantity=second_quantity,
        dt=second_dt,
        price=second_price,
        currency = 'EUR',
        fx_rate = 1.0,
        order_id=second_order_id,
        commission=second_commission
    )
    position.transact(second_transaction)

    assert position.current_price == second_price
    assert position.current_dt == second_dt

    assert position.buy_quantity == 100
    assert position.sell_quantity == 100
    assert position.avg_price_bought == 2615.27
    assert position.avg_price_sold == 2622.0
    assert position.commission_local == 7.81

    assert position.direction == 0
    assert position.market_value_local == 0.0
    assert position.avg_price == 0.0
    assert position.net_quantity == 0
    assert position.total_bought_local == 261527.0
    assert position.total_sold_local == 262200.0
    assert position.net_total_local == 673.0
    assert position.net_incl_commission_local == 665.19
    assert position.unrealised_pnl_local == 0.0
    assert position.realised_pnl_local == 665.19

def test_position_long_close_with_fx():
    """
    Tests that the properties on the Position
    are calculated for a long opening trade and
    subsequent closing trade.
    """
    # Initial long details
    type = "STOCK_TRANSACTION"
    asset = 'EQ:AMZN'
    quantity = 100
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 2615.27
    order_id = 123
    fx_rate = 1.25
    commission = 1.0

    # Create the initial transaction and position
    first_transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        order_id=order_id,
        currency = 'EUR',
        fx_rate = fx_rate,      
        commission=commission
    )
    position = Position_MC.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt
    assert position.market_value_local == 261527.0
    assert position.market_value_base == 326908.75

    # Closing trade
    second_quantity = -100
    second_dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    second_price = 2622.0
    second_order_id = 234
    second_fx_rate = 1.15
    second_commission = 6.81
    second_transaction = Transaction_MC(
        type,
        asset,
        quantity=second_quantity,
        dt=second_dt,
        price=second_price,
        currency = 'EUR',
        fx_rate = second_fx_rate,
        order_id=second_order_id,
        commission=second_commission
    )
    position.transact(second_transaction)

    assert position.current_price == second_price
    assert position.current_dt == second_dt

    assert position.buy_quantity == 100
    assert position.sell_quantity == 100
    assert position.avg_price_bought == 2615.27
    assert position.avg_price_sold == 2622.0
    assert position.commission_local == 7.81

    assert position.direction == 0
    assert position.market_value_local == 0.0
    assert position.market_value_base == 0.0
    assert position.avg_price == 0.0
    assert position.net_quantity == 0
    assert position.total_bought_local == 261527.0
    assert position.total_sold_local == 262200.0
    assert position.net_total_local == 673.0
    assert position.net_incl_commission_local == 665.19
    assert position.unrealised_pnl_local == 0.0
    assert position.realised_pnl_local == 665.19



def test_position_long_and_short():
    """
    Tests that the properties on the Position
    are calculated for a long trade followed by
    a partial closing short trade with differing
    market prices.
    """
    # Initial long details
    type = "STOCK_TRANSACTION"
    asset = 'EQ:SPY'
    quantity = 100
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 307.05
    order_id = 123
    commission = 1.0

    # Create the initial transaction and position
    first_transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'EUR',
        fx_rate=1.0,
        order_id=order_id,
        commission=commission
    )
    position = Position_MC.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    # Short details and transaction
    second_quantity = -60
    second_dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    second_price = 314.91
    second_order_id = 234
    second_commission = 1.42
    second_transaction = Transaction_MC(
        type,
        asset,
        quantity=second_quantity,
        dt=second_dt,
        price=second_price,
        currency = 'EUR',
        fx_rate=1.0,
        order_id=second_order_id,
        commission=second_commission
    )
    position.transact(second_transaction)

    assert position.current_price == second_price
    assert position.current_dt == second_dt

    assert position.buy_quantity == 100
    assert position.sell_quantity == 60
    assert position.avg_price_bought == 307.05
    assert position.avg_price_sold == 314.91
    assert position.commission_local == 2.42

    assert position.direction == 1
    assert np.isclose(position.market_value_local, 12596.40)
    assert position.avg_price == 307.06
    assert position.net_quantity == 40
    assert position.total_bought_local == 30705.0
    assert np.isclose(position.total_sold_local, 18894.60)
    assert np.isclose(position.net_total_local, -11810.40)
    assert np.isclose(position.net_incl_commission_local, -11812.82)
    assert np.isclose(position.unrealised_pnl_local, 314.0)
    assert np.isclose(position.realised_pnl_local, 469.58)


def test_position_long_and_short_with_fx():
    """
    Tests that the properties on the Position
    are calculated for a long trade followed by
    a partial closing short trade with differing
    market prices.
    """
    # Initial long details
    type = "STOCK_TRANSACTION"
    asset = 'EQ:SPY'
    quantity = 100
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 307.05
    order_id = 123
    fx_rate = 1.10
    commission = 1.0

    # Create the initial transaction and position
    first_transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        order_id=order_id,
        currency = 'EUR',
        fx_rate=fx_rate,
        commission=commission
    )
    position = Position_MC.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt
    assert np.isclose(position.market_value_local, 30705.0)
    assert np.isclose(position.market_value_base, 33775.5)

    # Short details and transaction
    second_quantity = -60
    second_dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    second_price = 314.91
    second_order_id = 234
    second_fx_rate = 1.05
    second_commission = 1.42
    second_transaction = Transaction_MC(
        type,
        asset,
        quantity=second_quantity,
        dt=second_dt,
        price=second_price,
        order_id=second_order_id,
        currency = 'EUR',
        fx_rate = second_fx_rate,
        commission=second_commission
    )
    position.transact(second_transaction)

    assert position.current_price == second_price
    assert position.current_dt == second_dt

    assert position.buy_quantity == 100
    assert position.sell_quantity == 60
    assert position.avg_price_bought == 307.05
    assert position.avg_price_sold == 314.91
    assert position.commission_local == 2.42

    assert position.direction == 1
    assert np.isclose(position.market_value_local, 12596.40)
    assert np.isclose(position.market_value_base, 13226.22)
    assert position.avg_price == 307.06
    assert position.net_quantity == 40
    assert position.total_bought_local == 30705.0
    assert np.isclose(position.total_sold_local, 18894.60)
    assert np.isclose(position.net_total_local, -11810.40)
    assert np.isclose(position.net_incl_commission_local, -11812.82)
    assert np.isclose(position.unrealised_pnl_local, 314.0)
    assert np.isclose(position.unrealised_pnl_base, 329.7)
    assert np.isclose(position.realised_pnl_local, 469.58)


def test_position_long_short_long_short_ending_long():
    """
    Tests that the properties on the Position
    are calculated for four trades consisting
    of a long, short, long and short, net long
    after all trades with varying quantities
    and market prices.
    """
    # First trade (first long)
    type = "STOCK_TRANSACTION"
    asset = 'EQ:SPY'
    quantity = 453
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 312.96
    order_id = 100
    commission = 1.95

    # Create the initial transaction and position
    first_transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'EUR',
        fx_rate = 1.0,
        order_id=order_id,
        commission=commission
    )
    position = Position_MC.open_from_transaction(first_transaction)

    # Second trade (first short)
    quantity = -397
    dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    price = 315.599924
    order_id = 101
    commission = 4.8
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'EUR',
        fx_rate = 1.0,
        order_id=order_id,
        commission=commission
    )
    position.transact(transaction)

    # Third trade (second long)
    quantity = 624
    dt = pd.Timestamp('2020-06-16 17:00:00', tz=pytz.UTC)
    price = 312.96
    order_id = 102
    commission = 2.68
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'EUR',
        fx_rate = 1.0,
        order_id=order_id,
        commission=commission
    )
    position.transact(transaction)

    # Fourth trade (second short), now net long
    quantity = -519
    dt = pd.Timestamp('2020-06-16 18:00:00', tz=pytz.UTC)
    price = 315.78
    order_id = 103
    commission = 6.28
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'EUR',
        fx_rate = 1.0,
        order_id=order_id,
        commission=commission
    )
    position.transact(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 1077
    assert position.sell_quantity == 916
    assert position.avg_price_bought == 312.96
    assert position.avg_price_sold == 315.70195396069863
    assert position.commission_local == 15.71

    assert position.direction == 1
    assert np.isclose(position.market_value_local, 50840.58)
    assert np.isclose(position.market_value_base, 50840.58)
    assert position.avg_price == 312.96429897864436
    assert position.net_quantity == 161
    assert position.total_bought_local == 337057.92
    assert np.isclose(position.total_sold_local, 289182.99)
    assert np.isclose(position.net_total_local, -47874.93)
    assert np.isclose(position.net_incl_commission_local, -47890.64)
    assert np.isclose(position.unrealised_pnl_local, 453.327864438)
    assert np.isclose(position.unrealised_pnl_base, 453.327864438)
    assert np.isclose(position.realised_pnl_local, 2496.61)


def test_position_long_short_long_short_close_with_fx():

    # First trade (first long)
    type = "STOCK_TRANSACTION"
    asset = 'EQ:SPY'
    quantity = 353
    dt = pd.Timestamp('2020-06-16 14:00:00', tz=pytz.UTC)
    price = 312.96
    order_id = 100
    fx_rate = 0.65
    commission = 1.95

    # Create the initial transaction and position
    first_transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        order_id=order_id,
        currency = 'EUR',
        fx_rate = fx_rate,
        commission=commission
    )
    position = Position_MC.open_from_transaction(first_transaction)


    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 353
    assert position.sell_quantity == 0
    assert position.avg_price_bought == 312.96
    assert position.avg_price_sold == 0
    assert position.commission_local == 1.95

    assert position.direction == 1
    assert np.isclose(position.market_value_local, 110474.88)
    assert np.isclose(position.market_value_base, 71808.672)
    assert position.avg_price == 312.96552407932006
    assert position.net_quantity == 353
    assert position.total_bought_local == 110474.87999999999
    assert np.isclose(position.total_sold_local, 0.0)
    assert np.isclose(position.net_total_local, -110474.87999999999)
    assert np.isclose(position.net_incl_commission_local, -110476.82999999999)
    assert np.isclose(position.unrealised_pnl_local, -1.9499999999890747)
    assert np.isclose(position.unrealised_pnl_base, -1.2674999999928986)
    assert np.isclose(position.realised_pnl_local, 0.0)


    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 311.23
    position.update_current_price(price, dt)
    fx = 0.71
    position.update_current_fx(fx,dt)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 353
    assert position.sell_quantity == 0
    assert position.avg_price_bought == 312.96
    assert position.avg_price_sold == 0
    assert position.commission_local == 1.95

    assert position.direction == 1
    assert np.isclose(position.market_value_local, 109864.19)
    assert np.isclose(position.market_value_base, 78003.5749)
    assert position.avg_price == 312.96552407932006
    assert position.net_quantity == 353
    assert position.total_bought_local == 110474.87999999999
    assert np.isclose(position.total_sold_local, 0.0)
    assert np.isclose(position.net_total_local, -110474.87999999999)
    assert np.isclose(position.net_incl_commission_local, -110476.82999999999)
    assert np.isclose(position.unrealised_pnl_local, -612.64)
    assert np.isclose(position.unrealised_pnl_base, -434.974)
    assert np.isclose(position.realised_pnl_local, 0.0)



    # Second trade (first short)
    quantity = -397
    dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    price = 313.4
    order_id = 101
    fx_rate = 0.75
    commission = 4.8
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        order_id=order_id,
        currency = 'EUR',
        fx_rate=fx_rate,
        commission=commission
    )
    position.transact(transaction)


    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 353
    assert position.sell_quantity == 397
    assert position.avg_price_bought == 312.96
    assert position.avg_price_sold == 313.4
    assert position.commission_local == 6.75

    assert position.direction == -1
    assert np.isclose(position.market_value_local, -13789.6)
    assert np.isclose(position.market_value_base, -10342.2)
    assert position.avg_price == 313.3879093198992
    assert position.net_quantity == -44.0
    assert position.total_bought_local == 110474.87999999999
    assert np.isclose(position.total_sold_local, 124419.8)
    assert np.isclose(position.net_total_local, 13944.92)
    assert np.isclose(position.net_incl_commission_local, 13938.17)
    assert np.isclose(position.unrealised_pnl_local, -0.531989924443224)
    assert np.isclose(position.unrealised_pnl_base, -0.398992443332418)
    assert np.isclose(position.realised_pnl_local, 149.101989924432)


    dt = pd.Timestamp('2020-06-16 17:00:00', tz=pytz.UTC)
    price = 316.35
    position.update_current_price(price, dt)
    fx = 0.63
    position.update_current_fx(fx,dt)


    ## Only thing that should change is unrealised P&L
    assert position.buy_quantity == 353
    assert position.sell_quantity == 397
    assert position.avg_price_bought == 312.96
    assert position.avg_price_sold == 313.4
    assert position.commission_local == 6.75
    assert position.direction == -1
    assert np.isclose(position.market_value_local, -13919.4)
    assert np.isclose(position.market_value_base, -8769.222)
    assert position.avg_price == 313.3879093198992
    assert position.net_quantity == -44.0
    assert position.total_bought_local == 110474.87999999999
    assert np.isclose(position.total_sold_local, 124419.8)
    assert np.isclose(position.net_total_local, 13944.92)
    assert np.isclose(position.net_incl_commission_local, 13938.17)
    assert np.isclose(position.unrealised_pnl_local, -130.3319)
    assert np.isclose(position.unrealised_pnl_base, -82.10915365239418)
    assert np.isclose(position.realised_pnl_local, 149.101989924432)


    # Third trade (second long)
    quantity = 624
    dt = pd.Timestamp('2020-06-16 18:00:00', tz=pytz.UTC)
    price = 314.68
    order_id = 103
    fx_rate = 0.55
    commission = 2.68
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        order_id=order_id,
        currency = 'EUR',
        fx_rate =fx_rate,
        commission=commission
    )
    position.transact(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 977
    assert position.sell_quantity == 397
    assert position.avg_price_bought == 314.05854657113616
    assert position.avg_price_sold == 313.4
    assert position.commission_local == 9.43

    assert position.direction == 1
    assert np.isclose(position.market_value_local, 182514.4)
    assert np.isclose(position.market_value_base, 100382.92)
    assert position.avg_price == 314.0632855680655
    assert position.net_quantity == 580.00
    assert position.total_bought_local == 306835.2
    assert np.isclose(position.total_sold_local, 124419.8)
    assert np.isclose(position.net_total_local, -182415.4)
    assert np.isclose(position.net_incl_commission_local, -182424.83)
    assert np.isclose(position.unrealised_pnl_local, 357.6943705220083)  ##Incorrect
    assert np.isclose(position.unrealised_pnl_base, 196.731903787104)  ##Incorrect
    assert np.isclose(position.realised_pnl_local, -268.1243705220275)  ##Incorrect


    dt = pd.Timestamp('2020-06-16 19:00:00', tz=pytz.UTC)
    price = 318.25
    position.update_current_price(price, dt)
    fx = 0.61
    position.update_current_fx(fx,dt)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 977
    assert position.sell_quantity == 397
    assert position.avg_price_bought == 314.05854657113616
    assert position.avg_price_sold == 313.4
    assert position.commission_local == 9.43

    assert position.direction == 1
    assert np.isclose(position.market_value_local, 184585.0)
    assert np.isclose(position.market_value_base, 112596.85)
    assert position.avg_price == 314.0632855680655
    assert position.net_quantity == 580.00
    assert position.total_bought_local == 306835.2
    assert np.isclose(position.total_sold_local, 124419.8)
    assert np.isclose(position.net_total_local, -182415.4)
    assert np.isclose(position.net_incl_commission_local, -182424.83)
    assert np.isclose(position.unrealised_pnl_local, 2428.294370522004)  ##Incorrect
    assert np.isclose(position.unrealised_pnl_base, 1481.2595660184224)  ##Incorrect
    assert np.isclose(position.realised_pnl_local, -268.1243705220275)  ##Incorrect

    # Fourth trade (short long)
    quantity = -750
    dt = pd.Timestamp('2020-06-16 20:00:00', tz=pytz.UTC)
    price = 317.68
    order_id = 104
    fx_rate = 0.57
    commission = 2.20
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        order_id=order_id,
        currency = 'EUR',
        fx_rate =fx_rate,
        commission=commission
    )
    position.transact(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 977
    assert position.sell_quantity == 1147
    assert position.avg_price_bought == 314.05854657113616
    assert position.avg_price_sold == 316.1986050566696
    assert position.commission_local == 11.629999999999999

    assert position.direction == -1
    assert np.isclose(position.market_value_local, -54005.6)
    assert np.isclose(position.market_value_base, -30783.192)
    assert position.avg_price == 316.19250217959893
    assert position.net_quantity == -170
    assert position.total_bought_local == 306835.2
    assert np.isclose(position.total_sold_local, 362679.8)
    assert np.isclose(position.net_total_local, 55844.59999999998)
    assert np.isclose(position.net_incl_commission_local, 55832.96999999998)
    assert np.isclose(position.unrealised_pnl_local, -252.87462946818266)  ##Incorrect
    assert np.isclose(position.unrealised_pnl_base, -144.1385387968641)  ##Incorrect
    assert np.isclose(position.realised_pnl_local, 2080.244629468152)  ##Incorrect


    dt = pd.Timestamp('2020-06-16 21:00:00', tz=pytz.UTC)
    price = 320.45
    position.update_current_price(price, dt)
    fx = 0.69
    position.update_current_fx(fx,dt)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 977
    assert position.sell_quantity == 1147.0
    assert position.avg_price_bought == 314.05854657113616
    assert position.avg_price_sold == 316.1986050566696
    assert position.commission_local == 11.629999999999999

    assert position.direction == -1
    assert np.isclose(position.market_value_local, -54476.5)
    assert np.isclose(position.market_value_base, -37588.785)
    assert position.avg_price == 316.19250217959893
    assert position.net_quantity == -170
    assert position.total_bought_local == 306835.2
    assert np.isclose(position.total_sold_local, 362679.8)
    assert np.isclose(position.net_total_local, 55844.59999999998)
    assert np.isclose(position.net_incl_commission_local, 55832.96999999998)
    assert np.isclose(position.unrealised_pnl_local, -723.7746294681796)  ##Incorrect
    assert np.isclose(position.unrealised_pnl_base, -499.4044943330439)  ##Incorrect
    assert np.isclose(position.realised_pnl_local, 2080.244629468152)  ##Incorrect


   # Fifth trade (Close)
    quantity = 170
    dt = pd.Timestamp('2020-06-16 22:00:00', tz=pytz.UTC)
    price = 321.6
    order_id = 105
    fx_rate = 0.7
    commission = 1.79
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        order_id=order_id,
        currency = 'EUR',
        fx_rate =fx_rate,
        commission=commission
    )
    position.transact(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 1147
    assert position.sell_quantity == 1147
    assert position.avg_price_bought == 315.1762859633827
    assert position.avg_price_sold == 316.1986050566696
    assert position.commission_local == 13.42

    assert position.direction == 0
    assert np.isclose(position.market_value_local, 0.0)
    assert np.isclose(position.market_value_base, 0.0)
    assert position.avg_price == 0.0
    assert position.net_quantity == 0.0
    assert position.total_bought_local == 361507.2
    assert np.isclose(position.total_sold_local, 362679.8)
    assert np.isclose(position.net_total_local, 1172.5999999999767)
    assert np.isclose(position.net_incl_commission_local, 1159.1799999999766)
    assert np.isclose(position.unrealised_pnl_local, 0.0) 
    assert np.isclose(position.unrealised_pnl_base, 0.0) 
    assert np.isclose(position.realised_pnl_local, 1159.1799999999766) 


def test_position_long_long_close_with_fx():

    # First trade (first long)
    type = "STOCK_TRANSACTION"
    asset = 'EQ:SPY'
    quantity = 353
    dt = pd.Timestamp('2020-06-16 14:00:00', tz=pytz.UTC)
    price = 312.96
    order_id = 100
    fx_rate = 0.65
    commission = 1.95

    # Create the initial transaction and position
    first_transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        order_id=order_id,
        currency = 'EUR',
        fx_rate = fx_rate,
        commission=commission
    )
    position = Position_MC.open_from_transaction(first_transaction)


    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 353
    assert position.sell_quantity == 0
    assert position.avg_price_bought == 312.96
    assert position.avg_price_sold == 0
    assert position.commission_local == 1.95

    assert position.direction == 1
    assert np.isclose(position.market_value_local, 110474.88)
    assert np.isclose(position.market_value_base, 71808.672)
    assert position.avg_price == 312.96552407932006
    assert position.net_quantity == 353
    assert position.total_bought_local == 110474.87999999999
    assert np.isclose(position.total_sold_local, 0.0)
    assert np.isclose(position.net_total_local, -110474.87999999999)
    assert np.isclose(position.net_incl_commission_local, -110476.82999999999)
    assert np.isclose(position.unrealised_pnl_local, -1.9499999999890747)
    assert np.isclose(position.unrealised_pnl_base, -1.2674999999928986)
    assert np.isclose(position.realised_pnl_local, 0.0)


    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 311.23
    position.update_current_price(price, dt)
    fx = 0.71
    position.update_current_fx(fx,dt)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 353
    assert position.sell_quantity == 0
    assert position.avg_price_bought == 312.96
    assert position.avg_price_sold == 0
    assert position.commission_local == 1.95

    assert position.direction == 1
    assert np.isclose(position.market_value_local, 109864.19)
    assert np.isclose(position.market_value_base, 78003.5749)
    assert position.avg_price == 312.96552407932006
    assert position.net_quantity == 353
    assert position.total_bought_local == 110474.87999999999
    assert np.isclose(position.total_sold_local, 0.0)
    assert np.isclose(position.net_total_local, -110474.87999999999)
    assert np.isclose(position.net_incl_commission_local, -110476.82999999999)
    assert np.isclose(position.unrealised_pnl_local, -612.64)
    assert np.isclose(position.unrealised_pnl_base, -434.974)
    assert np.isclose(position.realised_pnl_local, 0.0)


    dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    price = 313.4
    position.update_current_price(price, dt)
    fx = 0.75
    position.update_current_fx(fx,dt)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 353
    assert position.sell_quantity == 0
    assert position.avg_price_bought == 312.96
    assert position.avg_price_sold == 0
    assert position.commission_local == 1.95
    assert position.direction == 1
    assert np.isclose(position.market_value_local, 110630.2)
    assert np.isclose(position.market_value_base, 82972.65)
    assert position.avg_price == 312.96552407932006
    assert position.net_quantity == 353
    assert position.total_bought_local == 110474.87999999999
    assert np.isclose(position.total_sold_local, 0.0)
    assert np.isclose(position.net_total_local, -110474.87999999999)
    assert np.isclose(position.net_incl_commission_local, -110476.82999999999)
    assert np.isclose(position.unrealised_pnl_local, 153.37000000001012)
    assert np.isclose(position.unrealised_pnl_base, 115.028)
    assert np.isclose(position.realised_pnl_local, 0.0)


    dt = pd.Timestamp('2020-06-16 17:00:00', tz=pytz.UTC)
    price = 316.35
    position.update_current_price(price, dt)
    fx = 0.63
    position.update_current_fx(fx,dt)


    ## Only thing that should change is unrealised P&L
    assert position.buy_quantity == 353
    assert position.sell_quantity == 0
    assert position.avg_price_bought == 312.96
    assert position.avg_price_sold == 0
    assert position.commission_local == 1.95
    assert position.direction == 1
    assert np.isclose(position.market_value_local, 111671.55)
    assert np.isclose(position.market_value_base, 70353.077)
    assert position.avg_price == 312.96552407932006
    assert position.net_quantity == 353
    assert position.total_bought_local == 110474.87999999999
    assert np.isclose(position.total_sold_local, 0.0)
    assert np.isclose(position.net_total_local, -110474.87999999999)
    assert np.isclose(position.net_incl_commission_local, -110476.82999999999)
    assert np.isclose(position.unrealised_pnl_local, 1194.7200000000262)
    assert np.isclose(position.unrealised_pnl_base, 752.674)
    assert np.isclose(position.realised_pnl_local, 0.0)


    # Second trade (second long)
    quantity = 624
    dt = pd.Timestamp('2020-06-16 18:00:00', tz=pytz.UTC)
    price = 314.68
    order_id = 102
    fx_rate = 0.55
    commission = 2.68
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'EUR',
        order_id=order_id,
        fx_rate =fx_rate,
        commission=commission
    )
    position.transact(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 977
    assert position.sell_quantity == 0
    assert position.avg_price_bought == 314.05854657113616
    assert position.avg_price_sold == 0
    assert position.commission_local == 4.63

    assert position.direction == 1
    assert np.isclose(position.market_value_local, 307442.36)
    assert np.isclose(position.market_value_base, 169093.298)
    assert position.avg_price == 314.0632855680655
    assert position.net_quantity == 977
    assert position.total_bought_local == 306835.2
    assert np.isclose(position.total_sold_local, 0.0)
    assert np.isclose(position.net_total_local, -306835.2)
    assert np.isclose(position.net_incl_commission_local, -306839.83)
    assert np.isclose(position.unrealised_pnl_local, 602.5300000000036) 
    assert np.isclose(position.unrealised_pnl_base, 331.392)  
    assert np.isclose(position.realised_pnl_local, 0.0)  


    dt = pd.Timestamp('2020-06-16 19:00:00', tz=pytz.UTC)
    price = 318.25
    position.update_current_price(price, dt)
    fx = 0.61
    position.update_current_fx(fx,dt)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 977
    assert position.sell_quantity == 0.0
    assert position.avg_price_bought == 314.05854657113616
    assert position.avg_price_sold == 0.0
    assert position.commission_local == 4.63

    assert position.direction == 1
    assert np.isclose(position.market_value_local, 310930.25)
    assert np.isclose(position.market_value_base, 189667.45249999998)
    assert position.avg_price == 314.0632855680655
    assert position.net_quantity == 977
    assert position.total_bought_local == 306835.2
    assert np.isclose(position.total_sold_local, 0.0)
    assert np.isclose(position.net_total_local, -306835.2)
    assert np.isclose(position.net_incl_commission_local, -306839.83)
    assert np.isclose(position.unrealised_pnl_local, 4090.419999999997) 
    assert np.isclose(position.unrealised_pnl_base, 2495.156199999998) 
    assert np.isclose(position.realised_pnl_local, 0.0)  

    # Third trade (Close)
    quantity = -977
    dt = pd.Timestamp('2020-06-16 20:00:00', tz=pytz.UTC)
    price = 317.68
    order_id = 104
    fx_rate = 0.57
    commission = 2.20
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'EUR',
        fx_rate =fx_rate,
        order_id=order_id,
        commission=commission
    )
    position.transact(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 977
    assert position.sell_quantity == 977
    assert position.avg_price_bought == 314.05854657113616
    assert position.avg_price_sold == 317.68
    assert position.commission_local == 6.83

    assert position.direction == 0
    assert np.isclose(position.market_value_local, 0.0)
    assert np.isclose(position.market_value_base, 0.0)
    assert position.avg_price == 0.0
    assert position.net_quantity == 0.0
    assert position.total_bought_local == 306835.2
    assert np.isclose(position.total_sold_local, 310373.36)
    assert np.isclose(position.net_total_local, 3538.1599999999744)
    assert np.isclose(position.net_incl_commission_local, 3531.3299999999745)
    assert np.isclose(position.unrealised_pnl_local, 0.0)  
    assert np.isclose(position.unrealised_pnl_base, 0.0)
    assert np.isclose(position.realised_pnl_local, 3531.3299999999745) 



def test_position_short_short_close_with_fx():

    # First trade (first short)
    type = "STOCK_TRANSACTION"
    asset = 'EQ:SPY'
    quantity = -397
    dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    price = 313.4
    order_id = 101
    fx_rate = 0.75
    commission = 4.8
    first_transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        order_id=order_id,
        currency = 'EUR',
        fx_rate=fx_rate,
        commission=commission
    )
    position = Position_MC.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 0
    assert position.sell_quantity == 397
    assert position.avg_price_bought == 0.0
    assert position.avg_price_sold == 313.4
    assert position.commission_local == 4.8

    assert position.direction == -1
    assert np.isclose(position.market_value_local, -124419.79999999999)
    assert np.isclose(position.market_value_base, -93314.84999999999)
    assert position.avg_price == 313.3879093198992
    assert position.net_quantity == -397
    assert position.total_bought_local == 0.0
    assert np.isclose(position.total_sold_local, 124419.8)
    assert np.isclose(position.net_total_local, 124419.8)
    assert np.isclose(position.net_incl_commission_local, 124415.0)
    assert np.isclose(position.unrealised_pnl_local, -4.8)
    assert np.isclose(position.unrealised_pnl_base, -3.6)
    assert np.isclose(position.realised_pnl_local, 0.0)


    dt = pd.Timestamp('2020-06-16 17:00:00', tz=pytz.UTC)
    price = 316.35
    position.update_current_price(price, dt)
    fx = 0.63
    position.update_current_fx(fx,dt)


    assert position.buy_quantity == 0
    assert position.sell_quantity == 397
    assert position.avg_price_bought == 0.0
    assert position.avg_price_sold == 313.4
    assert position.commission_local == 4.8

    assert position.direction == -1
    assert np.isclose(position.market_value_local, -125590.95)
    assert np.isclose(position.market_value_base, -79122.2985)
    assert position.avg_price == 313.3879093198992
    assert position.net_quantity == -397
    assert position.total_bought_local == 0.0
    assert np.isclose(position.total_sold_local, 124419.8)
    assert np.isclose(position.net_total_local, 124419.79999999999)
    assert np.isclose(position.net_incl_commission_local, 124414.99999999999)

    assert np.isclose(position.unrealised_pnl_local, -1175.95)
    assert np.isclose(position.unrealised_pnl_base, -740.849)
    assert np.isclose(position.realised_pnl_local, 0.0)


    dt = pd.Timestamp('2020-06-16 18:00:00', tz=pytz.UTC)
    price = 314.68
    position.update_current_price(price, dt)
    fx = 0.55
    position.update_current_fx(fx,dt)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 0
    assert position.sell_quantity == 397
    assert position.avg_price_bought == 0.0
    assert position.avg_price_sold == 313.4
    assert position.commission_local == 4.8

    assert position.direction == -1
    assert np.isclose(position.market_value_local, -124927.96)
    assert np.isclose(position.market_value_base, -68710.378)
    assert position.avg_price == 313.3879093198992
    assert position.net_quantity == -397
    assert position.total_bought_local == 0.0
    assert np.isclose(position.total_sold_local, 124419.8)
    assert np.isclose(position.net_total_local, 124419.79999999999)
    assert np.isclose(position.net_incl_commission_local, 124414.99999999999)

    assert np.isclose(position.unrealised_pnl_local, -512.96)  
    assert np.isclose(position.unrealised_pnl_base, -282.128)  
    assert np.isclose(position.realised_pnl_local, 0.0)  


    dt = pd.Timestamp('2020-06-16 19:00:00', tz=pytz.UTC)
    price = 318.25
    position.update_current_price(price, dt)
    fx = 0.61
    position.update_current_fx(fx,dt)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 0
    assert position.sell_quantity == 397
    assert position.avg_price_bought == 0.0
    assert position.avg_price_sold == 313.4
    assert position.commission_local == 4.8

    assert position.direction == -1
    assert np.isclose(position.market_value_local, -126345.25)
    assert np.isclose(position.market_value_base, -77070.6025)
    assert position.avg_price == 313.3879093198992
    assert position.net_quantity == -397
    assert position.total_bought_local == 0.0
    assert np.isclose(position.total_sold_local, 124419.8)
    assert np.isclose(position.net_total_local, 124419.79999999999)
    assert np.isclose(position.net_incl_commission_local, 124414.99999999999)

    assert np.isclose(position.unrealised_pnl_local, -1930.25) 
    assert np.isclose(position.unrealised_pnl_base, -1177.453)  
    assert np.isclose(position.realised_pnl_local, 0.0) 

    # Second trade (shorter)
    quantity = -750
    dt = pd.Timestamp('2020-06-16 20:00:00', tz=pytz.UTC)
    price = 317.68
    order_id = 104
    fx_rate = 0.57
    commission = 2.20
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        order_id=order_id,
        currency = 'EUR',
        fx_rate =fx_rate,
        commission=commission
    )
    position.transact(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 0.0
    assert position.sell_quantity == 1147
    assert position.avg_price_bought == 0.0
    assert position.avg_price_sold == 316.1986050566696
    assert position.commission_local == 7.0

    assert position.direction == -1
    assert np.isclose(position.market_value_local, -364378.96)
    assert np.isclose(position.market_value_base, -207696.0072)
    assert position.avg_price == 316.19250217959893
    assert position.net_quantity == -1147.0
    assert position.total_bought_local == 0.0
    assert np.isclose(position.total_sold_local, 362679.8)
    assert np.isclose(position.net_total_local, 362679.8)
    assert np.isclose(position.net_incl_commission_local, 362672.8)
    assert np.isclose(position.unrealised_pnl_local, -1706.16)  
    assert np.isclose(position.unrealised_pnl_base, -972.5112)  
    assert np.isclose(position.realised_pnl_local, 0.0) 


    dt = pd.Timestamp('2020-06-16 21:00:00', tz=pytz.UTC)
    price = 320.45
    position.update_current_price(price, dt)
    fx = 0.69
    position.update_current_fx(fx,dt)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 0.0
    assert position.sell_quantity == 1147
    assert position.avg_price_bought == 0.0
    assert position.avg_price_sold == 316.1986050566696
    assert position.commission_local == 7.0

    assert position.direction == -1
    assert np.isclose(position.market_value_local, -367556.15)
    assert np.isclose(position.market_value_base, -253613.7435)
    assert position.avg_price == 316.19250217959893
    assert position.net_quantity == -1147.0
    assert position.total_bought_local == 0.0
    assert np.isclose(position.total_sold_local, 362679.8)
    assert np.isclose(position.net_total_local, 362679.8)
    assert np.isclose(position.net_incl_commission_local, 362672.8)
    assert np.isclose(position.unrealised_pnl_local, -4883.35)  
    assert np.isclose(position.unrealised_pnl_base, -3369.512) 
    assert np.isclose(position.realised_pnl_local, 0.0) 


   # Third trade (Close)
    quantity = 1147
    dt = pd.Timestamp('2020-06-16 22:00:00', tz=pytz.UTC)
    price = 321.6
    order_id = 105
    fx_rate = 0.7
    commission = 1.79
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        order_id=order_id,
        currency = 'EUR',
        fx_rate =fx_rate,
        commission=commission
    )
    position.transact(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 1147
    assert position.sell_quantity == 1147
    assert position.avg_price_bought == 321.6
    assert position.avg_price_sold == 316.1986050566696
    assert position.commission_local == 8.79

    assert position.direction == 0
    assert np.isclose(position.market_value_local, 0.0)
    assert np.isclose(position.market_value_base, 0.0)
    assert position.avg_price == 0.0
    assert position.net_quantity == 0.0
    assert position.total_bought_local == 368875.2
    assert np.isclose(position.total_sold_local, 362679.8)
    assert np.isclose(position.net_total_local, -6195.4)
    assert np.isclose(position.net_incl_commission_local, -6204.19)
    assert np.isclose(position.unrealised_pnl_local, 0.0) 
    assert np.isclose(position.unrealised_pnl_base, 0.0) 
    assert np.isclose(position.realised_pnl_local, -6204.19) 

    

def test_basic_short_equities_position():
    """
    Tests that the properties on the Position
    are calculated for a simple short equities position.
    """
    # Initial short details
    type = "STOCK_TRANSACTION"
    asset = 'EQ:TLT'
    quantity = -100
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 162.39
    order_id = 123
    commission = 1.37

    # Create the initial transaction and position
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'EUR',
        fx_rate=1.0,
        order_id=order_id,
        commission=commission
    )
    position = Position_MC.open_from_transaction(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    # Update the market price
    new_market_price = 159.43
    new_dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    position.update_current_price(new_market_price, new_dt)

    assert position.current_price == new_market_price
    assert position.current_dt == new_dt

    assert position.buy_quantity == 0
    assert position.sell_quantity == 100
    assert position.avg_price_bought == 0.0
    assert position.avg_price_sold == 162.39
    assert position.commission_local == 1.37

    assert position.direction == -1
    assert position.market_value_local == -15943.0
    assert position.market_value_base == -15943.0
    assert position.avg_price == 162.3763
    assert position.net_quantity == -100
    assert position.total_bought_local == 0.0

    # np.isclose used for floating point precision
    assert np.isclose(position.total_sold_local, 16239.0)
    assert np.isclose(position.net_total_local, 16239.0)
    assert np.isclose(position.net_incl_commission_local, 16237.63)
    assert np.isclose(position.unrealised_pnl_local, 294.63)
    assert np.isclose(position.unrealised_pnl_base, 294.63)
    assert np.isclose(position.realised_pnl_local, 0.0)


def test_basic_short_equities_position_with_fx():
    """
    Tests that the properties on the Position
    are calculated for a simple short equities position.
    """
    # Initial short details
    type = "STOCK_TRANSACTION"
    asset = 'EQ:TLT'
    quantity = -100
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 162.39
    order_id = 123
    fx_rate = 0.85
    commission = 1.37

    # Create the initial transaction and position
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        order_id=order_id,
        currency = 'EUR',
        fx_rate=fx_rate,
        commission=commission
    )
    position = Position_MC.open_from_transaction(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    # Update the market price
    new_market_price = 159.43
    new_dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    position.update_current_price(new_market_price, new_dt)

    assert position.current_price == new_market_price
    assert position.current_dt == new_dt

    assert position.buy_quantity == 0
    assert position.sell_quantity == 100
    assert position.avg_price_bought == 0.0
    assert position.avg_price_sold == 162.39
    assert position.commission_local == 1.37

    assert position.direction == -1
    assert position.market_value_local == -15943.0
    assert position.market_value_base == -13551.55
    assert position.avg_price == 162.3763
    assert position.net_quantity == -100
    assert position.total_bought_local == 0.0

    # np.isclose used for floating point precision
    assert np.isclose(position.total_sold_local, 16239.0)
    assert np.isclose(position.net_total_local, 16239.0)
    assert np.isclose(position.net_incl_commission_local, 16237.63)
    assert np.isclose(position.unrealised_pnl_local, 294.63)
    assert np.isclose(position.unrealised_pnl_base, 250.43549999999826)
    assert np.isclose(position.realised_pnl_local, 0.0)


def test_position_short_twice():
    """
    Tests that the properties on the Position
    are calculated for two consective short trades
    with differing quantities and market prices.
    """
    # Initial short details
    type = "STOCK_TRANSACTION"
    asset = 'EQ:MSFT'
    quantity = -100
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 194.55
    order_id = 123
    commission = 1.44

    # Create the initial transaction and position
    first_transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'USD',
        fx_rate = 1.0,
        order_id=order_id,
        commission=commission
    )
    position = Position_MC.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    # Second short
    second_quantity = -60
    second_dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    second_price = 194.76
    second_order_id = 234
    second_commission = 1.27
    second_transaction = Transaction_MC(
        type,
        asset,
        quantity=second_quantity,
        dt=second_dt,
        price=second_price,
        currency = 'USD',
        fx_rate = 1.0,
        order_id=second_order_id,
        commission=second_commission
    )
    position.transact(second_transaction)

    assert position.current_price == second_price
    assert position.current_dt == second_dt

    assert position.buy_quantity == 0
    assert position.sell_quantity == 160
    assert position.avg_price_bought == 0.0
    assert position.avg_price_sold == 194.62875
    assert position.commission_local == 2.71

    assert position.direction == -1
    assert np.isclose(position.market_value_local, -31161.6)
    assert np.isclose(position.avg_price, 194.6118125)
    assert position.net_quantity == -160
    assert position.total_bought_local == 0.0
    assert position.total_sold_local == 31140.60
    assert position.net_total_local == 31140.6
    assert position.net_incl_commission_local == 31137.89
    assert np.isclose(position.unrealised_pnl_local, -23.71)
    assert np.isclose(position.realised_pnl_local, 0.0)


def test_position_short_close():
    """
    Tests that the properties on the Position
    are calculated for a short opening trade and
    subsequent closing trade.
    """
    # Initial short details
    type = "STOCK_TRANSACTION"
    asset = 'EQ:TSLA'
    quantity = -100
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 982.13
    order_id = 123
    commission = 3.18

    # Create the initial transaction and position
    first_transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'USD',
        fx_rate = 1.0,
        order_id=order_id,
        commission=commission
    )
    position = Position_MC.open_from_transaction(first_transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    # Closing trade
    second_quantity = 100
    second_dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    second_price = 982.13
    second_order_id = 234
    second_commission = 1.0
    second_transaction = Transaction_MC(
        type,
        asset,
        quantity=second_quantity,
        dt=second_dt,
        price=second_price,
        currency = 'USD',
        fx_rate = 1.0,
        order_id=second_order_id,
        commission=second_commission
    )
    position.transact(second_transaction)

    assert position.current_price == second_price
    assert position.current_dt == second_dt

    assert position.buy_quantity == 100
    assert position.sell_quantity == 100
    assert position.avg_price_bought == 982.13
    assert position.avg_price_sold == 982.13
    assert position.commission_local == 4.18

    assert position.direction == 0
    assert position.market_value_local == 0.0
    assert position.avg_price == 0.0
    assert position.net_quantity == 0
    assert position.total_bought_local == 98213.0
    assert position.total_sold_local == 98213.0
    assert position.net_total_local == 0.0
    assert position.net_incl_commission_local == -4.18
    assert position.unrealised_pnl_local == 0.0
    assert position.realised_pnl_local == -4.18


def test_position_short_and_long():
    """
    Tests that the properties on the Position
    are calculated for a short trade followed by
    a partial closing long trade with differing
    market prices.
    """
    # Initial short details
    type = "STOCK_TRANSACTION"
    asset = 'EQ:TLT'
    quantity = -100
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 162.39
    order_id = 123
    commission = 1.37

    # Create the initial transaction and position
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'USD',
        fx_rate = 1.0,
        order_id=order_id,
        commission=commission
    )
    position = Position_MC.open_from_transaction(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    # Long details and transaction
    second_quantity = 60
    second_dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    second_price = 159.99
    second_order_id = 234
    second_commission = 1.0
    second_transaction = Transaction_MC(
        type,
        asset,
        quantity=second_quantity,
        dt=second_dt,
        price=second_price,
        currency = 'USD',
        fx_rate = 1.0,
        order_id=second_order_id,
        commission=second_commission
    )
    position.transact(second_transaction)

    assert position.current_price == second_price
    assert position.current_dt == second_dt

    assert position.buy_quantity == 60
    assert position.sell_quantity == 100
    assert np.isclose(position.avg_price_bought, 159.99)
    assert position.avg_price_sold == 162.39
    assert position.commission_local == 2.37

    assert position.direction == -1
    assert np.isclose(position.market_value_local, -6399.6)
    assert position.avg_price == 162.3763
    assert position.net_quantity == -40
    assert np.isclose(position.total_bought_local, 9599.40)
    assert np.isclose(position.total_sold_local, 16239.0)
    assert np.isclose(position.net_total_local, 6639.60)
    assert np.isclose(position.net_incl_commission_local, 6637.23)
    assert np.isclose(position.unrealised_pnl_local, 95.452)
    assert np.isclose(position.realised_pnl_local, 142.1779999999)


def test_position_short_long_short_long_ending_short():
    """
    Tests that the properties on the Position
    are calculated for four trades consisting
    of a short, long, short and long ending net
    short after all trades with varying quantities
    and market prices.
    """
    # First trade (first short)
    type = "STOCK_TRANSACTION"
    asset = 'EQ:AGG'
    quantity = -762
    dt = pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC)
    price = 117.74
    order_id = 100
    commission = 5.35
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'USD',
        fx_rate = 1.0,
        order_id=order_id,
        commission=commission
    )
    position = Position_MC.open_from_transaction(transaction)

    # Second trade (first long)
    quantity = 477
    dt = pd.Timestamp('2020-06-16 16:00:00', tz=pytz.UTC)
    price = 117.875597
    order_id = 101
    commission = 2.31
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'USD',
        fx_rate = 1.0,
        order_id=order_id,
        commission=commission
    )
    position.transact(transaction)

    # Third trade (second short)
    quantity = -595
    dt = pd.Timestamp('2020-06-16 17:00:00', tz=pytz.UTC)
    price = 117.74
    order_id = 102
    commission = 4.18
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'USD',
        fx_rate = 1.0,
        order_id=order_id,
        commission=commission
    )
    position.transact(transaction)

    # Fourth trade (second long), now net short
    quantity = 427
    dt = pd.Timestamp('2020-06-16 18:00:00', tz=pytz.UTC)
    price = 117.793115
    order_id = 103
    commission = 2.06
    transaction = Transaction_MC(
        type,
        asset,
        quantity=quantity,
        dt=dt,
        price=price,
        currency = 'USD',
        fx_rate = 1.0,
        order_id=order_id,
        commission=commission
    )
    position.transact(transaction)

    assert position.asset == asset
    assert position.current_price == price
    assert position.current_dt == dt

    assert position.buy_quantity == 904
    assert position.sell_quantity == 1357
    assert position.avg_price_bought == 117.83663702876107
    assert position.avg_price_sold == 117.74
    assert np.isclose(position.commission_local, 13.90)

    assert position.direction == -1
    assert np.isclose(position.market_value_local, -53360.281095)
    assert position.avg_price == 117.73297715549005
    assert position.net_quantity == -453
    assert position.total_bought_local == 106524.31987400001
    assert np.isclose(position.total_sold_local, 159773.18)
    assert np.isclose(position.net_total_local, 53248.86)
    assert np.isclose(position.net_incl_commission_local, 53234.95)
    assert np.isclose(position.unrealised_pnl_local, -27.242443563)
    assert np.isclose(position.realised_pnl_local, -98.0785254)


def test_transact_for_incorrect_asset():
    """
    Tests that the 'transact' method, when provided
    with a Transaction with an Asset that does not
    match the position's asset, raises an Exception.
    """
    type = "STOCK_TRANSACTION"
    asset1 = 'EQ:AAPL'
    asset2 = 'EQ:AMZN'

    position = Position_MC(
        asset1,
        currency='USD',
        current_price=950.0,
        current_fx=1.0,
        current_dt=pd.Timestamp('2020-06-16 15:00:00', tz=pytz.UTC),
        buy_quantity=100,
        sell_quantity=0,
        avg_price_bought=950.0,
        avg_price_sold=0.0,
        buy_commission=1.0,
        sell_commission=0.0
    )

    new_dt = pd.Timestamp('2020-06-16 16:00:00')
    transaction = Transaction_MC(
        type,
        asset2,
        quantity=50,
        dt=new_dt,
        price=960.0,
        currency ='USD',
        fx_rate = 1.0,
        order_id=123,
        commission=1.0
    )

    with pytest.raises(Exception):
        position.update(transaction)
