import sys
sys.path.append('T:\Projects_Code\_My_Work\_Strategy_Analysis\qstrader')

import uuid

from qstrader.broker.portfolio_mc.portfolio_mc import Portfolio_MC
from qstrader.broker.portfolio_mc.portfolio_event_mc import PortfolioEvent_MC
from qstrader.broker.transaction.transaction_mc import Transaction_MC
from qstrader.broker.portfolio_mc.position_handler_mc import PositionHandler_MC
from qstrader.broker.portfolio_mc.position_mc import Position_MC
from qstrader.broker.portfolio_mc.position_mc_cash import Position_MC_Cash
from qstrader.broker.transaction.transaction_leg_stock import Transaction_Leg_Stock
from qstrader.broker.transaction.transaction_leg_cash import Transaction_Leg_Cash


def get_transaction_mc(type, asset, currency, price, fx_rate, dt, quantity, commission):

    order_id = uuid.uuid4().hex
    return Transaction_MC(
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

def get_stock_leg_transaction(asset, currency, price, fx_rate, dt, quantity, commission):

    order_id = uuid.uuid4().hex
    return Transaction_Leg_Stock(
        asset,
        currency,
        quantity,
        dt,
        price,
        fx_rate,
        order_id,
        commission
    )


def get_stock_position(asset, currency, price, fx_rate, dt, quantity, commission):

    order_id = uuid.uuid4().hex
    stock_transaction = Transaction_Leg_Stock(
        asset,
        currency,
        quantity,
        dt,
        price,
        fx_rate,
        order_id,
        commission
    )

    return Position_MC.open_from_transaction(stock_transaction)


def get_cash_leg_transaction(asset, fx_rate, dt, quantity, commission):

    order_id = uuid.uuid4().hex
    return Transaction_Leg_Cash(
        asset,
        quantity,
        dt,
        fx_rate,
        order_id,
        commission
    )


def get_cash_position(asset, fx_rate, dt, quantity, commission):

    order_id = uuid.uuid4().hex
    cash_transaction = Transaction_Leg_Cash(
        asset,
        quantity,
        dt,
        fx_rate,
        order_id,
        commission
    )

    return Position_MC_Cash.open_from_transaction(cash_transaction)

