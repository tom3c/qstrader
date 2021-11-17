import copy
import uuid
import datetime
import logging

import pandas as pd

from qstrader import settings
from qstrader.broker.portfolio_mc.portfolio_event_mc import PortfolioEvent_MC
from qstrader.broker.portfolio_mc.position_handler_mc import PositionHandler_MC
from qstrader.broker.portfolio_mc.position_handler_cash_mc import PositionCashHandler_MC
#from qstrader.broker.transaction.transaction_cash import Transaction_Cash

##TC - Need to be able to create cash transactions here for subscriptions and withdrawals
# Otherwise most transactions take place at broker level

## Currency all price of cash transactions is 1.  Is that correct?

class Portfolio_MC(object):

    def __init__(
        self,
        start_dt,
        starting_cash=0.0,
        base_currency="USD",
        portfolio_id=None,
        name=None
    ):
        self.start_dt = start_dt
        self.current_dt = start_dt
        self.starting_cash = starting_cash
        self.base_currency = base_currency
        self.portfolio_id = portfolio_id
        self.name = name

        self.pos_handler = PositionHandler_MC()
        self.pos_cash_handler = PositionCashHandler_MC()
        self.history = []

        self.logger = logging.getLogger('Portfolio')
        self.logger.setLevel(logging.DEBUG)
        self.logger.info(
            '(%s) Portfolio "%s" instance initialised' % (
                self.current_dt.strftime(settings.LOGGING["DATE_FORMAT"]),
                self.portfolio_id
            )
        )

        self._initialise_portfolio_with_cash()

    def _initialise_portfolio_with_cash(self):
        
        ##TC -Create a transaction for starting cash and update the portfolio
        # Applies in base currency so fx_rate is 1
        txnc = Transaction_Cash(
            self.base_currency, self.starting_cash, self.current_dt,
            1.0, uuid.uuid4().hex, commission=0.0
        )
        self.pos_cash_handler.transact_cash_position(txnc)

        if self.starting_cash > 0.0:
            self.history.append(
                PortfolioEvent_MC.create_subscription(
                    self.current_dt, self.starting_cash, self.starting_cash
                )
            )

        self.logger.info(
            '(%s) Inital funds subscribed to portfolio "%s" '
            ' Currency - %s'
            '- Credit: %0.2f, Balance: %0.2f' % (
                self.current_dt.strftime(settings.LOGGING["DATE_FORMAT"]),
                self.portfolio_id,
                self.base_currency,
                round(self.starting_cash, 2),
                round(self.starting_cash, 2)
            )
        )

    @property
    def total_market_value(self):
        return self.pos_handler.total_market_value() 

    @property
    def total_cash_value(self):
        return self.pos_cash_handler.total_cash_market_value() 

    @property
    def total_equity(self):
        return self.total_market_value + self.pos_cash_handler.total_cash_market_value()

    @property
    def total_unrealised_pnl(self):
        return self.pos_handler.total_unrealised_pnl() + self.pos_cash_handler.total_cash_unrealised_pnl()

    @property
    def total_realised_pnl(self):
        return self.pos_handler.total_realised_pnl() + self.pos_cash_handler.total_cash_realised_pnl()

    @property
    def total_pnl(self):
        return self.pos_handler.total_pnl() + self.pos_cash_handler.total_cash_pnl()


    ##TC When subscribing funds needs to include currency.  If no currency then base used.
    ##if currency included in to specify fx rate otherwise error
    def subscribe_funds(self, dt, amount, currency=None, fx_rate=None):
        
        if dt < self.current_dt:
            raise ValueError(
                'Subscription datetime (%s) is earlier than '
                'current portfolio datetime (%s). Cannot '
                'subscribe funds.' % (dt, self.current_dt)
            )      
        self.current_dt = dt
        if amount < 0.0:
            raise ValueError(
                'Cannot credit negative amount: '
                '%s to the portfolio.' % amount
            )

        if currency is None:
            currency = self.base_currency
            if fx_rate is not None:
                raise ValueError(
                    'Cannot credit non base curreny with non unit fx rate: '
                    '%s.' % fx_rate
                )    

        ##TC Create a transaction for subscription cash and update the portfolio
        txnc = Transaction_Cash(
            currency, amount, dt,
            1.0, uuid.uuid4().hex, commission=0.0
        )
        self.pos_cash_handler.transact_cash_position(txnc)

        currency_bal = self.portfolio_cash_to_dict()[currency]['quantity']
        self.history.append(
            PortfolioEvent_MC.create_subscription(self.current_dt, amount,currency_bal)  
        )
        self.logger.info(
            '(%s) Funds subscribed to portfolio "%s" '
            ' Currency - %s '
            '- Credit: %0.2f, Balance: %0.2f' % (
                self.current_dt.strftime(settings.LOGGING["DATE_FORMAT"]),
                self.portfolio_id,
                currency,
                round(amount, 2),
                round(currency_bal, 2)
            )
        )

    def withdraw_funds(self, dt, amount, currency=None):
        # Check that amount is positive and that there is
        # enough in the portfolio to withdraw the funds
        if dt < self.current_dt:
            raise ValueError(
                'Withdrawal datetime (%s) is earlier than '
                'current portfolio datetime (%s). Cannot '
                'withdraw funds.' % (dt, self.current_dt)
            )
        self.current_dt = dt

        if amount < 0:
            raise ValueError(
                'Cannot debit negative amount: '
                '%0.2f from the portfolio.' % amount
            )

        if currency is None:
            currency = self.base_currency

        currency_bal = self.portfolio_cash_to_dict()[currency]['quantity']
        if amount > currency_bal:
            raise ValueError(
                'Not enough cash in the portfolio to '
                'withdraw. %s withdrawal request exceeds '
                'current portfolio cash balance of %s.' % (
                    amount, currency_bal
                )
            )

        # Create a transaction for withdrawal cash and update the portfolio
        txnc = Transaction_Cash(
            currency, -amount, dt,
            1.0, uuid.uuid4().hex, commission=0.0
        )
        self.pos_cash_handler.transact_cash_position(txnc)

        currency_bal = self.portfolio_cash_to_dict()[currency]['quantity']
        self.history.append(
            PortfolioEvent_MC.create_withdrawal(self.current_dt, amount, currency_bal)
        )
        self.logger.info(
            '(%s) Funds withdrawn from portfolio "%s" '
            ' Currency - %s '
            '- Debit: %0.2f, Balance: %0.2f' % (
                self.current_dt.strftime(settings.LOGGING["DATE_FORMAT"]),
                self.portfolio_id, 
                currency,
                round(amount, 2),
                round(currency_bal, 2)
            )
        )


    def transact_asset(self, txn):
        if txn.dt < self.current_dt:
            raise ValueError(
                'Transaction datetime (%s) is earlier than '
                'current portfolio datetime (%s). Cannot '
                'transact assets.' % (txn.dt, self.current_dt)
            )
        self.current_dt = txn.dt

        txn_share_cost = txn.price * txn.quantity
        txn_total_cost = txn_share_cost + txn.commission

        if txn_total_cost > self.portfolio_cash_to_dict()[txn.currency]['quantity']:
            if settings.PRINT_EVENTS:
                print(
                    'WARNING: Not enough cash in the portfolio to '
                    'carry out transaction. Transaction cost of %s '
                    'exceeds remaining cash of %s. Transaction '
                    'will proceed with a negative cash balance.' % (
                        txn_total_cost, self.cash
                    )
                )

        self.pos_handler.transact_position(txn)

        ##TC Create a transaction for cash and update the portfolio
        txn_cash = Transaction(txn.currency, -txn_total_cost, txn.dt, 1.0, txn.order_id * uuid.uuid4().hex, commission=0.0)
        self.pos_cash_handler.transact_cash_position(txn_cash)

        # Form Portfolio history details **TO DO TC**
        direction = "LONG" if txn.direction > 0 else "SHORT"
        description = "%s %s %s %0.2f %s" % (
            direction, txn.quantity, txn.asset.upper(),
            txn.price, datetime.datetime.strftime(txn.dt, "%d/%m/%Y")
        )

        ####TC TO DO - Really sort this out no currency record#####
        currency_bal = self.portfolio_cash_to_dict()[txn.currency]['quantity']

        if direction == "LONG":
            pe = PortfolioEvent_MC(
                dt=txn.dt, type='asset_transaction',
                description=description,
                debit=round(txn_total_cost, 2), credit=0.0,
                balance=round(currency_bal, 2)
            )
            self.logger.info(
                '(%s) Asset "%s" transacted LONG in portfolio "%s" '
                '- Debit: %0.2f, Balance: %0.2f' % (
                    txn.dt.strftime(settings.LOGGING["DATE_FORMAT"]),
                    txn.asset, self.portfolio_id,
                    round(txn_total_cost, 2), round(currency_bal, 2)
                )
            )
        else:
            pe = PortfolioEvent_MC(
                dt=txn.dt, type='asset_transaction',
                description=description,
                debit=0.0, credit=-1.0 * round(txn_total_cost, 2),
                balance=round(currency_bal, 2)
            )
            self.logger.info(
                '(%s) Asset "%s" transacted SHORT in portfolio "%s" '
                '- Credit: %0.2f, Balance: %0.2f' % (
                    txn.dt.strftime(settings.LOGGING["DATE_FORMAT"]),
                    txn.asset, self.portfolio_id,
                    -1.0 * round(txn_total_cost, 2), round(currency_bal, 2)
                )
            )
        self.history.append(pe)


    def portfolio_to_dict(self):
        holdings = {}
        for asset, pos in self.pos_handler.positions.items():
            holdings[asset] = {
                "quantity": pos.net_quantity,
                "market_value": pos.market_value,
                "unrealised_pnl": pos.unrealised_pnl,
                "realised_pnl": pos.realised_pnl,
                "total_pnl": pos.total_pnl
            }
        return holdings


    ##TC - Returns cash positions as dict
    def portfolio_cash_to_dict(self):
        holdings = {}
        for asset, pos in self.pos_cash_handler.positions.items():
            holdings[asset] = {
                "quantity": pos.net_quantity,
                "market_value": pos.market_value,
                "unrealised_pnl": pos.unrealised_pnl,
                "realised_pnl": pos.realised_pnl,
                "total_pnl": pos.total_pnl
            }
        return holdings

    ##TC - Returns positions of call asset
    def get_position(self, asset):
        all_pos = {**self.portfolio_to_dict, **self.portfolio_cash_to_dict}

        if asset in all_pos:
            all_pos[asset]['quantity']
        else:
            return 0.0

    ##TC - This needs to be added to pull currency exposure
    # def portfolio_currency_exp_to_dict(self):
    #     exposure = {}


    #     for asset, pos in self.pos_cash_handler.positions.items():
    #         holdings[asset] = {
    #             "quantity": pos.net_quantity,
    #             "market_value": pos.market_value,
    #             "unrealised_pnl": pos.unrealised_pnl,
    #             "realised_pnl": pos.realised_pnl,
    #             "total_pnl": pos.total_pnl
    #         }
    #     for asset, pos in self.pos_cash_handler.positions.items():
    #         holdings[asset] = {
    #             "quantity": pos.net_quantity,
    #             "market_value": pos.market_value,
    #             "unrealised_pnl": pos.unrealised_pnl,
    #             "realised_pnl": pos.realised_pnl,
    #             "total_pnl": pos.total_pnl
    #         }
 
    #     return holdings


    def update_market_value_of_asset(
        self, asset, current_price, current_dt
    ):
        """
        Update the market value of the asset to the current
        trade price and date.
        """
        if asset not in self.pos_handler.positions:
            return
        else:
            if current_price < 0.0:
                raise ValueError(
                    'Current trade price of %s is negative for '
                    'asset %s. Cannot update position.' % (
                        current_price, asset
                    )
                )

            if current_dt < self.current_dt:
                raise ValueError(
                    'Current trade date of %s is earlier than '
                    'current date %s of asset %s. Cannot update '
                    'position.' % (
                        current_dt, self.current_dt, asset
                    )
                )

            self.pos_handler.positions[asset].update_current_price(
                current_price, current_dt
            )

    def history_to_df(self):
        """
        Creates a Pandas DataFrame of the Portfolio history.
        """
        records = [pe.to_dict() for pe in self.history]
        return pd.DataFrame.from_records(
            records, columns=[
                "date", "type", "description", "debit", "credit", "balance"
            ]
        ).set_index(keys=["date"])
