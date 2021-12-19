import copy
import uuid
import datetime
import logging

import pandas as pd

from qstrader import settings
from qstrader.broker.portfolio_mc.portfolio_event_mc import PortfolioEvent_MC
from qstrader.broker.portfolio_mc.position_handler_mc import PositionHandler_MC
from qstrader.broker.portfolio_mc.position_handler_cash_mc import PositionHandler_Cash_MC
from qstrader.broker.transaction.transaction_leg_cash import Transaction_Leg_Cash
from qstrader.broker.transaction.transaction_leg_stock import Transaction_Leg_Stock

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
        self.pos_cash_handler = PositionHandler_Cash_MC()
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

        if self.starting_cash != 0.0:

            txn_one = Transaction_Leg_Cash(
                self.base_currency, self.starting_cash, self.current_dt,
                1.0, uuid.uuid4().hex, commission=0.0
            )
            self.pos_cash_handler.transact_cash_position(txn_one)

        if self.starting_cash > 0.0:
            self.history.append(
                PortfolioEvent_MC.create_subscription(
                    self.current_dt, self.base_currency, self.starting_cash, self.starting_cash
                )
            )

        self.logger.info(
            '(%s) Inital funds subscribed to portfolio "%s" '
            'Base Currency - %s'
            '- Credit: %0.2f, Balance: %0.2f' % (
                self.current_dt.strftime(settings.LOGGING["DATE_FORMAT"]),
                self.portfolio_id,
                self.base_currency,
                round(self.starting_cash, 2),
                round(self.starting_cash, 2)
            )
        )

    @property
    def total_market_value_base(self):
        return self.pos_handler.total_market_value_base() 

    #@property
    def total_market_value_local(self, currency):
        return self.pos_handler.total_market_value_local(currency)

    @property
    def total_cash_value_base(self):
        return self.pos_cash_handler.total_cash_market_value_base() 

    #@property
    def total_cash_value_local(self, currency):
        return self.pos_cash_handler.total_cash_market_value_local(currency) 

    @property
    def total_equity_base(self):
        return self.pos_handler.total_market_value_base() + self.pos_cash_handler.total_cash_market_value_base()

    #@property
    def total_equity_local(self, currency):
        return self.pos_handler.total_market_value_local(currency) + self.pos_cash_handler.total_cash_market_value_local(currency)

    @property
    def total_unrealised_pnl_base(self):
        return self.pos_handler.total_unrealised_pnl_base() + self.pos_cash_handler.total_cash_unrealised_pnl_base()

    #@property
    def total_unrealised_pnl_local(self, currency):
        return self.pos_handler.total_unrealised_pnl_local(currency) + self.pos_cash_handler.total_cash_unrealised_pnl_local(currency)

    #@property
    def total_realised_pnl_local(self, currency):
        return self.pos_handler.total_realised_pnl_local(currency) + self.pos_cash_handler.total_cash_realised_pnl_local(currency)

    #@property
    def total_pnl_local(self, currency):
        return self.pos_handler.total_pnl_local(currency) + self.pos_cash_handler.total_cash_pnl_local(currency)


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




    ##TC When subscribing funds done in base currency only.  Fx transactions changes if needs be
    def subscribe_funds(self, dt, amount):
        
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

        ##TC Create a transaction for subscription cash and update the portfolio
        txn_subscription = Transaction_Leg_Cash(
            self.base_currency, amount, dt,
            1.0, uuid.uuid4().hex, commission=0.0
        )

        self.pos_cash_handler.transact_cash_position(txn_subscription)

        currency_bal = self.portfolio_cash_to_dict()[self.base_currency]['quantity']
        self.history.append(
            PortfolioEvent_MC.create_subscription(self.current_dt,self.base_currency, amount,currency_bal)  
        )
        self.logger.info(
            '(%s) Funds subscribed to portfolio "%s" '
            'Base Currency - %s '
            '- Credit: %0.2f, Balance: %0.2f' % (
                self.current_dt.strftime(settings.LOGGING["DATE_FORMAT"]),
                self.portfolio_id,
                self.base_currency,
                round(amount, 2),
                round(currency_bal, 2)
            )
        )

    def withdraw_funds(self, dt, amount):
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

        currency_bal = self.portfolio_cash_to_dict()[self.base_currency]['quantity']
        if amount > currency_bal:
            raise ValueError(
                'Not enough cash in the portfolio to '
                'withdraw. %s withdrawal request exceeds '
                'current portfolio cash balance of %s.' % (
                    amount, currency_bal
                )
            )

        #TC Create a transaction for withdrawal cash and update the portfolio
        txn_withdraw = Transaction_Leg_Cash(
            self.base_currency, -amount, dt,
            1.0, uuid.uuid4().hex, commission=0.0
        )

        self.pos_cash_handler.transact_cash_position(txn_withdraw)

        currency_bal = self.portfolio_cash_to_dict()[self.base_currency]['quantity']
        self.history.append(
            PortfolioEvent_MC.create_withdrawal(self.current_dt, self.base_currency, amount, currency_bal)
        )
        self.logger.info(
            '(%s) Funds withdrawn from portfolio "%s" '
            'Base Currency - %s '
            '- Debit: %0.2f, Balance: %0.2f' % (
                self.current_dt.strftime(settings.LOGGING["DATE_FORMAT"]),
                self.portfolio_id, 
                self.base_currency,
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

        if txn.type is 'FX_TRANSACTION':
            
            #split commissions 50/50 across each position
            coms = txn.commission / 2.0
            txn_leg_curncy_one = Transaction_Leg_Cash(txn.asset,txn.quantity, txn.dt, txn.price, uuid.uuid4().hex,coms)     
            qty_curncy_two = (txn.price * txn.quantity) / txn.fx_rate
            txn_leg_curncy_two = Transaction_Leg_Cash(txn.currency,-qty_curncy_two, txn.dt, txn.fx_rate, uuid.uuid4().hex,coms) 
            self.pos_cash_handler.transact_cash_position(txn_leg_curncy_one)
            self.pos_cash_handler.transact_cash_position(txn_leg_curncy_two)
            txn_total_cost = qty_curncy_two + txn.commission

        else:
            #Stock transaction - coms applied to stock leg
            txn_leg_stock = Transaction_Leg_Stock(txn.asset,txn.currency, txn.quantity, txn.dt, txn.price, txn.fx_rate, uuid.uuid4().hex, txn.commission)            
            txn_total_cost = (txn.quantity * txn.price) + txn.commission
            txn_leg_cash = Transaction_Leg_Cash(txn.currency, -txn_total_cost, txn.dt, txn.fx_rate, uuid.uuid4().hex,0.0)  
            self.pos_handler.transact_position(txn_leg_stock)
            self.pos_cash_handler.transact_cash_position(txn_leg_cash)


        direction = "LONG" if txn.direction > 0 else "SHORT"
        description = "%s %s %s %0.2f %s %s" % (
            direction, txn.quantity, txn.asset.upper(),
            txn.price, txn.currency, datetime.datetime.strftime(txn.dt, "%d/%m/%Y")
        )

        txn_currency_bal = self.get_position(txn.currency)

        if direction == "LONG":
            pe = PortfolioEvent_MC(
                dt=txn.dt, type='asset_transaction',
                description=description,
                currency=txn.currency,
                debit=round(txn_total_cost, 2), 
                credit=0.0,
                balance=round(txn_currency_bal, 2)
            )
            self.logger.info(
                '(%s) Asset "%s" transacted LONG in portfolio "%s" '
                '- Currency: %s, Debit: %0.2f, Balance: %0.2f' % (
                    txn.dt.strftime(settings.LOGGING["DATE_FORMAT"]),
                    txn.asset, self.portfolio_id,
                    txn.currency,
                    round(txn_total_cost, 2), round(txn_currency_bal, 2)
                )
            )
        else:
            pe = PortfolioEvent_MC(
                dt=txn.dt, type='asset_transaction',
                description=description,
                currency=txn.currency,
                debit=0.0, 
                credit=-1.0 * round(txn_total_cost, 2),
                balance=round(txn_currency_bal, 2)
            )
            self.logger.info(
                '(%s) Asset "%s" transacted SHORT in portfolio "%s" '
                '- Currency: %s, Credit: %0.2f, Balance: %0.2f' % (
                    txn.dt.strftime(settings.LOGGING["DATE_FORMAT"]),
                    txn.asset, self.portfolio_id,
                    txn.currency,
                    -1.0 * round(txn_total_cost, 2), round(txn_currency_bal, 2)
                )
            )
        self.history.append(pe)

    ##TC - Returns all positions as dict
    def portfolio_to_dict(self):
        holdings = {**self.portfolio_equity_to_dict(), **self.portfolio_cash_to_dict()}
        return holdings

    ##TC - Returns equity positions as dict
    def portfolio_equity_to_dict(self):
        holdings = {}
        for asset, pos in self.pos_handler.positions.items():
            holdings[asset] = {
                "quantity": pos.net_quantity,
                "market_value_local": pos.market_value_local,
                "market_value_base": pos.market_value_base,
                "unrealised_pnl_local": pos.unrealised_pnl_local,
                "unrealised_pnl_base": pos.unrealised_pnl_base,
                "realised_pnl_local": pos.realised_pnl_local,
                "total_pnl_local": pos.total_pnl_local
            }
        return holdings

    ##TC - Returns cash positions as dict
    def portfolio_cash_to_dict(self):
        holdings = {}
        for asset, pos in self.pos_cash_handler.positions.items():
            holdings[asset] = {
                "quantity": pos.net_quantity,
                "market_value_local": pos.market_value_local,
                "market_value_base": pos.market_value_base,
                "unrealised_pnl_local": pos.unrealised_pnl_local,
                "unrealised_pnl_base": pos.unrealised_pnl_base,
                "realised_pnl_local": pos.realised_pnl_local,
                "total_pnl_local": pos.total_pnl_local
            }
        return holdings

    ##TC
    def get_position(self, asset):
        all_pos = {**self.portfolio_to_dict(), **self.portfolio_cash_to_dict()}

        if asset in all_pos:
            return all_pos[asset]['quantity']
        else:
            return 0.0


    ##TC
    def get_mv_local(self, asset):
        all_pos = {**self.portfolio_to_dict(), **self.portfolio_cash_to_dict()}

        if asset in all_pos:
            return all_pos[asset]['market_value_local']
        else:
            return 0.0

    ##TC
    def get_mv_base(self, asset):
        all_pos = {**self.portfolio_to_dict(), **self.portfolio_cash_to_dict()}

        if asset in all_pos:
            return all_pos[asset]['market_value_base']
        else:
            return 0.0


    def update_market_value_of_asset(self, asset, current_price, current_dt):

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

            self.pos_handler.positions[asset].update_current_price(current_price, current_dt)


    def update_fx_rate_of_asset(self, asset, current_fx, current_dt):

        if asset not in self.pos_handler.positions:
            return
        else:
            if current_fx < 0.0:
                raise ValueError(
                    'Current trade price of %s is negative for '
                    'asset %s. Cannot update position.' % (
                        current_fx, asset
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

            self.pos_handler.positions[asset].update_current_fx(current_fx,current_dt)


    def update_fx_rate(self, currency, current_fx, current_dt):

        if currency not in self.pos_cash_handler.positions:
            return
        else:
            if current_fx < 0.0:
                raise ValueError(
                    'Current trade price of %s is negative for '
                    'asset %s. Cannot update position.' % (
                        current_fx, currency
                    )
                )

            if current_dt < self.current_dt:
                raise ValueError(
                    'Current trade date of %s is earlier than '
                    'current date %s of asset %s. Cannot update '
                    'position.' % (
                        current_dt, self.current_dt, currency
                    )
                )

            self.pos_cash_handler.positions[currency].update_current_fx(current_fx, current_dt)


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
