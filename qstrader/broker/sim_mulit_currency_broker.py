######TC######

import queue

import numpy as np

from qstrader import settings
from qstrader.broker.broker import Broker
from qstrader.broker.fee_model.fee_model import FeeModel
from qstrader.broker.portfolio.portfolio_multi_currency import Portfolio_Multi_Currency
from qstrader.broker.transaction.transaction import Transaction
from qstrader.broker.fee_model.zero_fee_model import ZeroFeeModel


class SimulatedBrokerMultiCurrency(Broker):

    def __init__(
        self,
        start_dt,
        exchange,
        data_handler,
        account_id=None,
        base_currency="USD",
        initial_funds=0.0,
        fee_model=ZeroFeeModel(),
        slippage_model=None,
        market_impact_model=None
    ):
        self.start_dt = start_dt
        self.exchange = exchange
        self.data_handler = data_handler
        self.current_dt = start_dt
        self.account_id = account_id

        self.base_currency = self._set_base_currency(base_currency)
        self.initial_funds = self._set_initial_funds(initial_funds)
        self.fee_model = self._set_fee_model(fee_model)
        self.slippage_model = None  # TODO: Implement
        self.market_impact_model = None  # TODO: Implement

        self.cash_balances = self._set_cash_balances()
        self.portfolios = self._set_initial_portfolios()
        self.open_orders = self._set_initial_open_orders()

        if settings.PRINT_EVENTS:
            print('Initialising simulated broker "%s"...' % self.account_id)


    def _check_currency(self, currency):

        if currency not in settings.SUPPORTED['CURRENCIES']:
            raise ValueError(
                "Currency '%s' is not supported by QSTrader. Could not "
                "set the base currency in the SimulatedBroker "
                "entity." % currency
            )
        else:
            return True

    def _set_base_currency(self, base_currency):
        if self._check_currency(base_currency):
            return base_currency

    def _set_initial_funds(self, initial_funds):

        if initial_funds < 0.0:
            raise ValueError(
                "Could not create the SimulatedBroker entity as the "
                "provided initial funds of '%s' were "
                "negative." % initial_funds
            )
        else:
            return initial_funds

    def _set_fee_model(self, fee_model):

        if issubclass(fee_model.__class__, FeeModel):
            return fee_model
        else:
            raise TypeError(
                "Provided fee model '%s' in SimulatedBroker is not a "
                "FeeModel subclass, so could not create the "
                "Broker entity." % fee_model.__class__
            )

    def _set_cash_balances(self):

        cash_dict = dict(
            (currency, 0.0)
            for currency in settings.SUPPORTED['CURRENCIES']
        )
        if self.initial_funds > 0.0:
            cash_dict[self.base_currency] = self.initial_funds
        return cash_dict

    def _set_initial_portfolios(self):
        return {}

    def _set_initial_open_orders(self):
        return {}

    def subscribe_funds_to_account(self, amount, currency):

        if amount < 0.0:
            raise ValueError(
                "Cannot credit negative amount: "
                "'%s' to the broker account." % amount
            )

        if self._check_currency(currency):
            self.cash_balances[currency] += amount
            if settings.PRINT_EVENTS:
                print(
                    '(%s) - subscription: %0.2f subscribed to broker account "%s"' % (
                        self.current_dt, amount, self.account_id
                    )
                )

    def withdraw_funds_from_account(self, amount, currency):

        if amount < 0:
            raise ValueError(
                "Cannot debit negative amount: "
                "'%s' from the broker account." % amount
            )

        if self._check_currency(currency):
            if amount > self.cash_balances[currency]:
                raise ValueError(
                    "Not enough cash in the broker account to "
                    "withdraw. %0.2f withdrawal request exceeds "
                    "current broker account cash balance of %0.2f." % (
                        amount, self.cash_balances[currency]
                    )
                )
            self.cash_balances[currency] -= amount
            if settings.PRINT_EVENTS:
                print(
                    '(%s) - withdrawal: %0.2f withdrawn from broker account "%s"' % (
                        self.current_dt, amount, self.account_id
                    )
                )

    def get_account_cash_balance(self, currency=None):

        if currency is None:
            return self.cash_balances
        if currency not in self.cash_balances.keys():
            raise ValueError(
                "Currency of type '%s' is not found within the "
                "broker cash master accounts. Could not retrieve "
                "cash balance." % currency
            )
        return self.cash_balances[currency]

    ## TC NEW ##
    def get_account_total_cash_value(self):

        tcv_dict = {}
        master_tcv = 0.0
        for portfolio in self.portfolios.values():
            pcv = self.get_portfolio_cash_value(
                portfolio.portfolio_id
            )
            tcv_dict[portfolio.portfolio_id] = pcv
            master_tcv += pcv
        tcv_dict["master"] = master_tcv
        return tcv_dict

    def get_account_total_market_value(self):

        tmv_dict = {}
        master_tmv = 0.0
        for portfolio in self.portfolios.values():
            pmv = self.get_portfolio_market_value(
                portfolio.portfolio_id
            )
            tmv_dict[portfolio.portfolio_id] = pmv
            master_tmv += pmv
        tmv_dict["master"] = master_tmv
        return tmv_dict

    def get_account_total_equity(self):

        equity_dict = {}
        master_equity = 0.0
        for portfolio in self.portfolios.values():
            port_equity = self.get_portfolio_total_equity(
                portfolio.portfolio_id
            )
            equity_dict[portfolio.portfolio_id] = port_equity
            master_equity += port_equity
        equity_dict["master"] = master_equity

        return equity_dict

    def create_portfolio(self, portfolio_id, name=None):

        ## Currently only creating in base currency
        ## Which kind of makes sense

        portfolio_id_str = str(portfolio_id)
        if portfolio_id_str in self.portfolios.keys():
            raise ValueError(
                "Portfolio with ID '%s' already exists. Cannot create "
                "second portfolio with the same ID." % portfolio_id_str
            )
        else:
            p = Portfolio_Multi_Currency(
                self.current_dt,
                currency=self.base_currency,
                portfolio_id=portfolio_id_str,
                name=name
            )
            self.portfolios[portfolio_id_str] = p
            self.open_orders[portfolio_id_str] = queue.Queue()
            if settings.PRINT_EVENTS:
                print(
                    '(%s) - portfolio creation: Portfolio "%s" created at broker "%s"' % (
                        self.current_dt, portfolio_id_str, self.account_id
                    )
                )

    def list_all_portfolios(self):

        if self.portfolios == {}:
            return []
        return sorted(
            list(self.portfolios.values()),
            key=lambda port: port.portfolio_id
        )

    def subscribe_funds_to_portfolio(self, portfolio_id, amount, currency):

        if amount < 0.0:
            raise ValueError(
                "Cannot add negative amount: "
                "%0.2f to a portfolio account." % amount
            )
        if portfolio_id not in self.portfolios.keys():
            raise KeyError(
                "Portfolio with ID '%s' does not exist. Cannot subscribe "
                "funds to a non-existent portfolio." % portfolio_id
            )

        if amount > self.cash_balances[currency]:
            raise ValueError(
                "Not enough cash in the broker master account to "
                "fund portfolio '%s'. %0.2f subscription amount exceeds "
                "current broker account cash balance of %0.2f." % (
                    portfolio_id, amount,
                    self.cash_balances[currency]
                )
            )
        self.portfolios[portfolio_id].subscribe_funds(self.current_dt, amount, currency)
        self.cash_balances[currency] -= amount
        if settings.PRINT_EVENTS:
            print(
                '(%s) - subscription: %0.2f subscribed to portfolio "%s"' % (
                    self.current_dt, amount, portfolio_id
                )
            )

    def withdraw_funds_from_portfolio(self, portfolio_id, amount, currency):

        if amount < 0.0:
            raise ValueError(
                "Cannot withdraw negative amount: "
                "%0.2f from a portfolio account." % amount
            )
        if portfolio_id not in self.portfolios.keys():
            raise KeyError(
                "Portfolio with ID '%s' does not exist. Cannot "
                "withdraw funds from a non-existent "
                "portfolio. " % portfolio_id
            )
        if amount > self.portfolios[portfolio_id].portfolio_cash_to_dict[currency]['quantity']:
            raise ValueError(
                "Not enough cash in portfolio '%s' to withdraw "
                "into brokerage master account. Withdrawal "
                "amount %0.2f exceeds current portfolio cash "
                "balance of %0.2f." % (
                    portfolio_id, amount,
                    self.portfolios[portfolio_id].cash
                )
            )
        self.portfolios[portfolio_id].withdraw_funds(
            self.current_dt, amount, currency
        )
        self.cash_balances[currency] += amount
        if settings.PRINT_EVENTS:
            print(
                '(%s) - withdrawal: %0.2f withdrawn from portfolio "%s"' % (
                    self.current_dt, amount, portfolio_id
                )
            )

    # def get_portfolio_cash_balance(self, portfolio_id):

    #     if portfolio_id not in self.portfolios.keys():
    #         raise ValueError(
    #             "Portfolio with ID '%s' does not exist. Cannot "
    #             "retrieve cash balance for non-existent "
    #             "portfolio." % portfolio_id
    #         )
    #     return self.portfolios[portfolio_id].cash

    def get_portfolio_total_cash_value(self, portfolio_id):

        if portfolio_id not in self.portfolios.keys():
            raise ValueError(
                "Portfolio with ID '%s' does not exist. Cannot "
                "retrieve cash balance for non-existent "
                "portfolio." % portfolio_id
            )
        return self.portfolios[portfolio_id].total_cash_value

    def get_portfolio_total_market_value(self, portfolio_id):

        if portfolio_id not in self.portfolios.keys():
            raise KeyError(
                "Portfolio with ID '%s' does not exist. "
                "Cannot return total market value for a "
                "non-existent portfolio." % portfolio_id
            )
        return self.portfolios[portfolio_id].total_market_value

    def get_portfolio_total_equity(self, portfolio_id):

        if portfolio_id not in self.portfolios.keys():
            raise KeyError(
                "Portfolio with ID '%s' does not exist. "
                "Cannot return total equity for a "
                "non-existent portfolio." % portfolio_id
            )
        return self.portfolios[portfolio_id].total_equity

    def get_portfolio_as_dict(self, portfolio_id):
 
        if portfolio_id not in self.portfolios.keys():
            raise KeyError(
                "Cannot return portfolio as dictionary since "
                "portfolio with ID '%s' does not exist." % portfolio_id
            )
        return self.portfolios[portfolio_id].portfolio_to_dict()

    def get_portfolio_cash_as_dict(self, portfolio_id):
 
        if portfolio_id not in self.portfolios.keys():
            raise KeyError(
                "Cannot return portfolio as dictionary since "
                "portfolio with ID '%s' does not exist." % portfolio_id
            )
        return self.portfolios[portfolio_id].portfolio_cash_to_dict()


    def _execute_order(self, dt, portfolio_id, order):

        # Obtain a price for the asset, if no price then
        # raise a ValueError
        price_err_msg = (
            "Could not obtain a latest market price for "
            "Asset with ticker symbol '%s'. Order with ID '%s' was "
            "not executed." % (
                order.asset, order.order_id
            )
        )
        bid_ask = self.data_handler.get_asset_latest_bid_ask_price(
            dt, order.asset
        )
        if bid_ask == (np.NaN, np.NaN):
            raise ValueError(price_err_msg)

        # Calculate the consideration and total commission
        # based on the commission model
        if order.direction > 0:
            price = bid_ask[1]
        else:
            price = bid_ask[0]
        consideration = round(price * order.quantity)
        total_commission = self.fee_model.calc_total_cost(
            order.asset, order.quantity, consideration, self
        )

        # Check that sufficient cash exists to carry out the
        # order, else scale it down
        est_total_cost = consideration + total_commission
        total_cash = self.portfolios[portfolio_id].cash

        scaled_quantity = order.quantity
        if est_total_cost > total_cash:
            if settings.PRINT_EVENTS:
                print(
                    "WARNING: Estimated transaction size of %0.2f exceeds "
                    "available cash of %0.2f. Transaction will still occur "
                    "with a negative cash balance." % (est_total_cost, total_cash)
                )

        # Create a transaction entity and update the portfolio
        txn = Transaction(
            order.asset, scaled_quantity, self.current_dt,
            price, order.order_id, commission=total_commission
        )
        self.portfolios[portfolio_id].transact_asset(txn)
        if settings.PRINT_EVENTS:
            print(
                "(%s) - executed order: %s, qty: %s, price: %0.2f, "
                "consideration: %0.2f, commission: %0.2f, total: %0.2f" % (
                    self.current_dt, order.asset, scaled_quantity, price,
                    consideration, total_commission,
                    consideration + total_commission
                )
            )

    def submit_order(self, portfolio_id, order):
        """
        Execute an Order instance against the sub-portfolio
        with ID 'portfolio_id'. For the SimulatedBroker class
        specifically there are no restrictions on this occuring
        beyond having sufficient cash in the sub-portfolio to
        allow this to occur.

        This does not take into settlement dates, as with most
        brokerage accounts. The cash is taken immediately upon
        entering a long position and returned immediately upon
        closing out the position.

        Parameters
        ----------
        portfolio_id : `str`
            The portfolio ID string.
        order : `Order`
            The Order instance to submit.
        """
        # Check that the portfolio actually exists
        if portfolio_id not in self.portfolios.keys():
            raise KeyError(
                "Portfolio with ID '%s' does not exist. Order with "
                "ID '%s' was not executed." % (
                    portfolio_id, order.order_id
                )
            )
        self.open_orders[portfolio_id].put(order)
        if settings.PRINT_EVENTS:
            print(
                "(%s) - submitted order: %s, qty: %s" % (
                    self.current_dt, order.asset, order.quantity
                )
            )

    def update(self, dt):
        """
        Updates the current SimulatedBroker timestamp.

        Parameters
        ----------
        dt : `pd.Timestamp`
            The current timestamp to update the Broker to.
        """
        self.current_dt = dt

        # Update portfolio asset values
        for portfolio in self.portfolios:
            for asset in self.portfolios[portfolio].pos_handler.positions:
                mid_price = self.data_handler.get_asset_latest_mid_price(
                    dt, asset
                )
                self.portfolios[portfolio].update_market_value_of_asset(
                    asset, mid_price, self.current_dt
                )

        # Try to execute orders
        if self.exchange.is_open_at_datetime(self.current_dt):
            orders = []
            for portfolio in self.portfolios:
                while not self.open_orders[portfolio].empty():
                    orders.append(
                        (portfolio, self.open_orders[portfolio].get())
                    )

            sorted_orders = sorted(orders, key=lambda x: x[1].direction)
            for portfolio, order in sorted_orders:
                self._execute_order(dt, portfolio, order)
