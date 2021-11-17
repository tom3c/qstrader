######TC######

import queue
from typing_extensions import Required

import numpy as np

from qstrader import settings
from qstrader.broker.broker import Broker
from qstrader.broker.fee_model.fee_model import FeeModel
from qstrader.broker.portfolio_mc.portfolio_mc import Portfolio_MC
from qstrader.broker.transaction.transaction_mc import Transaction_MC
from qstrader.broker.fee_model.zero_fee_model import ZeroFeeModel

##TC - data handler is at this level.  I've added universe so also all relevant currencies are known too.
# You can subscribe/redeem funds to/from broker account - These need to be allocated/unallocated to a portfolio to trade 
# Broker has one to many relationship with portfolios.  

class SimulatedBroker_MC(Broker):

    def __init__(
        self,
        start_dt,
        universe,       ##TC Added
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
        self.universe = universe
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

    ##if querying for base currency fx return 1

    def _check_currency(self, currency):
        ##TC - Checks in universe####
        if currency not in self.universe.get_cash_assets(self.current_dt):  
            raise ValueError(
                "Currency '%s' is not available in universe. Could not "
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

    ##TC - Checks in universe and sets all to zero, initial fund allocated in base currency##
    def _set_cash_balances(self):

        cash_dict = dict(
            (currency, 0.0)
            for currency in self.universe.get_cash_assets(self.current_dt)
        )
        if self.initial_funds > 0.0:
            cash_dict[self.base_currency] = self.initial_funds
        return cash_dict

    def _set_initial_portfolios(self):
        return {}

    def _set_initial_open_orders(self):
        return {}


    ##TC - Need to refactor to only do in base currency
    ## Then can use FX transactions to change currencies
    def subscribe_funds_to_account(self, amount, currency=None):

        if amount < 0.0:
            raise ValueError(
                "Cannot credit negative amount: "
                "'%s' to the broker account." % amount
            )

        if currency is None:
            currency = self.base_currency

        if self._check_currency(currency):
            self.cash_balances[currency] += amount
            if settings.PRINT_EVENTS:
                print(
                    '(%s) - subscription: %0.2f subscribed to broker account "%s"' % (
                        self.current_dt, amount, self.account_id
                    )
                )

    ##TC - Need to refactor to only do in base currency
    ## Then can use FX transactions to change currencies
    def withdraw_funds_from_account(self, amount, currency=None):

        if amount < 0:
            raise ValueError(
                "Cannot debit negative amount: "
                "'%s' from the broker account." % amount
            )

        if currency is None:
            currency = self.base_currency

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

    ## TC Check - Returning local cash balance ##
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

    ##TC - Portfolio only created in base currency
    ## Then can use FX transactions to change currencies
    def create_portfolio(self, portfolio_id, name=None):

        portfolio_id_str = str(portfolio_id)
        if portfolio_id_str in self.portfolios.keys():
            raise ValueError(
                "Portfolio with ID '%s' already exists. Cannot create "
                "second portfolio with the same ID." % portfolio_id_str
            )
        else:
            p = Portfolio_MC(
                self.current_dt,
                base_currency=self.base_currency,
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

    ##TC Need to change so that subscriptions happen in base curreny
    ##Would need to apply fx transaction first for other currencies
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


    ##TC Need to change so that withdraws happen in base curreny
    ##Would need to apply fx transaction first for other currencies
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

    ##TC - Removed as cash are treated as positions
    # def get_portfolio_cash_balance(self, portfolio_id):

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

    ##TC - Added to get cash positions
    def get_portfolio_cash_as_dict(self, portfolio_id):
 
        if portfolio_id not in self.portfolios.keys():
            raise KeyError(
                "Cannot return portfolio as dictionary since "
                "portfolio with ID '%s' does not exist." % portfolio_id
            )
        return self.portfolios[portfolio_id].portfolio_cash_to_dict()

    ##TC Determines what order type and reroutes to correct function
    ##Can then easily add handling for different order types
    ##For examples fx derivs for hedging
    def _execute_order(self, dt, portfolio_id, order):

        if order.order_type is 'FX_ORDER':
            self._execute_fx_order(dt, portfolio_id, order)
        else:
            #Stock order
            self._execute_stock_order(dt, portfolio_id, order)

 
    ##Processes FX orders
    def _execute_fx_order(self, dt, portfolio_id, order):

        ##Need check whether debits possible or not - Add to Settings
        ##Currently will just create negative cash balance 

        second_currency = order.currency
        if second_currency is None: 
            second_currency = self.base_currency

        #Maybe buying to selling base currency so need to check asset
        #Asset is first currency for fx transaction
        first_currency = order.asset

        # Obtain a fx rate for first currency if not base
        if first_currency != self.base_currency:
            fx_rate_first = self._get_mark(dt, order.asset, order.order_id)       
        else:
            fx_rate_first = 1.0  

        # Obtain a fx rate for second currency if not base
        if second_currency != self.base_currency:
            fx_rate_second = self._get_mark(dt, order.asset, order.order_id)
        else:
            fx_rate_second = 1.0 

        # Calculate the consideration and total commission
        # based on the commission model
        consideration = round((fx_rate_first * order.quantity) / fx_rate_second)
        total_commission = self.fee_model.calc_total_cost(
            order.asset, order.quantity, consideration, self
        )
        est_total_cost = consideration + total_commission

        # Then check that sufficient cash exists to carry
        # out the order, else scale it down.  
        if order.direction > 0:
            ##Currency buying fx is second so need to check have enough
            total_currency_second = self.portfolios[portfolio_id].portfolio_cash_to_dict()[first_currency]['quantity']
            if est_total_cost > total_currency_second:
                if settings.PRINT_EVENTS:
                    print(
                        "WARNING: Estimated FX transaction size of %0.2f exceeds "
                        "available cash of %0.2f. Transaction will still occur "
                        "with a negative cash balance." % (est_total_cost, total_currency_second)
                    )
        else:
            ##Currency being sold is first so need to check have enough
            total_currency_first = self.portfolios[portfolio_id].portfolio_cash_to_dict()[first_currency]['quantity']
            if order.quantity > total_currency_first:
                if settings.PRINT_EVENTS:
                    print(
                        "WARNING: Estimated FX transaction size of %0.2f exceeds "
                        "available cash of %0.2f. Transaction will still occur "
                        "with a negative cash balance." % (est_total_cost, total_currency_first)
                    )


        txn_fx = Transaction_MC(
            "FX_TRANSACTION",
            first_currency, order.quantity, self.current_dt,
            fx_rate_first, second_currency, fx_rate_second, order.order_id, commission=total_commission
        )
        self.portfolios[portfolio_id].transact_asset(txn_fx)
        if settings.PRINT_EVENTS:
            print(
                "(%s) - executed fx order: %s, qty: %s, price: %0.2f, "
                "consideration: %0.2f, commission: %0.2f, total: %0.2f" % (
                self.current_dt, first_currency, order.quantity, fx_rate_first,
                consideration, total_commission,
                consideration + total_commission
                )
            )


    ##Processes stock orders
    def _execute_stock_order(self, dt, portfolio_id, order):

        ##Need check whether debits possible or not - Add to Settings
        ##Currently will just create negative cash balance 
        ##There is potential to create short positions as stands too

        if order.currency is None:
            currency = self.universe.get_equity_asset_currency(order.asset)

        price = self._get_mark(dt, order.asset, order.order_id)

        if currency != self.base_currency:
            # Obtain a fx rate for currency if not base
            fx_rate = self._get_mark(dt, currency, order.order_id)
        else:
            fx_rate = 1    

        # Check that sufficient cash exists to carry out the
        # order, else scale it down.  
        # Auto FX
        consideration = round(price * order.quantity)
        total_commission = self.fee_model.calc_total_cost(
            order.asset, order.quantity, consideration, self
        )

        est_total_cost = consideration + total_commission
        total_currency = self.portfolios[portfolio_id].portfolio_cash_to_dict()[currency]['quantity']

        ## TC, should probably check you have adequate amount of 
        ## asset to sell if selling order

        if order.direction > 0:
            if est_total_cost > total_currency:
                if order.auto_fx and currency != self.base_currency:
                
                    #Carry out auto FX from base currency.  Already have rate - fx_rate
                    amount_require = est_total_cost - total_currency

                    consideration_auto_fx = round(fx_rate * amount_require)
                    total_commission_auto_fx = self.fee_model.calc_total_cost(
                        currency, amount_require, consideration_auto_fx, self
                    )
                    amount_auto_fx = consideration_auto_fx + total_commission_auto_fx

                    total_currency_base = self.portfolios[portfolio_id].portfolio_cash_to_dict()[self.base_currency]['quantity']
                
                    if amount_auto_fx  > total_currency_base:
                        if settings.PRINT_EVENTS:
                            print(
                                "WARNING: Estimated Auto FX transaction size of %0.2f exceeds "
                                "available cash of %0.2f. Transaction will still occur "
                                "with a negative cash balance." % (amount_auto_fx , total_currency_base)
                            )

                    #Create Auto FX Transaction 
                    txn_auto_fx = Transaction_MC(
                        "FX_TRANSACTION",
                        currency, amount_auto_fx, self.current_dt,
                        fx_rate, self.base_currency, 1.0, order.order_id +'_Auto_FX', commission=total_commission
                    )
                    self.portfolios[portfolio_id].transact_asset(txn_auto_fx)
                    if settings.PRINT_EVENTS:
                        print(
                            "(%s) - executed auto fx order: %s, qty: %s, price: %0.2f, "
                            "consideration: %0.2f, commission: %0.2f, total: %0.2f" % (
                                self.current_dt, currency, amount_auto_fx, fx_rate,
                                consideration, total_commission,
                                consideration + total_commission
                            )
                        )

                else:
                    if settings.PRINT_EVENTS:
                        print(
                            "WARNING: Estimated transaction size of %0.2f exceeds "
                            "available cash of %0.2f. Transaction will still occur "
                            "with a negative cash balance." % (est_total_cost, total_currency)
                        )
            else:

                #Create Stock Transaction 
                txn_stock = Transaction_MC(
                    "STOCK_TRANSACTION",
                    order.asset, order.quantity, self.current_dt,
                    price, currency, fx_rate, order.order_id, commission=total_commission
                )
                self.portfolios[portfolio_id].transact_asset(txn_stock)
                if settings.PRINT_EVENTS:
                    print(
                        "(%s) - executed order: %s, qty: %s, price: %0.2f, "
                        "consideration: %0.2f, commission: %0.2f, total: %0.2f" % (
                            self.current_dt, order.asset, order.quantity, price,
                            consideration, total_commission,
                            consideration + total_commission
                        )
                    )
        else:

            current_stock_pos = self.portfolios[portfolio_id].get_position(order.asset)
            if abs(order.quantity) > current_stock_pos:
                #Adjust order size to sell max available
                scaled_quantity = current_stock_pos 
            else:
                scaled_quantity = order.quantity 

            #Create Stock Transaction 
            txn_stock = Transaction_MC(
                "STOCK_TRANSACTION",
                order.asset, scaled_quantity, self.current_dt,
                price, currency, fx_rate, order.order_id, commission=total_commission
            )
            self.portfolios[portfolio_id].transact_asset(txn_stock)
            if settings.PRINT_EVENTS:
                print(
                    "(%s) - executed order: %s, qty: %s, price: %0.2f, "
                    "consideration: %0.2f, commission: %0.2f, total: %0.2f" % (
                        self.current_dt, order.asset, scaled_quantity, price,
                        consideration, total_commission,
                        consideration + total_commission
                    )
                )

            if order.auto_fx and currency != self.base_currency:

                    consideration_auto_fx = round(fx_rate * consideration)
                    
                    ##Note commissions on auto FX incorrect - current using zero fee model
                    total_commission_auto_fx = self.fee_model.calc_total_cost(
                        currency, consideration_auto_fx, consideration_auto_fx, self
                    )
                    amount_auto_fx = -(consideration_auto_fx + total_commission_auto_fx) #Negative as selling

                    #Create Auto FX Transaction from resulting cash generated
                    txn_auto_fx = Transaction_MC(
                        "FX_TRANSACTION",
                        currency, amount_auto_fx, self.current_dt,
                        fx_rate, self.base_currency, 1.0, order.order_id +'_Auto_FX', commission=total_commission
                    )
                    self.portfolios[portfolio_id].transact_asset(txn_auto_fx)
                    if settings.PRINT_EVENTS:
                        print(
                            "(%s) - executed auto fx order: %s, qty: %s, price: %0.2f, "
                            "consideration: %0.2f, commission: %0.2f, total: %0.2f" % (
                                self.current_dt, currency, amount_auto_fx, fx_rate,
                                consideration, total_commission,
                                consideration + total_commission
                            )
                        )



    def _get_mark(self, dt, asset, order_id):

        price_err_msg = (
            "Could not obtain a latest market price for "
            "Asset with ticker symbol '%s'. Order with ID '%s' was "
            "not executed." % (
                asset, order_id
            )
        )

        bid_ask_fx_second = self.data_handler.get_asset_latest_bid_ask_price(
            dt, asset
        )
            
        if bid_ask_fx_second == (np.NaN, np.NaN):
            raise ValueError(price_err_msg)

        ## Note doesn't handle bid offer property.  Assumes no spread
        return bid_ask_fx_second[1]


    ## TC Note - SUBMIT FX ORDER will use the below function.  Order contains asset, quantity and date.  
    ## The transaction type can be determined by the asset type. This is just taking orders from models
    ## And passing them to broker.  They are executed on broker update call.
    def submit_order(self, portfolio_id, order):
   
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

            for cash_asset in self.portfolios[portfolio].pos_cash_handler.positions:
                
                ##TC - Base currency always 1 so not updated
                if cash_asset != self.portfolios[portfolio].base_currency:
                    mid_price = self.data_handler.get_asset_latest_mid_price(
                        dt, cash_asset
                    )
                    self.portfolios[portfolio].update_market_value_of_asset(
                        cash_asset, mid_price, self.current_dt
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
