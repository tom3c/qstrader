from qstrader import settings
from qstrader.execution.order_mc import Order_MC


class PortfolioConstructionModel_MC(object):

    def __init__(
        self,
        broker,
        broker_portfolio_id,
        multi_currency_universe,
        order_sizer,
        optimiser,
        alpha_model=None,
        risk_model=None,
        cost_model=None,
        data_handler=None,
    ):
        self.broker = broker
        self.broker_portfolio_id = broker_portfolio_id
        self.multi_currency_universe = multi_currency_universe
        self.order_sizer = order_sizer
        self.optimiser = optimiser
        self.alpha_model = alpha_model
        self.risk_model = risk_model
        self.cost_model = cost_model
        self.data_handler = data_handler



    def _obtain_full_asset_list(self, dt):

        broker_portfolio = self.broker.get_portfolio_as_dict(self.broker_portfolio_id)
        
        broker_assets = list(broker_portfolio.keys())
        universe_equity_assets = self.multi_currency_universe.get_equity_assets(dt)
        universe_cash_assets = self.multi_currency_universe.get_cash_assets(dt)
        universe_assets = universe_equity_assets + universe_cash_assets
        return sorted(list(set(broker_assets).union(set(universe_assets))))


    def _obtain_equity_asset_list(self, dt):

        broker_portfolio = self.broker.get_portfolio_equity_as_dict(self.broker_portfolio_id)
        
        broker_assets = list(broker_portfolio.keys())
        universe_equity_assets = self.multi_currency_universe.get_equity_assets(dt)
        return sorted(list(set(broker_assets).union(set(universe_equity_assets))))

    def _obtain_cash_asset_list(self, dt):

        broker_portfolio = self.broker.get_portfolio_as_dict(
            self.broker_portfolio_id
        )
        broker_assets = list(broker_portfolio.keys())
        universe_cash_assets = self.multi_currency_universe.get_cash_assets(dt)
        return sorted(
            list(
                set(broker_assets).union(set(universe_cash_assets))
            )
        )

    def _create_zero_target_weight_vector(self, full_assets):
        return {asset: 0.0 for asset in full_assets}

    def _create_full_asset_weight_vector(self, zero_weights, optimised_weights):
        return {**zero_weights, **optimised_weights}

    def _generate_target_portfolio(self, dt, weights):
        return self.order_sizer(dt, weights)

    def _obtain_current_full_portfolio(self):
        return self.broker.get_portfolio_as_dict(self.broker_portfolio_id)

    def _obtain_current_equity_portfolio(self):
        return self.broker.get_portfolio_equity_as_dict(self.broker_portfolio_id)

    def _obtain_current_cash_portfolio(self):
        return self.broker.get_portfolio_cash_as_dict(self.broker_portfolio_id)

    def _generate_rebalance_orders(
        self,
        dt,
        target_portfolio,
        current_portfolio
    ):

        # Set all assets from the target portfolio that
        # aren't in the current portfolio to zero quantity
        # within the current portfolio
        for asset in target_portfolio:
            if asset not in current_portfolio:
                current_portfolio[asset] = {"quantity": 0}

        # Set all assets from the current portfolio that
        # aren't in the target portfolio (and aren't cash) to
        # zero quantity within the target portfolio
        for asset in current_portfolio:
            if type(asset) != str:
                if asset not in target_portfolio:
                    target_portfolio[asset] = {"quantity": 0}

        # Iterate through the asset list and create the difference
        # quantities required for each asset
        rebalance_portfolio = {}
        for asset in target_portfolio.keys():
            target_qty = target_portfolio[asset]["quantity"]
            current_qty = current_portfolio[asset]["quantity"]
            order_qty = target_qty - current_qty
            rebalance_portfolio[asset] = {"quantity": order_qty}

        # Create the rebalancing Order list from the order portfolio
        # only where quantities are non-zero
        rebalance_orders = [
            Order_MC('STOCK_ORDER', dt, asset, rebalance_portfolio[asset]["quantity"], currency=self.multi_currency_universe.get_equity_asset_currency(asset))   #self.multi_currency_universe.get_equity_asset_currency(asset)
            for asset, asset_dict in sorted(
                rebalance_portfolio.items(), key=lambda x: x[0]
            )
            if rebalance_portfolio[asset]["quantity"] != 0
        ]

        return rebalance_orders

    def _create_zero_target_equity_weights_vector(self, dt):

        assets = self.multi_currency_universe.get_equity_assets(dt)
        return {asset: 0.0 for asset in assets}


    def __call__(self, dt, stats=None):

        # If an AlphaModel is provided use its suggestions, otherwise
        # create a null weight vector (zero for all Assets).
        if self.alpha_model:
            weights = self.alpha_model(dt)
        else:
            weights = self._create_zero_target_weights_vector(dt)

        # If a risk model is present use it to potentially
        # override the alpha model weights
        if self.risk_model:
            weights = self.risk_model(dt, weights)

        # Run the portfolio optimisation
        optimised_weights = self.optimiser(dt, initial_weights=weights)

        # Ensure any Assets in the Broker Portfolio are sold out if
        # they are not specifically referenced on the optimised weights
        
        ##TC - Only Dealing with equity assets at the moment
        #full_assets = self._obtain_full_asset_list(dt)
        #full_zero_weights = self._create_zero_target_weight_vector(full_assets)

        equity_assets = self._obtain_equity_asset_list(dt)
        equity_zero_weights = self._create_zero_target_weight_vector(equity_assets)

        full_weights = self._create_full_asset_weight_vector(
            equity_zero_weights, optimised_weights
        )
        if settings.PRINT_EVENTS:
            print(
                "(%s) - target weights: %s" % (dt, full_weights)
            )

        # TODO: Improve this with a full statistics logging handler
        if stats is not None:
            alloc_dict = {'Date': dt}
            alloc_dict.update(full_weights)
            stats['target_allocations'].append(alloc_dict)

        # Calculate target portfolio in notional
        target_portfolio = self._generate_target_portfolio(dt, full_weights)

        # Obtain current Broker account portfolio
        current_portfolio = self._obtain_current_equity_portfolio()

        # Create rebalance trade Orders
        rebalance_orders = self._generate_rebalance_orders(
            dt, target_portfolio, current_portfolio
        )
        # TODO: Implement cost model

        return rebalance_orders
