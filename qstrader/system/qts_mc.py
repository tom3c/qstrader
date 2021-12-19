from qstrader.execution.execution_handler import ExecutionHandler
from qstrader.execution.execution_algo.market_order import MarketOrderExecutionAlgorithm
from qstrader.portcon.pcm_mc import PortfolioConstructionModel_MC
from qstrader.portcon.optimiser.fixed_weight import FixedWeightPortfolioOptimiser
from qstrader.portcon.order_sizer.dollar_weighted import DollarWeightedCashBufferedOrderSizer
from qstrader.portcon.order_sizer.long_short import LongShortLeveragedOrderSizer


class QuantTradingSystem_MC(object):

    def __init__(
        self,
        multi_currency_universe,
        broker,
        broker_portfolio_id,
        data_handler,
        alpha_model,
        *args,
        risk_model=None,
        long_only=False,
        submit_orders=False,
        **kwargs
    ):
        self.multi_currency_universe = multi_currency_universe
        self.broker = broker
        self.broker_portfolio_id = broker_portfolio_id
        self.data_handler = data_handler
        self.alpha_model = alpha_model
        self.risk_model = risk_model
        self.long_only = long_only
        self.submit_orders = submit_orders
        self._initialise_models(**kwargs)

    def _create_order_sizer(self, **kwargs):

        if self.long_only:
            if 'cash_buffer_percentage' not in kwargs:
                raise ValueError(
                    'Long only portfolio specified for Quant Trading System '
                    'but no cash buffer percentage supplied.'
                )
            cash_buffer_percentage = kwargs['cash_buffer_percentage']

            order_sizer = DollarWeightedCashBufferedOrderSizer(
                self.broker,
                self.broker_portfolio_id,
                self.data_handler,
                cash_buffer_percentage=cash_buffer_percentage
            )
        else:
            if 'gross_leverage' not in kwargs:
                raise ValueError(
                    'Long/short leveraged portfolio specified for Quant '
                    'Trading System but no gross leverage percentage supplied.'
                )
            gross_leverage = kwargs['gross_leverage']

            order_sizer = LongShortLeveragedOrderSizer(
                self.broker,
                self.broker_portfolio_id,
                self.data_handler,
                gross_leverage=gross_leverage
            )

        return order_sizer

    def _initialise_models(self, **kwargs):

        # Determine the appropriate order sizing mechanism
        order_sizer = self._create_order_sizer(**kwargs)

        # TODO: Allow optimiser to be generated from config
        optimiser = FixedWeightPortfolioOptimiser(
            data_handler=self.data_handler
        )

        # Generate the portfolio construction
        self.portfolio_construction_model = PortfolioConstructionModel_MC(
            self.broker,
            self.broker_portfolio_id,
            self.multi_currency_universe,
            order_sizer,
            optimiser,
            alpha_model=self.alpha_model,
            risk_model=self.risk_model,
            data_handler=self.data_handler
        )

        # Execution
        execution_algo = MarketOrderExecutionAlgorithm()
        self.execution_handler = ExecutionHandler(
            self.broker,
            self.broker_portfolio_id,
            self.multi_currency_universe,
            submit_orders=self.submit_orders,
            execution_algo=execution_algo,
            data_handler=self.data_handler
        )

    def __call__(self, dt, stats=None):

        # Construct the target portfolio
        rebalance_orders = self.portfolio_construction_model(dt, stats=stats)

        # Execute the orders
        self.execution_handler(dt, rebalance_orders)
