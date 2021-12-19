import os

import pandas as pd

#from qstrader.asset.equity import Equity
#from qstrader.broker.simulated_broker import SimulatedBroker
#from qstrader.broker.fee_model.zero_fee_model import ZeroFeeModel
#from qstrader.data.backtest_data_handler import BacktestDataHandler
#from qstrader.data.daily_bar_csv import CSVDailyBarDataSource
from qstrader.exchange.simulated_exchange import SimulatedExchange
from qstrader.simulation.daily_bday import DailyBusinessDaySimulationEngine
from qstrader.system.qts_mc import QuantTradingSystem_MC
from qstrader.system.rebalance.buy_and_hold import BuyAndHoldRebalance
from qstrader.system.rebalance.daily import DailyRebalance
from qstrader.system.rebalance.end_of_month import EndOfMonthRebalance
from qstrader.system.rebalance.weekly import WeeklyRebalance
from qstrader.trading.trading_session import TradingSession
from qstrader import settings



from qstrader.asset.asset_mc.equity_mc import Equity_MC
from qstrader.asset.asset_mc.cash_mc import Cash_MC
from qstrader.broker.simulated_broker_mc import SimulatedBroker_MC
from qstrader.broker.fee_model.zero_fee_model import ZeroFeeModel
from qstrader.data.backtest_data_handler import BacktestDataHandler
from qstrader.data.daily_bar_equity_csv import CSVDailyBarEquityDataSource
from qstrader.data.daily_bar_fx_csv import CSVDailyBarFxDataSource




DEFAULT_ACCOUNT_NAME = 'Backtest Simulated Broker Account'
DEFAULT_PORTFOLIO_ID = '000001'
DEFAULT_PORTFOLIO_NAME = 'Backtest Simulated Broker Portfolio'


class BacktestTradingSession_MC(TradingSession):


    def __init__(
        self,
        start_dt,
        end_dt,
        multi_currency_universe,
        alpha_model,
        risk_model=None,
        signals=None,
        initial_cash=1e6,
        rebalance='weekly',
        account_name=DEFAULT_ACCOUNT_NAME,
        portfolio_id=DEFAULT_PORTFOLIO_ID,
        portfolio_name=DEFAULT_PORTFOLIO_NAME,
        long_only=False,
        fee_model=ZeroFeeModel(),
        burn_in_dt=None,
        data_handler=None,
        **kwargs
    ):
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.multi_currency_universe = multi_currency_universe
        self.alpha_model = alpha_model
        self.risk_model = risk_model
        self.signals = signals
        self.initial_cash = initial_cash
        self.rebalance = rebalance
        self.account_name = account_name
        self.portfolio_id = portfolio_id
        self.portfolio_name = portfolio_name
        self.long_only = long_only
        self.fee_model = fee_model
        self.burn_in_dt = burn_in_dt

        self.exchange = self._create_exchange()
        self.data_handler = self._create_data_handler(data_handler)
        self.broker = self._create_broker()
        self.sim_engine = self._create_simulation_engine()

        if rebalance == 'weekly':
            if 'rebalance_weekday' in kwargs:
                self.rebalance_weekday = kwargs['rebalance_weekday']
            else:
                raise ValueError(
                    "Rebalance frequency was set to 'weekly' but no specific "
                    "weekday was provided. Try adding the 'rebalance_weekday' "
                    "keyword argument to the instantiation of "
                    "BacktestTradingSession, e.g. with 'WED'."
                )
        self.rebalance_schedule = self._create_rebalance_event_times()

        self.qts = self._create_quant_trading_system(**kwargs)
        self.equity_curve = []
        self.target_allocations = []

    def _is_rebalance_event(self, dt):
        return dt in self.rebalance_schedule

    def _create_exchange(self):
        return SimulatedExchange(self.start_dt)

    def _create_data_handler(self, data_handler):
        
        if data_handler is not None:
            return data_handler
        try:
            os.environ['QSTRADER_CSV_DATA_DIR']
        except KeyError:
            if settings.PRINT_EVENTS:
                print(
                    "The QSTRADER_CSV_DATA_DIR environment variable has not been set. "
                    "This means that QSTrader will fall back to finding data within the "
                    "current directory where the backtest has been executed. However "
                    "it is strongly recommended that a QSTRADER_CSV_DATA_DIR environment "
                    "variable is set for future backtests."
                )
            csv_dir = '.'
        else:
            csv_dir = os.environ.get('QSTRADER_CSV_DATA_DIR')
       
        equity_assets = [('SPY', 'USD'), ('AGG', 'USD')]
        stock_list = ['SPY', 'AGG']
        #currency_list = ['AUD','JPY','EUR','GBP']

        data_source_equity = CSVDailyBarEquityDataSource(csv_dir, Equity_MC, csv_symbols=stock_list)
        #data_source_fx = CSVDailyBarFxDataSource(csv_dir, Cash_MC, csv_symbols=['EUR'])

        data_handler = BacktestDataHandler(self.multi_currency_universe, data_sources=[data_source_equity])
        
        
        return data_handler

    def _create_broker(self):

        broker = SimulatedBroker_MC(
            self.start_dt,
            self.multi_currency_universe,   ##TC Added
            self.exchange,
            self.data_handler,
            account_id=self.account_name,
            initial_funds=self.initial_cash,
            fee_model=self.fee_model
        )
        broker.create_portfolio(self.portfolio_id, self.portfolio_name)
        broker.subscribe_funds_to_portfolio(self.portfolio_id, self.initial_cash)
        return broker

    def _create_simulation_engine(self):
        
        return DailyBusinessDaySimulationEngine(
            self.start_dt, self.end_dt, pre_market=False, post_market=False
        )

    def _create_rebalance_event_times(self):
        
        if self.rebalance == 'buy_and_hold':
            rebalancer = BuyAndHoldRebalance(self.start_dt)
        elif self.rebalance == 'daily':
            rebalancer = DailyRebalance(
                self.start_dt, self.end_dt
            )
        elif self.rebalance == 'weekly':
            rebalancer = WeeklyRebalance(
                self.start_dt, self.end_dt, self.rebalance_weekday
            )
        elif self.rebalance == 'end_of_month':
            rebalancer = EndOfMonthRebalance(self.start_dt, self.end_dt)
        else:
            raise ValueError(
                'Unknown rebalance frequency "%s" provided.' % self.rebalance
            )
        return rebalancer.rebalances

    def _create_quant_trading_system(self, **kwargs):

        if self.long_only:
            if 'cash_buffer_percentage' not in kwargs:
                raise ValueError(
                    'Long only portfolio specified for Quant Trading System '
                    'but no cash buffer percentage supplied.'
                )
            cash_buffer_percentage = kwargs['cash_buffer_percentage']

            qts = QuantTradingSystem_MC(
                self.multi_currency_universe,
                self.broker,
                self.portfolio_id,
                self.data_handler,
                self.alpha_model,
                self.risk_model,
                long_only=self.long_only,
                cash_buffer_percentage=cash_buffer_percentage,
                submit_orders=True
            )
        else:
            if 'gross_leverage' not in kwargs:
                raise ValueError(
                    'Long/short leveraged portfolio specified for Quant '
                    'Trading System but no gross leverage percentage supplied.'
                )
            gross_leverage = kwargs['gross_leverage']

            qts = QuantTradingSystem_MC(
                self.multi_currency_universe,
                self.broker,
                self.portfolio_id,
                self.data_handler,
                self.alpha_model,
                self.risk_model,
                long_only=self.long_only,
                gross_leverage=gross_leverage,
                submit_orders=True
            )

        return qts

    def _update_equity_curve(self, dt):

        self.equity_curve.append(
            (dt, self.broker.get_account_total_equity()["master"])
        )

    def output_holdings(self):
        self.broker.portfolios[self.portfolio_id].holdings_to_console()

    def get_equity_curve(self):
        equity_df = pd.DataFrame(
            self.equity_curve, columns=['Date', 'Equity']
        ).set_index('Date')
        equity_df.index = equity_df.index.date
        return equity_df

    def get_target_allocations(self):
        equity_curve = self.get_equity_curve()
        alloc_df = pd.DataFrame(self.target_allocations).set_index('Date')
        alloc_df.index = alloc_df.index.date
        alloc_df = alloc_df.reindex(index=equity_curve.index, method='ffill')
        if self.burn_in_dt is not None:
            alloc_df = alloc_df[self.burn_in_dt:]
        return alloc_df

    def run(self, results=False):
        if settings.PRINT_EVENTS:
            print("Beginning backtest simulation...")

        stats = {'target_allocations': []}

        for event in self.sim_engine:
            # Output the system event and timestamp
            dt = event.ts
            if settings.PRINT_EVENTS:
                print("(%s) - %s" % (event.ts, event.event_type))

            # Update the simulated broker
            self.broker.update(dt)

            # Update any signals on a daily basis
            if self.signals is not None and event.event_type == "market_close":
                self.signals.update(dt)

            # If we have hit a rebalance time then carry
            # out a full run of the quant trading system
            if self.burn_in_dt is not None:
                if dt >= self.burn_in_dt:
                    if self._is_rebalance_event(dt):
                        if settings.PRINT_EVENTS:
                            print(
                                "(%s) - trading logic "
                                "and rebalance" % event.ts
                            )
                        self.qts(dt, stats=stats)
            else:
                if self._is_rebalance_event(dt):
                    if settings.PRINT_EVENTS:
                        print(
                            "(%s) - trading logic "
                            "and rebalance" % event.ts
                        )
                    self.qts(dt, stats=stats)

            # Out of market hours we want a daily
            # performance update, but only if we
            # are past the 'burn in' period
            if event.event_type == "market_close":
                if self.burn_in_dt is not None:
                    if dt >= self.burn_in_dt:
                        self._update_equity_curve(dt)
                else:
                    self._update_equity_curve(dt)

        self.target_allocations = stats['target_allocations']

        # At the end of the simulation output the
        # portfolio holdings if desired
        if results:
            self.output_holdings()

        if settings.PRINT_EVENTS:
            print("Ending backtest simulation.")
