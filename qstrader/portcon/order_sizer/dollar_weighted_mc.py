import numpy as np

from qstrader.portcon.order_sizer.order_sizer import OrderSizer


class DollarWeightedCashBufferedOrderSizer_MC(OrderSizer):

    def __init__(
        self,
        broker,
        broker_portfolio_id,
        data_handler,
        cash_buffer_percentage=0.05
    ):
        self.broker = broker
        self.broker_portfolio_id = broker_portfolio_id
        self.data_handler = data_handler
        self.cash_buffer_percentage = self._check_set_cash_buffer(
            cash_buffer_percentage
        )

    def _check_set_cash_buffer(self, cash_buffer_percentage):
        if (
            cash_buffer_percentage < 0.0 or cash_buffer_percentage > 1.0
        ):
            raise ValueError(
                'Cash buffer percentage "%s" provided to dollar-weighted '
                'execution algorithm is negative or '
                'exceeds 100%.' % cash_buffer_percentage
            )
        else:
            return cash_buffer_percentage

    def _obtain_broker_portfolio_total_equity(self):
        return self.broker.get_portfolio_total_equity(self.broker_portfolio_id)

    def _normalise_weights(self, weights):
        if any([weight < 0.0 for weight in weights.values()]):
            raise ValueError(
                'Dollar-weighted cash-buffered order sizing does not support '
                'negative weights. All positions must be long-only.'
            )

        weight_sum = sum(weight for weight in weights.values())

        # If the weights are very close or equal to zero then rescaling
        # is not possible, so simply return weights unscaled
        if np.isclose(weight_sum, 0.0):
            return weights

        return {
            asset: (weight / weight_sum)
            for asset, weight in weights.items()
        }

    def __call__(self, dt, weights):
        total_equity = self._obtain_broker_portfolio_total_equity()
        cash_buffered_total_equity = total_equity * (
            1.0 - self.cash_buffer_percentage
        )

        # Pre-cost dollar weight
        N = len(weights)
        if N == 0:
            # No forecasts so portfolio remains in cash
            # or is fully liquidated
            return {}

        # Ensure weight vector sums to unity
        normalised_weights = self._normalise_weights(weights)

        target_portfolio = {}
        for asset, weight in sorted(normalised_weights.items()):
            pre_cost_dollar_weight = cash_buffered_total_equity * weight

            # Estimate broker fees for this asset
            est_quantity = 0  # TODO: Needs to be added for IB
            est_costs = self.broker.fee_model.calc_total_cost(
                asset, est_quantity, pre_cost_dollar_weight, broker=self.broker
            )

            # Calculate integral target asset quantity assuming broker costs
            after_cost_dollar_weight = pre_cost_dollar_weight - est_costs
            asset_price = self.data_handler.get_asset_latest_ask_price(
                dt, asset
            )

            if np.isnan(asset_price):
                raise ValueError(
                    'Asset price for "%s" at timestamp "%s" is Not-a-Number (NaN). '
                    'This can occur if the chosen backtest start date is earlier '
                    'than the first available price for a particular asset. Try '
                    'modifying the backtest start date and re-running.' % (asset, dt)
                )

            # TODO: Long only for the time being.
            asset_quantity = int(
                np.floor(after_cost_dollar_weight / asset_price)
            )

            # Add to the target portfolio
            target_portfolio[asset] = {"quantity": asset_quantity}

        return target_portfolio
