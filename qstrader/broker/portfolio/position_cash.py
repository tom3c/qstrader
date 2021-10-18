from math import floor

import numpy as np


class PositionCash(object):

    def __init__(
        self,
        asset,
        current_price,
        current_dt,
        buy_quantity,
        sell_quantity,
        avg_bought,
        avg_sold,
        buy_commission,
        sell_commission
    ):
        self.asset = asset
        self.current_price = current_price
        self.current_dt = current_dt
        self.buy_quantity = buy_quantity
        self.sell_quantity = sell_quantity
        self.avg_bought = avg_bought
        self.avg_sold = avg_sold
        self.buy_commission = buy_commission
        self.sell_commission = sell_commission

    @classmethod
    def open_from_transaction(cls, transaction):

        asset = transaction.asset
        current_price = transaction.price
        current_dt = transaction.dt

        if transaction.quantity > 0:
            buy_quantity = transaction.quantity
            sell_quantity = 0
            avg_bought = current_price
            avg_sold = 0.0
            buy_commission = transaction.commission
            sell_commission = 0.0
        else:
            buy_quantity = 0
            sell_quantity = -1.0 * transaction.quantity
            avg_bought = 0.0
            avg_sold = current_price
            buy_commission = 0.0
            sell_commission = transaction.commission

        return cls(
            asset,
            current_price,
            current_dt,
            buy_quantity,
            sell_quantity,
            avg_bought,
            avg_sold,
            buy_commission,
            sell_commission
        )

    def _check_set_dt(self, dt):

        if dt is not None:
            if (dt < self.current_dt):
                raise ValueError(
                    'Supplied update time of "%s" is earlier than '
                    'the current time of "%s".' % (dt, self.current_dt)
                )
            else:
                self.current_dt = dt

    @property
    def direction(self):
        if self.net_quantity == 0:
            return 0
        else:
            return np.copysign(1, self.net_quantity)

    @property
    def market_value(self):
        return self.current_price * self.net_quantity

    @property
    def avg_price(self):
        if self.net_quantity == 0:
            return 0.0
        elif self.net_quantity > 0:
            return (self.avg_bought * self.buy_quantity + self.buy_commission) / self.buy_quantity
        else:
            return (self.avg_sold * self.sell_quantity - self.sell_commission) / self.sell_quantity

    @property
    def net_quantity(self):
        return self.buy_quantity - self.sell_quantity

    @property
    def total_bought(self):
        return self.avg_bought * self.buy_quantity

    @property
    def total_sold(self):
        return self.avg_sold * self.sell_quantity

    @property
    def net_total(self):
        return self.total_sold - self.total_bought

    @property
    def commission(self):
        return self.buy_commission + self.sell_commission

    @property
    def net_incl_commission(self):
        return self.net_total - self.commission

    @property
    def realised_pnl(self):
        if self.direction == 1:
            if self.sell_quantity == 0:
                return 0.0
            else:
                return (
                    ((self.avg_sold - self.avg_bought) * self.sell_quantity) -
                    ((self.sell_quantity / self.buy_quantity) * self.buy_commission) -
                    self.sell_commission
                )
        elif self.direction == -1:
            if self.buy_quantity == 0:
                return 0.0
            else:
                return (
                    ((self.avg_sold - self.avg_bought) * self.buy_quantity) -
                    ((self.buy_quantity / self.sell_quantity) * self.sell_commission) -
                    self.buy_commission
                )
        else:
            return self.net_incl_commission

    @property
    def unrealised_pnl(self):
        return (self.current_price - self.avg_price) * self.net_quantity

    @property
    def total_pnl(self):
        return self.realised_pnl + self.unrealised_pnl

    def update_current_price(self, market_price, dt=None):
        self._check_set_dt(dt)

        if market_price <= 0.0:
            raise ValueError(
                'Market price "%s" of asset "%s" must be positive to '
                'update the position.' % (market_price, self.asset)
            )
        else:
            self.current_price = market_price

    def _transact_buy(self, quantity, price, commission):
        self.avg_bought = ((self.avg_bought * self.buy_quantity) + (quantity * price)) / (self.buy_quantity + quantity)
        self.buy_quantity += quantity
        self.buy_commission += commission

    def _transact_sell(self, quantity, price, commission):
        self.avg_sold = ((self.avg_sold * self.sell_quantity) + (quantity * price)) / (self.sell_quantity + quantity)
        self.sell_quantity += quantity
        self.sell_commission += commission

    def transact(self, transaction):
        if self.asset != transaction.asset:
            raise ValueError(
                'Failed to update Position with asset %s when '
                'carrying out transaction in asset %s. ' % (
                    self.asset, transaction.asset
                )
            )

        # Nothing to do if the transaction has no quantity
        if int(floor(transaction.quantity)) == 0:
            return

        # Depending upon the direction of the transaction
        # ensure the correct calculation is called
        if transaction.quantity > 0:
            self._transact_buy(
                transaction.quantity,
                transaction.price,
                transaction.commission
            )
        else:
            self._transact_sell(
                -1.0 * transaction.quantity,
                transaction.price,
                transaction.commission
            )

        # Update the current trade information
        self.update_current_price(transaction.price, transaction.dt)
        self.current_dt = transaction.dt
