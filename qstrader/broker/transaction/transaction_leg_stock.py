import numpy as np

class Transaction_Leg_Stock(object):

    def __init__(
        self,
        asset,
        currency,
        quantity,
        dt,
        price,
        fx_rate,
        order_id,
        commission=0.0
    ):
        self.asset = asset
        self.currency = currency
        self.quantity = quantity
        self.direction = np.copysign(1, self.quantity)
        self.dt = dt
        self.price = price
        self.fx_rate = fx_rate
        self.order_id = order_id
        self.commission = commission

    def __repr__(self):
        return "%s(asset=%s, currency=%s, quantity=%s, dt=%s, " \
            "price=%s, order_id=%s)" % (
                type(self).__name__, self.asset,
                self.currency,
                self.quantity, self.dt,
                self.price, self.order_id
            )

    @property
    def cost_without_commission(self):
        return self.quantity * self.price

    @property
    def cost_with_commission(self):
        if self.commission == 0.0:
            return self.cost_without_commission
        else:
            return self.cost_without_commission + self.commission
