import numpy as np

##TC - FX Transaction, changing one currency for another.  Needs to be completed
class Transaction_FX(object):

    def __init__(
        self,
        asset,
        quantity,
        dt,
        price,
        order_id,
        commission=0.0
    ):
        self.asset = asset
        self.quantity = quantity
        self.direction = np.copysign(1, self.quantity)
        self.dt = dt
        self.price = price
        self.order_id = order_id
        self.commission = commission

    def __repr__(self):
        return "%s(asset=%s, quantity=%s, dt=%s, " \
            "price=%s, order_id=%s)" % (
                type(self).__name__, self.asset,
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
