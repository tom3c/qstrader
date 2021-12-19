import numpy as np

##TC - Transaction with FX details included
class Transaction_MC(object):

    def __init__(
        self,
        type,
        asset,
        quantity,
        dt,
        price,
        currency,
        fx_rate,
        order_id,
        commission=0.0,
        
    ):
        self.type = type
        self.asset = asset
        self.quantity = quantity
        self.direction = np.copysign(1, self.quantity)
        self.dt = dt
        self.price = price
        self.currency = currency
        self.fx_rate = fx_rate
        self.order_id = order_id
        self.commission = commission
        

    def __repr__(self):

        return "%s(asset=%s, quantity=%s, dt=%s, " \
            "price=%s, fx_rate=%s, order_id=%s)" % (
                type(self).__name__, self.asset,
                self.quantity, self.dt,
                self.price, self.fx_rate, self.order_id
            )

    #Note - comms currency done in trade currency
    @property
    def cost_without_commission(self):

        return self.quantity * self.price

    @property
    def cost_with_commission(self):

        if self.commission == 0.0:
            return self.cost_without_commission
        else:
            return self.cost_without_commission + self.commission
