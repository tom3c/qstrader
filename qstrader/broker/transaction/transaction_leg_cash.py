import numpy as np

##TC - Transaction to adjust cash on the back of stock purchase or subscriptions & redemptions
# These are purely accounting transactions so have a value in the asset/currency 
# and always zero commissions
class Transaction_Cash(object):

    def __init__(
        self,
        asset,
        quantity,
        dt,
        #price,
        order_id,
        #commission=0.0
    ):
        self.asset = asset
        self.quantity = quantity
        self.direction = np.copysign(1, self.quantity)
        self.dt = dt
        #self.price = price
        self.order_id = order_id
        #self.commission = commission

    ## TC price removed from below
    def __repr__(self):
        return "%s(asset=%s, quantity=%s, dt=%s, " \
            "order_id=%s)" % (
                type(self).__name__, self.asset,
                self.quantity, self.dt,
                self.order_id
            )

    @property
    def cost_without_commission(self):
        return self.quantity #* self.price

    @property
    def cost_with_commission(self):
        #if self.commission == 0.0:
        return self.cost_without_commission
        #else:
        #    return self.cost_without_commission + self.commission
