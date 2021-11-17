import uuid

import numpy as np


class Order_MC(object):

    ##TC Added currency parameter.  Need to specify currency and if none
    ##then defaults to asset currency denomination or for fx base currency

    def __init__(
        self,
        order_type, ##TC#
        dt,
        asset,
        quantity,
        commission=0.0,
        currency=None, ##TC#
        auto_fx=False, ##TC#
        order_id=None
    ):
        self.created_dt = dt
        self.order_type = order_type
        self.cur_dt = dt
        self.asset = asset
        self.quantity = quantity
        self.commission = commission
        self.currency = currency
        self.direction = np.copysign(1, self.quantity)
        self.order_id = self._set_or_generate_order_id(order_id)

    def _order_attribs_equal(self, other):
        """
        Asserts whether all attributes of the Order are equal
        with the exception of the order ID.

        Used primarily for testing that orders are generated correctly.

        """
        if self.created_dt != other.created_dt:
            return False
        if self.order_type != other.order_type:
            return False
        if self.cur_dt != other.cur_dt:
            return False
        if self.asset != other.asset:
            return False
        if self.quantity != other.quantity:
            return False
        if self.commission != other.commission:
            return False
        if self.currency != other.currency:
            return False
        if self.direction != other.direction:
            return False
        return True

    def __repr__(self):

        return (
            "Order(dt='%s', asset='%s', quantity=%s, "
            "commission=%s, currency=%s,  direction=%s, order_id=%s)" % (
                self.created_dt, self.asset, self.quantity,
                self.commission, self.currency, self.direction, self.order_id
            )
        )

    def _set_or_generate_order_id(self, order_id=None):
        if order_id is None:
            return uuid.uuid4().hex
        else:
            return order_id
