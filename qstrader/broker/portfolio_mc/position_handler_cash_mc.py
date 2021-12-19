from collections import OrderedDict

from qstrader.broker.portfolio_mc.position_mc_cash import Position_MC_Cash

##TC - Need to add logic that creates new position if direction changes

class PositionHandler_Cash_MC(object):
    """
    A class that keeps track of, and updates, the current
    list of Position instances stored in a Portfolio entity.
    """

    def __init__(self):
        self.positions = OrderedDict()

    def transact_cash_position(self, transaction):
        asset = transaction.asset
        if asset in self.positions:
            self.positions[asset].transact(transaction)
        else:
            position = Position_MC_Cash.open_from_transaction(transaction)
            self.positions[asset] = position

        # If the position has zero quantity remove it
        if self.positions[asset].net_quantity == 0:
            del self.positions[asset]

    def total_cash_market_value_base(self):
        return sum(
            pos.market_value_base for asset, pos in self.positions.items()
        )

    def total_cash_market_value_local(self, currency):
        if currency in self.positions.keys():
            return self.positions[currency].market_value_local
        else:
            return 0.0

    def total_cash_unrealised_pnl_base(self):
        return sum(
            pos.unrealised_pnl_base for asset, pos in self.positions.items()
        )

    def total_cash_unrealised_pnl_local(self, currency):
        if currency in self.positions.keys():
            return self.positions[currency].unrealised_pnl_local
        else:
            return 0.0

    def total_cash_realised_pnl_local(self, currency):
        if currency in self.positions.keys():
            return self.positions[currency].realised_pnl_local
        else:
            return 0.0

    def total_cash_pnl_local(self, currency):
        if currency in self.positions.keys():
            return self.positions[currency].total_pnl_local
        else:
            return 0.0