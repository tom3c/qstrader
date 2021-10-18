from collections import OrderedDict

from qstrader.broker.portfolio.position import Position


class PositionCashHandler(object):
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
            position = Position.open_from_transaction(transaction)
            self.positions[asset] = position

        # If the position has zero quantity remove it
        if self.positions[asset].net_quantity == 0:
            del self.positions[asset]

    def total_cash_market_value(self):
        return sum(
            pos.market_value
            for asset, pos in self.positions.items()
        )

    def total_cash_unrealised_pnl(self):
        return sum(
            pos.unrealised_pnl
            for asset, pos in self.positions.items()
        )

    def total_cash_realised_pnl(self):
        return sum(
            pos.realised_pnl
            for asset, pos in self.positions.items()
        )

    def total_cash_pnl(self):
        return sum(
            pos.total_pnl
            for asset, pos in self.positions.items()
        )
