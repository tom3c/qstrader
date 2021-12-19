from collections import OrderedDict

from qstrader.broker.portfolio_mc.position_mc import Position_MC

##TC - Need to add logic that creates new position if direction changes

class PositionHandler_MC(object):

    def __init__(self):
        self.positions = OrderedDict()

    def transact_position(self, transaction):
        asset = transaction.asset
        if asset in self.positions:
            self.positions[asset].transact(transaction)
        else:
            position = Position_MC.open_from_transaction(transaction)
            self.positions[asset] = position

        # If the position has zero quantity remove it
        if self.positions[asset].net_quantity == 0:
            del self.positions[asset]

    def total_market_value_base(self):
        return sum(
            pos.market_value_base for asset, pos in self.positions.items()
        )

    def total_market_value_local(self, currency):
        pos_list = list(self.positions.values())
        return sum(
            pos.market_value_local for pos in pos_list if (pos.currency == currency)
        )

    def total_unrealised_pnl_base(self):
        return sum(
            pos.unrealised_pnl_base for asset, pos in self.positions.items()
        )

    def total_unrealised_pnl_local(self, currency):
        pos_list = list(self.positions.values())
        return sum(
            pos.unrealised_pnl_local for pos in pos_list if (pos.currency == currency)
        )

    def total_realised_pnl_local(self, currency):
        pos_list = list(self.positions.values())
        return sum(
            pos.realised_pnl_local for pos in pos_list if (pos.currency == currency)
        )

    def total_pnl_local(self, currency):
        pos_list = list(self.positions.values())
        return sum(
            pos.total_pnl_local for pos in pos_list if (pos.currency == currency)
        )
