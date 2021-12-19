import pandas as pd

from qstrader.asset.equity import Equity
from qstrader.broker.transaction.transaction_mc import Transaction_MC


# def test_transaction_representation():
#     """
#     Tests that the Transaction representation
#     correctly recreates the object.
#     """
#     dt = pd.Timestamp('2015-05-06')
#     asset = Equity('Apple, Inc.', 'AAPL')
#     transaction = Transaction_MC(
#         asset, quantity=168, dt=dt, price=56.18, order_id=153
#     )
#     exp_repr = (
#         "Transaction(asset=Equity(name='Apple, Inc.', symbol='AAPL', tax_exempt=True), "
#         "quantity=168, dt=2015-05-06 00:00:00, price=56.18, order_id=153)"
#     )
#     assert repr(transaction) == exp_repr
