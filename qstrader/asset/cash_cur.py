##TC##

from qstrader.asset.asset_cur import Asset_Cur


class Cash_Cur(Asset_Cur):

    def __init__(self, currency='USD'):
        self.cash_like = True
        self.currency = currency

    def get_currency(self):
        return self.currency
