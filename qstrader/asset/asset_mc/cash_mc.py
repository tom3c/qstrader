##TC##

from qstrader.asset.asset_mc.asset_mc import Asset_MC

class Cash_MC(Asset_MC):

    def __init__(self, currency='USD'):
        self.cash_like = True
        self.currency = currency

    def get_currency(self):
        return self.currency
