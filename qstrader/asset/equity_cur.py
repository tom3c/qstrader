####TC####

from qstrader.asset.asset_cur import Asset_Cur

class Equity_Cur(Asset_Cur):

    def __init__(
        self,
        name,
        symbol,
        tax_exempt=True,
        currency='USD'
    ):
        self.cash_like = False
        self.name = name
        self.symbol = symbol
        self.tax_exempt = tax_exempt
        self.currency = currency

    def get_currency(self):
        return self.currency

    def __repr__(self):
        """
        String representation of the Equity Asset.
        """
        return (
            "Equity(name='%s', symbol='%s', tax_exempt=%s, currency=%s)" % (
                self.name, self.symbol, self.tax_exempt, self.currency
            )
        )
