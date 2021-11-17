####TC####

from qstrader.asset.asset_mc.asset_mc import Asset_MC

class Equity_MC(Asset_MC):

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
