####TC####

import numpy as np

from qstrader.asset.universe.universe_multi_currency import Universe_Multi_Currency
from qstrader.asset.equity_cur import Equity_Cur
from qstrader.asset.cash_cur import Cash_Cur

class StaticMultiCurrencyUniverse(Universe_Multi_Currency):

    def __init__(self, equity_tuple_list, currency_list=None):
        
        self.equity_dict = dict((a, Equity_Cur(a,a,currency=b)) for (a,b) in equity_tuple_list)
        
        cash_list = [b for (a,b) in equity_tuple_list]
        if currency_list is not None:
            cash_list = cash_list + currency_list
        cash_list = np.unique(cash_list)

        self.cash_dict = dict((c, Cash_Cur(c)) for c in cash_list)      
    
    def get_equity_assets(self, dt):
        return list(self.equity_dict.keys())

    def get_cash_assets(self, dt):
        return list(self.cash_dict.keys())

    def get_equity_asset_currency(self, name):
        return self.equity_dict[name].get_currency()
        
