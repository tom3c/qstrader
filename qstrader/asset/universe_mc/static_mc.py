####TC####

import numpy as np

from qstrader.asset.universe_mc.universe_mc import Universe_MC
from qstrader.asset.asset_mc.equity_mc import Equity_MC
from qstrader.asset.asset_mc.cash_mc import Cash_MC

class StaticUniverse_MC(Universe_MC):

    def __init__(self, equity_tuple_list, currency_list=None):
        
        self.equity_dict = dict((a, Equity_MC(a,a,currency=b)) for (a,b) in equity_tuple_list)
        
        cash_list = [b for (a,b) in equity_tuple_list]
        if currency_list is not None:
            cash_list = cash_list + currency_list
        cash_list = np.unique(cash_list)

        self.cash_dict = dict((c, Cash_MC(c)) for c in cash_list)      
    
    def get_assets(self, dt):
        return list(self.equity_dict.keys()) + list(self.cash_dict.keys())

    def get_equity_assets(self, dt):
        return list(self.equity_dict.keys())

    def get_cash_assets(self, dt):
        return list(self.cash_dict.keys())

    def get_equity_asset_currency(self, name):
        return self.equity_dict[name].get_currency()
        
