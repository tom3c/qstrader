##TC##

from abc import ABCMeta, abstractmethod

class Asset_Cur(object):
    """
    Generic asset class that stores meta data about a trading asset.
    """

    __metaclass__ = ABCMeta


    @abstractmethod
    def get_currency(self):
        """
        All assets have a currency
        """
        raise NotImplementedError("Should implement update()")
