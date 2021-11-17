class PortfolioEvent_MC(object):

    def __init__(
        self,
        dt,
        type,
        description,
        currency,
        debit,
        credit,
        balance
    ):
        self.dt = dt
        self.type = type
        self.description = description
        self.currency = currency
        self.debit = debit
        self.credit = credit
        self.balance = balance

    def __eq__(self, other):
        if self.dt != other.dt:
            return False
        if self.type != other.type:
            return False
        if self.description != other.description:
            return False
        if self.currency != other.currency:
            return False
        if self.debit != other.debit:
            return False
        if self.credit != other.credit:
            return False
        if self.balance != other.balance:
            return False
        return True

    def __repr__(self):
        return (
            "PortfolioEvent(dt=%s, type=%s, description=%s, "
            "currency=%s, debit=%s, credit=%s, balance=%s)" % (
                self.dt, self.type, self.description, self.currency,
                self.debit, self.credit, self.balance
            )
        )

    @classmethod
    def create_subscription(cls, dt, currency, credit, balance):
        return cls(
            dt, type='subscription', description='SUBSCRIPTION', currency= currency,
            debit=0.0, credit=round(credit, 2), balance=round(balance, 2)
        )

    @classmethod
    def create_withdrawal(cls, dt, currency, debit, balance):
        return cls(
            dt, type='withdrawal', description='WITHDRAWAL', currency= currency,
            debit=round(debit, 2), credit=0.0, balance=round(balance, 2)
        )

    def to_dict(self):
        return {
            'dt': self.dt,
            'type': self.type,
            'description': self.description,
            'currency' : self.currency,
            'debit': self.debit,
            'credit': self.credit,
            'balance': self.balance
        }
