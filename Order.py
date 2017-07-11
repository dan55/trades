class Order():
    ''' Encapsulates open order information. '''

    def __init__(self, order_id, stock_symbol=None, quantity=None):
        self.order_id = order_id
        self.stock_symbol = stock_symbol
        self.quantity = quantity

    def __eq__(self, other):
        return self.order_id == other.order_id and \
            self.stock_symbol == other.stock_symbol and \
            self.quantity == other.quantity 

    def __str__(self):
        return self.stock_symbol + ': %s' % str(self.quantity)