import os

from collections import defaultdict

class Order():
    ''' Encapsulates order information. Currently used only for Add Orders. '''

    def __init__(self, order_id, stock_symbol=None, quantity=None):
        self.order_id = order_id
        self.stock_symbol = stock_symbol
        self.quantity = quantity

    def __eq__(self, other):
        return self.order_id == other.order_id and \
            self.stock_symbol == other.stock_symbol and \
            self.quantity == other.quantity 


class PitchParser():
    '''Parses PITCH messages and tracks executed orders to determine volumes traded.
    
    Attributes:

        file - the file in which to find the messages to parse.
        open_orders - a dictionary of the form {order_id : order object}
        executed_orders - a dictionary of the form {ticker : trade volume}
    '''

    def __init__(self, data_file=None):

        if data_file:
            self.file = os.path.abspath(data_file)

        self.open_orders = {}
        self.executed_orders = defaultdict(int)


    # methods for parsing the messages, per the spec

    def get_order_id_from_msg(self, msg):
        ORDER_ID_OFFSET = 9
        ORDER_ID_LEN = 12

        return self.splice_msg(msg, ORDER_ID_OFFSET, ORDER_ID_LEN)

    def get_order_type_char_from_msg(self, msg):
        ORDER_TYPE_OFFSET = 8
        ORDER_TYPE_LEN = 1

        return self.splice_msg(msg, ORDER_TYPE_OFFSET, ORDER_TYPE_LEN)

    def get_quantity_from_add_msg(self, msg):
        QUANTITY_OFFSET = 22
        QUANTITY_LEN = 6

        return int(self.splice_msg(msg, QUANTITY_OFFSET, QUANTITY_LEN))

    def get_quantity_from_exec_msg(self, msg):
        QUANTITY_OFFSET = 21
        QUANTITY_LEN = 6

        return int(self.splice_msg(msg, QUANTITY_OFFSET, QUANTITY_LEN))

    def get_quantity_from_trade_msg(self, msg):
        return self.get_quantity_from_add_msg(msg)

    def get_stock_symbol_from_msg(self, msg):
        STOCK_SYMBOL_OFFSET = 28
        STOCK_SYMBOL_LEN = 6

        return self.splice_msg(msg, STOCK_SYMBOL_OFFSET, STOCK_SYMBOL_LEN).rstrip()        

    def get_order_obj(self, msg, order_id):
        ''' Construct an order object '''

        stock_symbol = self.get_stock_symbol_from_msg(msg)
        quantity = self.get_quantity_from_add_msg(msg)

        return Order(order_id, stock_symbol, quantity)

    def splice_msg(self, msg, offset, msg_len):
        return msg[offset : offset + msg_len]


    # methods for determining message type

    def is_msg_an_add_order(self, msg):
        return self.get_order_type_char_from_msg(msg) == 'A'

    def is_msg_a_cancel_order(self, msg):
        return self.get_order_type_char_from_msg(msg) == 'X'

    def is_msg_an_exec_order(self, msg):
        return self.get_order_type_char_from_msg(msg) == 'E'

    def is_msg_a_trade_order(self, msg):
        return self.get_order_type_char_from_msg(msg) == 'P'


    def parse(self):
        ''' Reads a file containing messages, processing each line. '''

        with open(self.file, 'r') as f:    
            while True:
                line = f.readline()

                if line:
                    # remove the newline char and, per the instructions, 
                    # ignore the initial 'S'
                    order_str = line.rstrip()[1:]

                    self.process_msg(order_str)
                else:
                    break

    def process_msg(self, order_str):
        ''' Tracks order executions. '''

        # all messages have an order_id
        order_id = self.get_order_id_from_msg(order_str)

        # if msg is an add order, add it to dict of open orders
        if self.is_msg_an_add_order(order_str):
            self.open_orders[order_id] = self.get_order_obj(order_str, order_id)
        
        # if it's an execution order, remove it from dict of open orders, and record the volume executed
        elif self.is_msg_an_exec_order(order_str):
            try:
                order = self.open_orders.pop(order_id)
                actual_shares_traded = self.get_quantity_from_exec_msg(order_str)

                self.executed_orders[order.stock_symbol] += actual_shares_traded
            except KeyError:
                pass

        # if it's a trade order, record the volume executed (there is no corresponding add order)
        elif self.is_msg_a_trade_order(order_str):
            ticker = self.get_stock_symbol_from_msg(order_str)
            quantity = self.get_quantity_from_trade_msg(order_str)

            self.executed_orders[ticker] += quantity

        # if it's a cancel order, simply remove the order from dict of open orders, if it exists
        elif self.is_msg_a_cancel_order(order_str):    
            order = self.open_orders.pop(order_id, None)
      
        # otherwise, ignore the message
        else:
            pass

    def print_total_volume(self):
        ''' Output trade volumes to stdout. '''

        TICKER_MAX_LEN = 8 

        for ticker, quantity in sorted(self.executed_orders.items(), key=lambda k: k[1], reverse=True)[:10]:
            print ticker.ljust(TICKER_MAX_LEN), quantity 


if __name__ == '__main__':

    parser = PitchParser('pitch_example_data')
    parser.parse()
    parser.print_total_volume()