import os, sys

from collections import defaultdict

from Order import Order


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


    # methods for determining message type

    def is_msg_an_add_order(self, msg):
        return self.get_order_type_char_from_msg(msg) == 'A'

    def is_msg_a_cancel_order(self, msg):
        return self.get_order_type_char_from_msg(msg) == 'X'

    def is_msg_an_exec_order(self, msg):
        return self.get_order_type_char_from_msg(msg) == 'E'

    def is_msg_a_trade_order(self, msg):
        return self.get_order_type_char_from_msg(msg) == 'P'


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

    def get_quantity_from_cancel_msg(self, msg):
        return self.get_quantity_from_exec_msg(msg)

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


    # methods for processing the messages. TODO: Move these to a new class.

    def decrement_order_quantity(self, order, amt):
        ''' Updates an order quantity, removing the order from the
            dict of open orders if it is wholly executed or cancelled. 
        '''

        order.quantity -= amt

        if order.quantity > 0:
            self.open_orders[order.order_id] = order

    def process_add_order(self, order_id, msg):
        self.open_orders[order_id] = self.get_order_obj(msg, order_id)

    def process_cancel_order(self, order_id, msg):
        ''' Updates the order according to number of shares cancelled. '''

        try:
            order = self.open_orders.pop(order_id)
            shares_cancelled = self.get_quantity_from_cancel_msg(msg)

            self.decrement_order_quantity(order, shares_cancelled)
        except KeyError:
            pass

    def process_execute_order(self, order_id, msg):
        ''' Records the executed volume and updates open orders according to whether 
            the trade was executed in whole or in part.
        '''

        try:
            order = self.open_orders.pop(order_id)
            actual_shares_traded = self.get_quantity_from_exec_msg(msg)

            self.decrement_order_quantity(order, actual_shares_traded)

            self.executed_orders[order.stock_symbol] += actual_shares_traded
        except KeyError:
            pass

    def process_trade_order(self, order_id, msg):
        ''' Record the executed volume. There is no corresponding add order. '''

        ticker = self.get_stock_symbol_from_msg(msg)
        quantity = self.get_quantity_from_trade_msg(msg)

        self.executed_orders[ticker] += quantity

    def process_msg(self, order_str):
        ''' Tracks order executions. '''

        # all messages have an order_id
        order_id = self.get_order_id_from_msg(order_str)

        if self.is_msg_an_add_order(order_str):
            self.process_add_order(order_id, order_str)

        elif self.is_msg_an_exec_order(order_str):
            self.process_execute_order(order_id, order_str)

        elif self.is_msg_a_cancel_order(order_str):    
            self.process_cancel_order(order_id, order_str)

        elif self.is_msg_a_trade_order(order_str):
            self.process_trade_order(order_id, order_str)

        else:
            pass


    # methods for outputting results

    def print_total_volume(self):
        ''' Output trade volumes to stdout. '''

        TICKER_MAX_LEN = 8 

        for ticker, quantity in self.sort_by_volume(self.executed_orders)[:10]:
            print ticker.ljust(TICKER_MAX_LEN), quantity 

    def sort_by_volume(self, executed_orders):
        ''' Sorts executed trades by volume. '''

        return sorted(executed_orders.items(), key=lambda k: k[1], reverse=True)


if __name__ == '__main__':
    try:
        data_file = sys.argv[1]
    except IndexError:
        print('Usage: challenge_2.py data_file')
        sys.exit(-1)


    parser = PitchParser(data_file)
    parser.parse()
    parser.print_total_volume()