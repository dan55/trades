import os, pdb

from collections import defaultdict

class Order():

    def __init__(self, order_id, stock_symbol=None, quantity=None):
        self.order_id = order_id
        self.stock_symbol = stock_symbol
        self.quantity = quantity

    def __eq__(self, other):
        return self.order_id == other.order_id and \
            self.stock_symbol == other.stock_symbol and \
            self.quantity == other.quantity 


class PitchParser():

    def __init__(self, data_file=None):
        if data_file:
            self.file = os.path.abspath(data_file)

        self.open_orders = {}
        self.executed_orders = defaultdict(int)

    def get_order_id_from_msg(self, msg):
        ORDER_ID_OFFSET = 9
        ORDER_ID_LEN = 12

        return self.splice_msg(msg, ORDER_ID_OFFSET, ORDER_ID_LEN)

    def get_order_type_char_from_msg(self, msg):
        ORDER_TYPE_OFFSET = 8
        ORDER_TYPE_LEN = 1

        return self.splice_msg(msg, ORDER_TYPE_OFFSET, ORDER_TYPE_LEN)

    def get_quantity_from_msg(self, msg):
        QUANTITY_OFFSET = 22
        QUANTITY_LEN = 6

        return int(self.splice_msg(msg, QUANTITY_OFFSET, QUANTITY_LEN))

    def get_stock_symbol_from_msg(self, msg):
        STOCK_SYMBOL_OFFSET = 28
        STOCK_SYMBOL_LEN = 6

        return self.splice_msg(msg, STOCK_SYMBOL_OFFSET, STOCK_SYMBOL_LEN).rstrip()        

    def get_order_obj(self, msg, order_id):
        stock_symbol = self.get_stock_symbol_from_msg(msg)
        quantity = self.get_quantity_from_msg(msg)

        return Order(order_id, stock_symbol, quantity)

    def is_msg_an_add_order(self, msg):
        return self.get_order_type_char_from_msg(msg) == 'A'

    def is_msg_a_cancel_order(self, msg):
        return self.get_order_type_char_from_msg(msg) == 'X'

    def is_msg_an_exec_order(self, msg):
        return self.get_order_type_char_from_msg(msg) == 'E'

    def parse(self):
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
        # all messages have an order_id
        order_id = self.get_order_id_from_msg(order_str)

        # if msg is an add order, add it to dict of open orders
        if self.is_msg_an_add_order(order_str):
            self.open_orders[order_id] = self.get_order_obj(order_str, order_id)
        
        # if it's an execution order, remove it from dict of open orders,
        # and record the volume executed
        elif self.is_msg_an_exec_order(order_str):
            try:
                order = self.open_orders.pop(order_id)

                self.executed_orders[order.stock_symbol] += order.quantity 
            except KeyError:
                #print 'An execute order (%s) has no corresponding add order.' % str(order_id)
                pass

        # if it's a cancel order, simply remove the order from dict of open orders, if it exists
        elif self.is_msg_a_cancel_order(order_str):    
            order = self.open_orders.pop(order_id, None)
      
        # otherwise, ignore the message
        else:
            pass

    def splice_msg(self, msg, offset, msg_len):
        return msg[offset : offset + msg_len]

    def print_total_volume(self):
        for ticker, quantity in sorted(self.executed_orders.items(), key=lambda k: k[1], reverse=True)[:10]:
            print ticker, quantity 


if __name__ == '__main__':

    parser = PitchParser('pitch_example_data')
    parser.parse()
    parser.print_total_volume()