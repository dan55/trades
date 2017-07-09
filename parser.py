import os, pdb

from collections import defaultdict

class Order():

    def __init__(self, order_id, stock_symbol=None, quantity=None):
        self.order_id = order_id
        self.stock_symbol = stock_symbol
        self.quantity = quantity


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
        quantity = self.splice_msg(QUANTITY_OFFSET, QUANTITY_LEN)

        return Order(order_id, stock_symbol, quantity)

    def is_msg_an_add_order(self, order_type_char):
        return order_type_char == 'A'

    def is_msg_a_cancel_order(self, order_type_char):
        return order_type_char == 'X'

    def is_msg_an_exec_order(self, order_type_char):
        return order_type_char == 'E'

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
            self.open_orders[order_id] = self.get_order_obj(msg, order_id)
        
        # if it's and execution order, remove it from dict of open orders,
        # and record the volume executed
        elif self.is_msg_an_exec_order(order_str):
            try:
                order = open_orders.pop(order_id)
                self.executed_orders[order.stock_symbol] += order.quantity 
            except KeyError:
                print 'An execute order (%s) has no corresponding add order.' % str(order_id)

        # if it's a cancel order, simply remove it from dict of open orders
        elif self.is_msg_a_cancel_order(order_str):
            try:
                order = open_orders.pop(order_id)
            except KeyError:
                print 'A cancel order (%s) has no corresponding add order.' % str(order_id)
      
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

'''
open_orders, executed_orders = {}, defaultdict(int)

ORDER_TYPE_OFFSET = 8

ORDER_ID_OFFSET = ORDER_TYPE_OFFSET + 1
ORDER_ID_LEN = 12

STOCK_SYMBOL_OFFSET = 28
STOCK_SYMBOL_LEN = 6

QUANTITY_OFFSET = 22
QUANTITY_LEN = 6

ADD_ORDER_DELIMITER = 'A'
CANCEL_ORDER_DELIMITER = 'X'
EXECUTED_ORDER_DELIMITER = 'E'

trades_added = 0
trades_cancelled = 0
trades_executed = 0
trades_other = 0

with open('pitch_example_data', 'r') as f:
    while True:
        line = f.readline()

        if line:
            # remove the newline char and, per the instructions, 
            # ignore the initial 'S'
            order_str = line.rstrip()[1:]

            order_type = order_str[ORDER_TYPE_OFFSET]

            order_id = order_str[ORDER_ID_OFFSET : ORDER_ID_OFFSET + ORDER_ID_LEN]

            if order_type == ADD_ORDER_DELIMITER:
                trades_added += 1

                stock_symbol = order_str[STOCK_SYMBOL_OFFSET : STOCK_SYMBOL_OFFSET + STOCK_SYMBOL_LEN].rstrip()

                quantity = int(order_str[QUANTITY_OFFSET : QUANTITY_OFFSET + QUANTITY_LEN])

                open_orders[order_id] = Order(order_id, stock_symbol, quantity) 
            elif order_type == CANCEL_ORDER_DELIMITER:
                trades_cancelled += 1

                try:
                    order = open_orders.pop(order_id)
                except KeyError:
                    print 'A cancel order (%s) has no corresponding add order.' % str(order_id)
            elif order_type == EXECUTED_ORDER_DELIMITER:
                trades_executed += 1

                try:
                    order = open_orders.pop(order_id)
                    executed_orders[order.stock_symbol] += order.quantity 
                except KeyError:
                    print 'An execute order (%s) has no corresponding add order.' % str(order_id)
            else:
                trades_other += 1
        else:
            break

print trades_added
print trades_cancelled
print trades_executed
print trades_other

print trades_added + trades_cancelled + trades_executed + trades_other

#print executed_orders

for ticker, quantity in sorted(executed_orders.items(), key=lambda k: k[1], reverse=True)[:10]:
    print ticker, quantity 
'''