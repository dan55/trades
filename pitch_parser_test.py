import unittest

from parser import Order, PitchParser 

class PitchParserTest(unittest.TestCase):
    def setUp(self):
        #test_data_file = 'pitch_test_data.txt'

        self.parser = PitchParser()
    
        self.sample_add_order_msg = '28800162A1K27GA00000VB001234AAPL  0001828600Y'
        self.expected_order_id = '1K27GA00000V'
        self.expected_quantity = 1234
        self.expected_symbol = 'AAPL'
        
        self.sample_add_order = Order(
            self.expected_order_id, 
            self.expected_symbol,
            self.expected_quantity
        )

        self.sample_execute_order_msg = '28884189E1K27GA00000V001234'

    def test_order_id_parsing(self):
        parsed_order_id = self.parser.get_order_id_from_msg(self.sample_add_order_msg)

        self.assertEqual(parsed_order_id, self.expected_order_id)

    def test_order_obj(self):
        order = self.parser.get_order_obj(self.sample_add_order_msg, self.expected_order_id)

        self.assertEqual(order, self.sample_add_order)

    def test_quantity_parsing(self):
        parsed_quantity = self.parser.get_quantity_from_msg(self.sample_add_order_msg)

        self.assertEqual(parsed_quantity, self.expected_quantity)

    def test_stock_symbol_parsing(self):
        parsed_symbol = self.parser.get_stock_symbol_from_msg(self.sample_add_order_msg)

        self.assertEqual(parsed_symbol, self.expected_symbol)


    def test_add_order_processing(self):
        expected_num_open_orders = 1

        # process the message
        self.parser.process_msg(self.sample_add_order_msg)

        # the order and nothing else is added to dict of open orders
        self.assertEqual(len(self.parser.open_orders), expected_num_open_orders)
        
        open_order = self.parser.open_orders[self.expected_order_id]

        self.assertEqual(open_order, self.sample_add_order)

    def test_execute_order_processing(self):
        expected_num_open_orders = 0
        expected_num_executed_orders = 1
        expected_executed_dict = {self.expected_symbol: self.expected_quantity}

        # add an order and then execute it
        self.parser.process_msg(self.sample_add_order_msg)
        self.parser.process_msg(self.sample_execute_order_msg)

        self.assertEqual(len(self.parser.open_orders), expected_num_open_orders)
        
        self.assertEqual(len(self.parser.executed_orders), expected_num_executed_orders)
        self.assertEqual(self.parser.executed_orders, expected_executed_dict)

if __name__ == '__main__':
    unittest.main()