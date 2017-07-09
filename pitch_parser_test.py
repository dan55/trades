import unittest

from parser import Order, PitchParser 

class PitchParserTest(unittest.TestCase):
    def setUp(self):
        #test_data_file = 'pitch_test_data.txt'

        self.parser = PitchParser()
    
        self.sample_add_order = '28800162A1K27GA00000VB001234AAPL  0001828600Y'

    def test_order_id_parsing(self):
        expected_order_id = '1K27GA00000V'
        parsed_order_id = self.parser.get_order_id_from_msg(self.sample_add_order)

        self.assertEqual(parsed_order_id, expected_order_id)

    def test_quantity_parsing(self):
        expected_quantity = 1234
        parsed_quantity = self.parser.get_quantity_from_msg(self.sample_add_order)

    def test_stock_symbol_parsing(self):
        expected_symbol = 'AAPL'
        parsed_symbol = self.parser.get_stock_symbol_from_msg(self.sample_add_order)

        self.assertEqual(parsed_symbol, expected_symbol)

if __name__ == '__main__':
    unittest.main()