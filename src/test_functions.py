import pandas as pa
from functions import *

def test_clean_products():
    initial = ['Large,Latte,2.5',',Green tea,1.9','Regular,Hot chocolate,1.4,Regular,Tea,1.5']
    expected = [['Large-Latte'], ['Green tea'], ['Regular-Hot chocolate', 'Regular-Tea']]
    result = clean_products(initial)
    assert result == expected

def test_return_item_tuples():
    initial = [['Large-Latte'], ['Green tea'], ['Regular-Hot chocolate', 'Regular-Tea']]
    expected = [(0, 'Large-Latte'), (1, 'Green tea'), (2, 'Regular-Hot chocolate'), (2, 'Regular-Tea')]
    result = return_item_tuples(initial)
    assert result == expected

def test_write_into_dataframe():
    initial = [(0, 'Large-Latte'), (1, 'Green tea'), (2, 'Regular-Hot chocolate'), (2, 'Regular-Tea')]
    dictionary = {'product': ['Large-Latte', 'Green tea', 'Regular-Hot chocolate', 'Regular-Tea'], 'order_id': [0, 1, 2, 2]}
    expected = pa.DataFrame(data=dictionary)
    result = write_into_dataframe(initial)
    pa.testing.assert_frame_equal(expected, result)