import pandas as pa
import unittest
from functions import *
from pandas.testing import assert_index_equal


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

def test_size_fix():
    #arrange
    mock_individual_items = ['Babyccino', '0.0', 'Speciality Tea - Camomile', '1.3', 'Regular', 'Cappuccino', '2.15', 'Regular', 'Americano', '1.95', 'Regular', 'Flavoured latte - Hazelnut', '2.55']
    expected = ['Babyccino', '0.0', 'Speciality Tea - Camomile', '1.3', 'Regular-Cappuccino', '2.15', 'Regular-Americano', '1.95', 'Regular-Flavoured latte - Hazelnut', '2.55']
    #act
    actual = size_fix(mock_individual_items)
    #assert
    tc = unittest.TestCase()
    tc.assertListEqual(expected, actual)

def test_remove_personal_information_returns_card_provider_without_identifiable_numbers():
    #arrange 
    data_test_csv = "C:/Users/saj19/Documents/Generation/groupproject/team-3-project-main/src/data_test" ##ammend this file path to your local path
    expected = pa.DataFrame(columns=["datetime","location","product","payment_method","amount_paid","order_id","card_provider"])
    mock_row = {"datetime": NULL,"location": NULL,"product": NULL,"payment_method": NULL,"amount_paid": NULL,"order_id": NULL,"card_provider": "americanexpress"}
    expected = expected.append(mock_row, ignore_index= True)
    expected = expected["card_provider"][0]
    actual = remove_personal_info(data_test_csv)
    actual = str(actual["card_provider"][0])
    print(expected)
    print(actual)
    assert expected == actual

def test_remove_personal_info_returns_correct_columns():
    #arrange 
    data_test_csv = "C:/Users/saj19/Documents/Generation/groupproject/team-3-project-main/src/data_test"  ##ammend this file path to your local path
    expected = pa.DataFrame(columns=["datetime","location","product","payment_method","amount_paid","order_id","card_provider"])#mock
    expected = expected.columns
    actual = remove_personal_info(data_test_csv)
    actual = actual.columns
    print(expected)
    print(actual)
    #assert expected == actual
    assert_index_equal(expected, actual, check_names =True)
    
def test_contains_digit():
    #Arrange
    mock_string = "1 Apple"
    expected = True
    #Actual 
    actual = contains_digit(mock_string)
    #Assert
    assert expected == actual
