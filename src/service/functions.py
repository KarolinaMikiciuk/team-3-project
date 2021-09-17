import pandas as pa
import numpy as np
from datetime import datetime
import psycopg2
import re
#from sqlalchemy import create_engine


# engine = create_engine('postgresql+psycopg2://root:password@localhost:5432/team_3')

def return_start_id(connection):
    with connection.connect() as conn:

        record_object = conn.execute("SELECT * FROM basket ORDER BY order_id DESC LIMIT 1;")
        list_of_rows = record_object.all()
        conn.close()
        
        if len(list_of_rows) == 0:
            return 0
            
        else:
            start = list_of_rows[0]
            start = start[0]
            print(start)
            return start


def contains_digit(string):
    comparison = list(string)
    
    for char in comparison:
        if char.isdigit() == True:
            return True
        elif char.isdigit() == False:
            pass
    return False
    
def size_fix(individual_items):
    
    product_list = []
    i = 0
    for entry in individual_items:
        entry = individual_items[i]
        if (entry == "Large") or (entry == "Regular"):
            join_these = [entry, individual_items[i+1]]
            joined_item = "-".join(join_these)
            product_list.append(joined_item)
            individual_items.pop(i)
            i += 1
        else:
            product_list.append(entry)
            i += 1
            
    return product_list


def remove_personal_info(orders_df):   #legacy
   
    #orders_df = pa.read_csv(io.StringIO(csv_string),names=["datetime","location","customer_name","product","payment_method","amount_paid","card_provider"])
    #orders_df = orders_df.drop(columns="customer_name")
    orders_df['order_id'] = np.arange(orders_df.shape[0])

    credit_card_number_list = orders_df["card_provider"].tolist()
    orders_df = orders_df.drop(columns="card_provider")
    new_list = []
    for entry in credit_card_number_list:
        if entry == "None":
            new_list.append(entry)
        else:
            entry_list = entry.split(",")
            cleaned_entry = entry_list[0]
            new_list.append(cleaned_entry)

    orders_df['card_provider'] = pa.Series(new_list)

    return orders_df
    
def clean_products(items_list):
    full_list = []
    list_of_order_lists = []
    for order in items_list:
        individual_items = order.split(',')
        list_of_order_lists.append(individual_items)
    for order_list in list_of_order_lists:
        new_order_list = []
        for string in order_list:
            no_number_string = re.sub(r'[0-9]+', '', string)
            clean_string = no_number_string.replace(" - .","")
            new_order_list.append(clean_string.strip())
        full_list.append(new_order_list)
    # for order in no_number_list:
    #     order_string = ','.join(order)
    #     full_list.append(order_string)
    return full_list

def return_item_tuples(my_products, start):
    list_of_tuples = []
    if start == 0:
        i = 0
    else:
        i = start + 1
    for order_list in my_products:
        in_list = []
        q = 1
        for item in order_list:
            if item not in in_list:
                individual_tuple = [i,item,q]
                list_of_tuples.append(individual_tuple)
                in_list.append(item)
            elif item in in_list:
                for tuple_list in list_of_tuples:
                    if item in tuple_list and i in tuple_list:
                        q += 1
                        tuple_list[2] = q
        i += 1
    return list_of_tuples

def write_into_dataframe(the_tuples):
    product_list = []
    order_id_list = []
    quantity_list = []
    for product_tuple in the_tuples:
        id = product_tuple[0]
        item = product_tuple[1]
        quantity = product_tuple[2]
        product_list.append(item)
        order_id_list.append(id)
        quantity_list.append(quantity)
    value_dictionary = {"product": product_list, "order_id" : order_id_list, "quantity" : quantity_list}
    df = pa.DataFrame(value_dictionary)
    print(df)
    return df 


def adjust_timestamp(orders_df):
    
    timestamp_list = orders_df['datetime'].tolist()
    for x in range(len(timestamp_list)):
        timestamp_list[x] = timestamp_list[x].replace("/", "-")
        
    orders_df = orders_df.drop(columns="datetime")
    orders_df['datetime'] = pa.Series(timestamp_list)

    for x in range(len(timestamp_list)):
        orders_df['datetime'].values[x] = datetime.strptime(orders_df['datetime'].values[x], '%d-%m-%Y %H:%M')
        orders_df['datetime'].values[x] = datetime.strftime(orders_df['datetime'].values[x],'%Y-%m-%d %H:%M')
        orders_df['datetime'].values[x] = orders_df['datetime'].values[x] + ":00"

    pa.to_datetime(orders_df['datetime'].values)
    
    return orders_df