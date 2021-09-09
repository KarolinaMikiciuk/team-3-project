import pandas as pa
from src.service.functions import clean_products, return_item_tuples, write_into_dataframe, return_start_id
from sqlalchemy import create_engine 
import numpy as np 


def the_etl_pipe_function(orders_df,connection):

    orders_df['order_id'] = np.arange(orders_df.shape[0])

    basket_table = pa.concat([orders_df['product'], orders_df['order_id']], axis=1, keys=['product', 'order_id'])
    orders_df = orders_df.drop(columns="product")

    items_list = basket_table["product"].tolist()

    my_products = clean_products(items_list)
    the_tuples = return_item_tuples(my_products)
    basket_table_data = write_into_dataframe(the_tuples)

    first_id = return_start_id()
    orders_df['order_id'] = range(first_id, first_id+len(orders_df))
    
    orders_df.to_sql('orders', connection,if_exists="append",index=False)
    basket_table_data.to_sql('basket', connection, if_exists="append",index=False)
