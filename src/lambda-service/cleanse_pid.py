import pandas as pa
from functions import remove_personal_info, clean_products, return_item_tuples, write_into_dataframe
from sqlalchemy import create_engine  


orders_df = remove_personal_info("isle_of_wight")

basket_table = pa.concat([orders_df['product'], orders_df['order_id']], axis=1, keys=['product', 'order_id'])
orders_df = orders_df.drop(columns="product")

items_list = basket_table["product"].tolist()

my_products = clean_products(items_list)
the_tuples = return_item_tuples(my_products)
basket_table_data = write_into_dataframe(the_tuples)

#8080 is just the adminer , not the database
engine = create_engine('postgresql+psycopg2://root:password@localhost:5432/team_3')

orders_df.to_sql('orders', engine,if_exists="append",index=False)
basket_table_data.to_sql('basket', engine, if_exists="append",index=False)

#FIX INDEX