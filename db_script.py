import psycopg2

db_1_string = """
CREATE TABLE orders (
datetime timestamp NOT NULL,
order_id INT PRIMARY KEY,
location VARCHAR(200) NOT NULL,
payment_method VARCHAR(15) NOT NULL,
amount_paid FLOAT NOT NULL,
);
"""

db_2_string ="CREATE TABLE basket( order_id INT NOT NULL, product VARCHAR(500), quantity INT NOT NULL, FOREIGN KEY (order_id) REFERENCES orders(order_id));"

connection = psycopg2.connect('host=localhost dbname=team_3 user=root password=password')
#postgresql+psycopg2://root:password@localhost:8080/team_3

cursor = connection.cursor()
cursor.execute(db_1_string)
connection.commit()
cursor.execute(db_2_string)
connection.commit()
cursor.close()
connection.close()