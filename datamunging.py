#sanika k
import sqlite3
import pandas as pd

#start a connection
con = sqlite3.connect('shipment_database.db')

#read databases
base0 = pd.read_csv('data/shipping_data_0.csv')
base0.to_sql(name='base0',con=con,if_exists='replace',index=True)

base1 = pd.read_csv('data/shipping_data_1.csv')
base1.to_sql(name='base1',con=con,if_exists='replace',index=True)

base2 = pd.read_csv('data/shipping_data_2.csv')
base2.to_sql(name='base2',con=con,if_exists='replace',index=True)

#cursor
cursor = con.cursor()

zero_one = cursor.execute("""SELECT base0.origin_warehouse, base0.destination_store, base0.product, base0.on_time, base0.product_quantity, base0.driver_identifier
FROM base0
LEFT JOIN base1 ON base0.product=base1.product
union
SELECT base0.origin_warehouse, base0.destination_store, base0.product, base0.on_time, base0.product_quantity, base0.driver_identifier
FROM base0
LEFT JOIN base2 ON base0.origin_warehouse=base2.origin_warehouse;""")
#full outer joins not supported in sqlite3
    
zero_two = cursor.execute("""SELECT base0.origin_warehouse, base0.destination_store, base0.product, base0.on_time, base0.product_quantity, base0.driver_identifier
FROM base0
LEFT JOIN base2 ON base0.origin_warehouse=base2.origin_warehouse
union
SELECT base0.origin_warehouse, base0.destination_store, base0.product, base0.on_time, base0.product_quantity, base0.driver_identifier
FROM base2
LEFT JOIN base0 ON base0.origin_warehouse=base2.origin_warehouse;""")

for i in zero_two.fetchall():
    print(i)

con.commit()
con.close()