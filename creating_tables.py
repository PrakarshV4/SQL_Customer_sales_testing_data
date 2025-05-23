import mysql.connector
import pandas as pd

conn = mysql.connector.connect(
    host="localhost",           
    port=3306,
    user="root",
    password="root",
    database="mydb"
)

cursor = conn.cursor()

# Drop existing tables (for clean run)
tables = ["sales_fact", "orders_fact", "customer_dim", "product_dim", "date_dim", "region_dim"]
for table in tables:
    cursor.execute(f"DROP TABLE IF EXISTS {table}")
    print(f"{table} dropped")

############### Fact Table #################

# sales_fact
# sales_id = 4 uses customer_id = 3, which exists in customer_dim
# sales_id = 5 uses customer_id = 9, which does not exist in customer_dim
cursor.execute("""
CREATE TABLE sales_fact (
    sales_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    date_id INT,
    region_id INT,
    quantity_sold INT,
    total_amount DECIMAL(10, 2),
    created_at TIMESTAMP
)
""")

# orders_fact
# order_id = 5 uses customer_id = 10, which does not exist
# order_id = 4 uses customer_id = 4, which exists
cursor.execute("""
CREATE TABLE orders_fact (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    date_id INT,
    region_id INT,
    order_amount DECIMAL(10, 2),
    discount_applied DECIMAL(10, 2),
    order_timestamp TIMESTAMP
)
""")

############### Dimension Table #################
# customer_dim
# customer_id = 5 is a duplicate of 1
# customer_id = 6 is not used in sales or orders
cursor.execute("""
CREATE TABLE customer_dim (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(20),
    gender VARCHAR(10),
    birthdate DATE
)
""")

# product_dim
cursor.execute("""
CREATE TABLE product_dim (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10, 2)
)
""")

# date_dim
cursor.execute("""
CREATE TABLE date_dim (
    date_id INT PRIMARY KEY,
    full_date DATE,
    day_of_week VARCHAR(20),
    month VARCHAR(20),
    year INT
)
""")

# region_dim
cursor.execute("""
CREATE TABLE region_dim (
    region_id INT PRIMARY KEY,
    region_name VARCHAR(100),
    country VARCHAR(100)
)
""")

csv_table_map = {
    "customer_dim": "customer_dim.csv",
    "product_dim": "product_dim.csv",
    "date_dim": "date_dim.csv",
    "region_dim": "region_dim.csv",
    "sales_fact": "sales_fact.csv",
    "orders_fact": "orders_fact.csv"
}

################# Inserting Data Into Tables #######################

def insert_data_from_csv_to_mysql(table_name, csv_file, cursor):
    df = pd.read_csv(f"Data/{csv_file}")
    columns = ",".join(df.columns)              # columns = "customer_id,customer_name,email,phone,gender,birthdate"
    placeholders = ",".join(["%s"] * len(df.columns)) # placeholders = "%s,%s,%s,%s,%s,%s"
    values = []
    for row in df.to_numpy():
        values.append(tuple(row)) 

    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    cursor.executemany(insert_query, values)

    print(f"Inserted {len(values)} rows into {table_name} from {csv_file}")


for table_name, csv_file in csv_table_map.items():
    insert_data_from_csv_to_mysql(table_name, csv_file, cursor)


conn.commit()
conn.close()
print("Tables created successfully.")