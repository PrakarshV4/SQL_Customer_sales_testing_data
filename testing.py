import mysql.connector
import re 
conn = mysql.connector.connect(
    host="localhost",           
    port=3306,
    user="root",
    password="root",
    database="mydb"
)
cursor = conn.cursor()


# Function to create a dict with table_name as key and its corrosponding columns as values 
def get_tables_and_columns(cursor):
    cursor.execute("SHOW TABLES;")
    tables = []
    for table in cursor.fetchall():
        tables.append(table[0])

    table_schemas = {}

    for table in tables:
        cursor.execute(f"DESCRIBE {table};")
        values = cursor.fetchall()
        columns = []
        for row in values:
            columns.append(row[0])
        
        table_schemas[table] = columns
        
    
    # for key,val in table_schemas.items():
    #     print(key,val)
    #     print("############")
    
    return table_schemas

# Classification of fact and dim tables
def classify_by_naming_convention(tables):
    fact_tables, dim_tables, other_tables = [], [], []

    for table in tables:
        if '_fact' in table.lower() or 'f_' in table.lower() or '_transaction' in table.lower():
            fact_tables.append(table)
            # print(f"Table => {table} inserted in Fact table")
        elif '_dim' in table.lower() or '_dimension' in table.lower() or 'd_' in table.lower():
            dim_tables.append(table)
            # print(f"Table => {table} inserted in Dimension table")

    table_types = {
        "Fact_Tables": fact_tables,
        "Dimension_Tables": dim_tables,
    }

    return table_types

def classify_by_column_names(table_schemas):
    keywords = {"amount", "quantity", "total", "timestamp", "date_id", "order", "sales" } 
    fact_tables, dim_tables = [], []
    for table, columns in table_schemas.items():
        cnt = 0
        for col in columns:
            col = col.lower()
            for keyword in keywords:
                if re.search(rf"\b{keyword}\b", col) or keyword in col:
                    cnt += 1

        # print(f"Table: {table}") 
        # print(f"Columns: {columns}") 
        # print(f"Keyword_matches: {cnt}")
        
        if cnt >= 2: 
            fact_tables.append(table)
            # print("It is Fact Table")
        else: 
            dim_tables.append(table)
            # print("It is Dimension Table")
    
    return {
        "Fact_Tables": fact_tables,
        "Dimension_Tables": dim_tables,
    } 

def classify_by_type_by_foreign_key_ratio(table_schema):
    fact_table, dim_table = [], []

    for table, columns_list in table_schemas.items():
        cnt_foreign_key = 0
        for col in columns_list:
            if '_id' in col:
                print(col)
                cnt_foreign_key += 1
        
        print(f"approx foreign keys in {table} : ", cnt_foreign_key)
        if cnt_foreign_key >= 3:
            fact_table.append(table)
            print('It is a Fact Table')
        else:
            dim_table.append(table)
            print('It is a Dimesion Table')
    
    return {
        "Fact Table": fact_table,
        "Dimension Table" : dim_table
    }


table_schemas = get_tables_and_columns(cursor)
# for key, val in table_schemas.items():
#     print(key, val)

# table_types = classify_by_naming_convention(list(table_schemas.keys()))
# for key, val in table_types.items():
#     print(key, val)

# print('--------------------------------------------------------')

# table_types_by_keywords = classify_by_column_names(table_schemas)
# for key, val in table_types_by_keywords.items():
#     print(key, val)

# print('--------------------------------------------------------')

table_type_by_foreign_key_ratio = classify_by_type_by_foreign_key_ratio(table_schemas)
for key, val in table_type_by_foreign_key_ratio.items():
    print(key, val)












# def show_cust_not_in_sales(query):
#     cursor.execute(query)
#     missing_cust_in_sales = cursor.fetchall() # => return a tuple

#     if missing_cust_in_sales:
#         print('Customer id that are not in sales: ')
#         for cust_id in missing_cust_in_sales:
#             print(cust_id[0])
#     else:
#         print('All customer have bought something')

# def show_duplicate_cust_id_in_customer(query):
#     cursor.execute(query)
#     duplicates = cursor.fetchall()

#     if duplicates:
#         print('There are duplicate customer id present in customer dimension table')
#         for row in duplicates:
#             print(f'Customer ID: {row[0]} appears {row[1]} times')
#     else:
#         print('All customer id are unique in customer table')

# def show_cust_in_sales_not_in_customer(query):
#     cursor.execute(query)
#     cust_in_sales_not_in_customer = cursor.fetchall()

#     if cust_in_sales_not_in_customer:
#         print("Customer ID that are prensent in Sales Table but not in Customer Table")
#         for cust_id in cust_in_sales_not_in_customer:
#             print(cust_id[0])
#     else:
#         print("All customer id in sales are present in customer table")


# cursor = conn.cursor()
# cursor.execute("SHOW TABLES;")
# print('Tables =>')
# for table in cursor.fetchall():
#     print(table[0])

# print('-----------------------------------------------------------------------------------')

# # Customer ids which are not in sales
# query = '''
#     Select distinct customer_id 
#     from customer
#     where customer_id NOT IN(
#         Select distinct customer_id from sales
#     ); 
# '''
# show_cust_not_in_sales(query)

# print('-----------------------------------------------------------------------------------')

# # Check if customer_id is unique in customer table
# query = '''
#     Select customer_id, Count(*) as count
#     from customer
#     group by customer_id
#     having Count(*) > 1;
# '''

# show_duplicate_cust_id_in_customer(query)

# print('-----------------------------------------------------------------------------------')

# # Customer id which are in sales but not in customer table
# query = '''
#     Select distinct customer_id 
#     from sales
#     where customer_id NOT IN (
#         Select customer_id 
#         from customer  
#     ); 
# '''
# show_cust_in_sales_not_in_customer(query)

# cursor.close()
# conn.close()