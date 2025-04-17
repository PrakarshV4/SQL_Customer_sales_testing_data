import mysql.connector

conn = mysql.connector.connect(
    host="localhost",           
    port=3306,
    user="root",
    password="root",
    database="mydb"
)
def show_cust_not_in_sales(query):
    cursor.execute(query)
    missing_cust_in_sales = cursor.fetchall() # => return a tuple

    if missing_cust_in_sales:
        print('Customer id that are not in sales: ')
        for cust_id in missing_cust_in_sales:
            print(cust_id[0])
    else:
        print('All customer have bought something')

def show_duplicate_cust_id_in_customer(query):
    cursor.execute(query)
    duplicates = cursor.fetchall()

    if duplicates:
        print('There are duplicate customer id present in customer dimension table')
        for row in duplicates:
            print(f'Customer ID: {row[0]} appears {row[1]} times')
    else:
        print('All customer id are unique in customer table')

def show_cust_in_sales_not_in_customer(query):
    cursor.execute(query)
    cust_in_sales_not_in_customer = cursor.fetchall()

    if cust_in_sales_not_in_customer:
        print("Customer ID that are prensent in Sales Table but not in Customer Table")
        for cust_id in cust_in_sales_not_in_customer:
            print(cust_id[0])
    else:
        print("All customer id in sales are present in customer table")


cursor = conn.cursor()
cursor.execute("SHOW TABLES;")
print('Tables =>')
for table in cursor.fetchall():
    print(table[0])

print('-----------------------------------------------------------------------------------')

# Customer ids which are not in sales
query = '''
    Select distinct customer_id 
    from customer
    where customer_id NOT IN(
        Select distinct customer_id from sales
    ); 
'''
show_cust_not_in_sales(query)

print('-----------------------------------------------------------------------------------')

# Check if customer_id is unique in customer table
query = '''
    Select customer_id, Count(*) as count
    from customer
    group by customer_id
    having Count(*) > 1;
'''

show_duplicate_cust_id_in_customer(query)

print('-----------------------------------------------------------------------------------')

# Customer id which are in sales but not in customer table
query = '''
    Select distinct customer_id 
    from sales
    where customer_id NOT IN (
        Select customer_id 
        from customer  
    ); 
'''
show_cust_in_sales_not_in_customer(query)

cursor.close()
conn.close()