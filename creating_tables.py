import mysql.connector


conn = mysql.connector.connect(
    host="localhost",           
    port=3306,
    user="root",
    password="root",
    database="mydb"
)

cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS sales")
cursor.execute("DROP TABLE IF EXISTS customer")

# customer dim table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS customer (
        customer_id INT,
        customer_name VARCHAR(100),
        email VARCHAR(100)
    )
""")
# sales fact table
cursor.execute("""
    CREATE TABLE sales (
        sale_id INT AUTO_INCREMENT PRIMARY KEY,
        customer_id INT,
        product_id INT,
        amount DECIMAL(10,2),
        sale_date DATE
    )
""")

cursor.executemany("""
    INSERT INTO customer (customer_id, customer_name, email)
    VALUES (%s, %s, %s)
    """, [
        (1, 'Alice', 'alice@example.com'),
        (2, 'Bob', 'bob@example.com'),
        (2, 'Bob', 'bob@example.com'),  #  duplicate
        (3, 'Charlie', 'charlie@example.com'),
        (4, 'Luffy', 'luffy@example.com'), #duplicate
        (4, 'Luffy', 'luffy@example.com'),
        (4, 'Luffy', 'luffy@example.com'),              
        (5, 'Eve', 'eve@example.com'),  # Will not appear in sales
])


cursor.executemany("""
INSERT INTO sales (customer_id, product_id, amount, sale_date)
VALUES (%s, %s, %s, %s)
""", [
    (1, 101, 50.00, '2024-01-10'),
    (2, 102, 75.50, '2024-01-12'),
    (3, 103, 60.00, '2024-01-15'),
    (7, 107, 65.00, '2024-01-18')
])

conn.commit()
print("Tables created and data inserted.")

cursor.close()
conn.close()
