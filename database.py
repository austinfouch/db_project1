import psycopg2, psycopg2.extras
from urllib.parse import urlparse, uses_netloc
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
connection_string = config['database']['postgres_connection']

# The following functions are REQUIRED - you should REPLACE their implementation
# with the appropriate code to interact with your PostgreSQL database.
def initialize():
    createCustomers = "CREATE TABLE IF NOT EXISTS Customers(id SERIAL PRIMARY KEY, firstName VARCHAR(100), lastName VARCHAR(100), street VARCHAR(100), city VARCHAR(100), state VARCHAR(2), zip INT);"
    createProducts = "CREATE TABLE IF NOT EXISTS Products(id SERIAL PRIMARY KEY, name VARCHAR(100), price REAL);"
    createOrders = "CREATE TABLE IF NOT EXISTS Orders(id SERIAL PRIMARY KEY, customerId INT, productId INT, FOREIGN KEY(customerId) REFERENCES Customers(id) ON DELETE CASCADE, FOREIGN KEY(productId) REFERENCES Products(id) ON DELETE CASCADE, date DATE);"
    with conn.cursor() as cursor:
        cursor.execute(createCustomers)
        cursor.execute(createProducts)
        cursor.execute(createOrders)
    conn.commit()

def get_customers():
    getCustomers = "SELECT Customers.id, Customers.firstName, Customers.lastName, Customers.street, Customers.city, Customers.state, Customers.zip FROM Customers;"
    with conn.cursor() as cursor:
        cursor.execute(getCustomers)
        customers = cursor.fetchall()
        for a_customer in customers:
            customer = {}
            customer["id"] = a_customer[0]
            customer["firstName"] = a_customer[1]
            customer["lastName"] = a_customer[2]
            customer["street"] = a_customer[3]
            customer["city"] = a_customer[4]
            customer["state"] = a_customer[5]
            customer["zip"]= a_customer[6]
            yield customer
    conn.commit()

def get_customer(id):
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    with dict_cur as cursor:
        cursor.execute("""SELECT Customers.id, Customers.firstName, Customers.lastName, Customers.street, Customers.city, Customers.state, Customers.zip FROM Customers WHERE Customers.id=%s;""", [id])
        customer = cursor.fetchone()
    conn.commit()
    return customer

## TODO ##
def upsert_customer(customer):
    if 'id' in customer.keys():
        print("edit customer...")
    else:
        customerData = list(customer.values())
        with conn.cursor() as cursor:
            cursor.execute("""INSERT INTO Customers (firstName, lastName, street, city, state, zip) VALUES(%s, %s, %s, %s, %s, %s);""", customerData)
        conn.commit()

def delete_customer(id):
    with conn.cursor() as cursor:
        cursor.execute("""DELETE FROM Customers WHERE Customers.id=%s;""", [id])
    conn.commit()
    
def get_products():
    getProducts = "SELECT Products.id, Products.name, Products.price FROM Products;"
    with conn.cursor() as cursor:
        cursor.execute(getProducts)
        products = cursor.fetchall()
        for a_product in products:
            product = {}
            product["id"] = a_product[0]
            product["name"] = a_product[1]
            product["price"] = a_product[2]
            yield product
    conn.commit()

def get_product(id):
    getProduct = "SELECT Products.id, Products.name, Products.price FROM Products WHERE id='{0}'".format(id)
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    with dict_cur as cursor:
        cursor.execute(getProduct)
        product = cursor.fetchone()
    conn.commit()
    return product

## TODO ##
def upsert_product(product):
    productData = list(product.values())
    with conn.cursor() as cursor:
        if 'id' in product.keys():
            cursor.execute("""UPDATE Products SET Products.name=%s, Products.price=%s WHERE Products.id=%s""", productData)
        else:
            cursor.execute("""INSERT INTO Products (name, price) VALUES(%s, %s);""", productData)
    conn.commit()

def delete_product(id):
    deleteProduct = "DELETE FROM Products WHERE Products.id='{0}'".format(id)
    with conn.cursor() as cursor:
        cursor.execute(deleteProduct)
    conn.commit()

## TODO ##
def get_orders():
    getOrders = "SELECT Orders.id, Orders.customerId, Orders.productId, Orders.date FROM Orders LEFT JOIN Customers ON Orders.customerId = Customers.id LEFT JOIN Products ON Orders.productId = Products.id;"
    with conn.cursor() as cursor:
        cursor.execute(getOrders)
        orders = cursor.fetchall()
        for a_order in orders:
            print(a_order)
            order = {}
            customer = get_customer(a_order[1])
            product = get_product(a_order[2])
            print(customer)
            print(product)
            order["id"] = a_order[0]
            order["customer"] = get_customer(a_order[1])
            order["product"] = get_product(a_order[2])
            order["date"] = a_order[3]
            yield a_order
    conn.commit()

## TODO ##
#def get_order(id):
#    return _find_by_id(orders, id)

## TODO ##
def upsert_order(order):
    orderData = list(order.values())
    print(orderData)
    with conn.cursor() as cursor:
        if 'id' in order.keys():
            print("edit product...")
        else:
            cursor.execute("""INSERT INTO Orders (customerId, productId, date) VALUES(%s, %s, %s);""", (orderData[1], orderData[2], orderData[5]))
    conn.commit()

## TODO ##
#def delete_order(id):
#    _delete_by_id(orders, id)

## TODO ##
# Return the customer, with a list of orders.  Each order should have a product 
# property as well.
def customer_report(id):
    customer = {} #_find_by_id(customers, id)
    orders = get_orders()
    customer['orders'] = [o for o in orders if o['customerId'] == id]
    return customer

## TODO ##
def sales_report():
    products = get_products()
    for product in products:
        orders = [o for o in get_orders() if o['productId'] == product['id']] 
        orders = sorted(orders, key=lambda k: k['date']) 
        product['last_order_date'] = orders[-1]['date']
        product['total_sales'] = len(orders)
        product['gross_revenue'] = product['price'] * product['total_sales']
    return products

# This is called at the bottom of this file.  You can re-use this important function in any python program
# that uses psychopg2.  The connection string parameter comes from the config.ini file in this
# particular example.  You do not need to edit this code.
def connect_to_db(conn_str):
    uses_netloc.append("postgres")
    url = urlparse(conn_str)

    conn = psycopg2.connect(database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port)

    return conn

conn = connect_to_db(connection_string)