# Author:   Austin Fouch
# Project:  CMPS364 Project 1
# Date:     04/05/2018
# Note: HTML templates using this file MUST have fields in lowercase ONLY -- NO camelCase.

import psycopg2, psycopg2.extras
from urllib.parse import urlparse, uses_netloc
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
connection_string = config['database']['postgres_connection']

def initialize():
    createCustomers = "CREATE TABLE IF NOT EXISTS Customers(id SERIAL PRIMARY KEY, firstName VARCHAR(100), lastName VARCHAR(100), street VARCHAR(100), city VARCHAR(100), state VARCHAR(2), zip VARCHAR(5));"
    createProducts = "CREATE TABLE IF NOT EXISTS Products(id SERIAL PRIMARY KEY, name VARCHAR(100), price REAL);"
    createOrders = "CREATE TABLE IF NOT EXISTS Orders(id SERIAL PRIMARY KEY, customerId INT, productId INT, FOREIGN KEY(customerId) REFERENCES Customers(id) ON DELETE CASCADE, FOREIGN KEY(productId) REFERENCES Products(id) ON DELETE CASCADE, date DATE);"
    with conn.cursor() as cursor:
        cursor.execute(createCustomers)
        cursor.execute(createProducts)
        cursor.execute(createOrders)
    conn.commit()

def get_customers():
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    with dict_cur as cursor:
        cursor.execute("SELECT * FROM Customers;")
        for customer in cursor:
            yield customer
    conn.commit()

def get_customer(id):
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    with dict_cur as cursor:
        cursor.execute("""SELECT * FROM Customers WHERE Customers.id=%s;""", [id])
        customer = cursor.fetchone()
    conn.commit()
    return customer

def upsert_customer(customer):
    customerData = list(customer.values())
    with conn.cursor() as cursor:
        if 'id' in customer.keys():
            # place id at the end of list so it is paramatized last
            customerData.append(customerData[0])
            customerData.pop(0)
            cursor.execute("""UPDATE Customers SET firstName=%s, lastName=%s, street=%s, city=%s, state=%s, zip=%s WHERE Customers.id=%s;""", customerData)
        else:
            cursor.execute("""INSERT INTO Customers (firstName, lastName, street, city, state, zip) VALUES(%s, %s, %s, %s, %s, %s);""", customerData)
    conn.commit()

def delete_customer(id):
    with conn.cursor() as cursor:
        cursor.execute("""DELETE FROM Customers WHERE Customers.id=%s;""", [id])
    conn.commit()
    
def get_products():
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    with dict_cur as cursor:
        cursor.execute("SELECT * FROM Products;")
        for product in cursor:
            yield product
    conn.commit()

def get_product(id):
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    with dict_cur as cursor:
        cursor.execute("""SELECT * FROM Products WHERE id=%s;""", [id])
        product = cursor.fetchone()
    conn.commit()
    return product

def upsert_product(product):
    productData = list(product.values())
    with conn.cursor() as cursor:
        if 'id' in product.keys():
            # place id at the end of list so it is paramatized last
            productData.append(productData[0])
            productData.pop(0)
            cursor.execute("""UPDATE Products SET name=%s, price=%s WHERE Products.id=%s;""", productData)
        else:
            cursor.execute("""INSERT INTO Products (name, price) VALUES(%s, %s);""", productData)
    conn.commit()

def delete_product(id):
    with conn.cursor() as cursor:
        cursor.execute("""DELETE FROM Products WHERE Products.id=%s;""", [id])
    conn.commit()

def get_orders():
    getOrders = "SELECT * FROM Orders;"
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    with dict_cur as cursor:
        cursor.execute(getOrders)
        for order in cursor:
            order['customer'] = get_customer(order['customerid'])
            order['product'] = get_product(order['productid'])
            yield order
    conn.commit()

def get_order(id):
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    with dict_cur as cursor:
        cursor.execute("""SELECT * FROM Orders WHERE Order.id=%s;""", [id])
        order = cursor.fetchone()
    conn.commit()
    return order

def upsert_order(order):
    orderData = list(order.values())
    with conn.cursor() as cursor:
        if 'id' in order.keys():
            # place id at the end of list so it is paramatized last
            orderData.append(orderData[0])
            orderData.pop(0)
            cursor.execute("""UPDATE Orders SET customerId=%s, productId=%s, date=%s WHERE Orders.id=%s;""", orderData)
        else:
            cursor.execute("""INSERT INTO Orders (customerId, productId, date) VALUES(%s, %s, %s);""", (orderData[0], orderData[1], orderData[5]))
    conn.commit()

def delete_order(id):
    deleteOrder = "DELETE FROM Orders WHERE Orders.id=%s;""", [id]
    with conn.cursor() as cursor:
        cursor.execute(deleteOrder)
    conn.commit()

def customer_report(id):
    dict_cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    with dict_cur as cursor:
        customer = get_customer(id)
        customer['orders'] = []
        cursor.execute("""SELECT * FROM Orders WHERE customerId=%s;""", [id])
        for order in cursor:
            order['product'] = get_product(order['productid'])
            customer['orders'].append(order)
    conn.commit()
    return customer

def sales_report():
    products = get_products()
    for product in products:
        orders = [o for o in get_orders() if o['productid'] == product['id']] 
        orders = sorted(orders, key=lambda k: k['date']) 
        product['last_order_date'] = orders[-1]['date']
        product['total_sales'] = len(orders)
        product['gross_revenue'] = product['price'] * product['total_sales']
        yield product

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