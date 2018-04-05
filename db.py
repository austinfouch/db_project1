import psycopg2
import csv
from urllib.parse import urlparse, uses_netloc
import configparser

#######################################################################
# IMPORTANT:  You must set your config.ini values!
#######################################################################
# The connection string is provided by elephantsql.  Log into your account and copy it into the 
# config.ini file.  It should look something like this:
# postgres://vhepsma:Kdcads89DSFlkj23&*dsdc-32njkDSFS@foo.db.elephantsql.com:7812/vhepsma
# Make sure you copy the entire thing, exactly as displayed in your account page!
#######################################################################
config = configparser.ConfigParser()
config.read('config.ini')
connection_string = config['database']['postgres_connection']

#  You may use this in seed_database.  The function reads the CSV files
#  and returns a list of lists.  The first list is a list of classes, 
#  the second list is a list of ships.
def load_seed_data():
    classes = list()
    with open('classes.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            classes.append(row)

    ships = list()
    with open('ships.csv', newline='') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            ships.append(row)
    return [classes, ships]


def seed_database():
#    deleteClass = "DROP TABLE CLASSES CASCADE;"
#    deleteShips = "DROP TABLE SHIPS;"
    createClasses = "CREATE TABLE IF NOT EXISTS Classes(Class VARCHAR(100), Type VARCHAR(2), Country VARCHAR(100), NumGuns INT, Bore REAL, Displacement REAL, PRIMARY KEY(Class));"
    createShips = "CREATE TABLE IF NOT EXISTS Ships(Name VARCHAR(100), Class VARCHAR(100), Launched INT, FOREIGN KEY(Class) REFERENCES Classes(Class) ON DELETE CASCADE);"
    with conn.cursor() as cursor:
        cursor.execute(createClasses)
        cursor.execute(createShips)
        cursor.execute("SELECT * from CLASSES;")
        if(cursor.rowcount == 0):
            [classes, ships] = load_seed_data()
            for a_class in classes:
                add_class(a_class)
            for a_ship in ships:
                temp = a_ship[0]
                a_ship[0] = a_ship[1]
                a_ship[1] = temp
                add_ship(a_ship)
#        cursor.execute(deleteClass)
#        cursor.execute(deleteShips)
    conn.commit()
    # you must create the necessary tables, if they do not already exist.
    # BE SURE to setup the necessary foreign key constraints such that deleting
    # a class results in all ships of that class being removed automatically.
    
    # after ensuring the tables are present, count how many classes there are. 
    # if there are none, then call load_data to get the data found in config.json.
    # Insert the data returned from load_data.  Hint - it returns a tuple, with the first being a list
    # of tuples representing classes, and the second being the list of ships.  Carefully
    # examine the code or print the returned data to understand exactly how the data is structured
    # i.e. column orders, etc.

    # If there is already data, there is no need to do anything at all...

# Return list of all classes.  Important, to receive full credit you
# should use a Python generator to yield each item out of the cursor.
# Column order should be class, type, country, guns, bore, displacement
def get_classes():
    getClasses = "SELECT Classes.Class, Classes.Type, Classes.Country, Classes.NumGuns, Classes.Bore, Classes.Displacement FROM Classes;"
    with conn.cursor() as cursor:
        cursor.execute(getClasses)
        classes = cursor.fetchall()
        for a_class in classes:
            yield a_class
    conn.commit()
    
   
# Return list of all ships, joined with class.  Important, to receive full credit you
# should use a Python generator to yield each item out of the cursor.
# Column order should be ship.class, name, launched, class.class, type, country, guns, bore, displacement
# If class_name is not None, only return ships with the given class_name.  Otherwise, return all ships
def get_ships(class_name):
    if class_name == None:
        with conn.cursor() as cursor:
            cursor.execute("SELECT Ships.Class, Ships.Name, Ships.Launched, Classes.Class, Classes.Type, Classes.Country, Classes.NumGuns, Classes.Bore, Classes.Displacement FROM Ships LEFT JOIN Classes ON Ships.Class=Classes.Class;")
            ships = cursor.fetchall()
            for ship in ships:
                yield ship
    else:
        with conn.cursor() as cursor:
            cursor.execute("SELECT Ships.Class, Ships.Name, Ships.Launched, Classes.Class, Classes.Type, Classes.Country, Classes.NumGuns, Classes.Bore, Classes.Displacement FROM Ships LEFT JOIN Classes ON Ships.Class=Classes.Class WHERE Ships.Class='{0}';".format(class_name))
            ships = cursor.fetchall()
            for ship in ships:
                yield ship    
    conn.commit()
    

# Data will be a list ordered with class, type, country, guns, bore, displacement.
def add_class(data):
    with conn.cursor() as cursor:
        cursor.execute("""INSERT INTO Classes values(%s, %s, %s, %s, %s, %s);""", data)
    conn.commit()
    

# Data will be a list ordered with class, name, launched.
def add_ship(data):
    with conn.cursor() as cursor:
        cursor.execute("""INSERT INTO Ships(Class, Name, Launched) values(%s, %s, %s);""", data)
    conn.commit()
    

# Delete class with given class name.  Note while there should only be one
# SQL execution, all ships associated with the class should also be deleted.
def delete_class(class_name):
    deleteClass = "DELETE FROM Classes WHERE Class='{0}'".format(class_name)
    with conn.cursor() as cursor:
        cursor.execute(deleteClass)
    conn.commit()

# Deletes the ship with the given class and ship name.
def delete_ship(ship_name, class_name):
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM Ships WHERE Ships.Name='{0}' AND Ships.Class='{1}'".format(ship_name, class_name))
    conn.commit()



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

# This establishes the connection, conn will be used across the lifetime of the program.
conn = connect_to_db(connection_string)
seed_database()