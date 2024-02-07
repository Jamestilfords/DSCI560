import yfinance as yf
import mysql.connector
from mysql.connector import Error

def create_database(cursor, database_name):
    try:
        cursor.execute(f"CREATE DATABASE {database_name}")
    except Error as err:
        print(f"Failed to create database: {err}")

def create_table(cursor, table_query):
    try:
        cursor.execute(table_query)
    except Error as err:
        print(f"Failed to create table: {err}")

def fetch_stock_data(stock_symbol):
    stock = yf.Ticker(stock_symbol)
    data = stock.history(period="1d")
    return data

def insert_stock_data(db_connection, stock_symbol, data):
    cursor = db_connection.cursor()
    for index, row in data.iterrows():
        date = index.strftime('%Y-%m-%d')
        open_price = row['Open']
        high = row['High']
        low = row['Low']
        close = row['Close']
        volume = row['Volume']

        query = ("INSERT INTO stock_data (symbol, date, open, high, low, close, volume) "
                 "VALUES (%s, %s, %s, %s, %s, %s, %s)")
        values = (stock_symbol, date, open_price, high, low, close, volume)

        cursor.execute(query, values)

    db_connection.commit()
    cursor.close()

try:
    # Connect to MySQL Server (without specifying a database)
    connection = mysql.connector.connect(host='localhost',
                                         user='root',
                                         password='james')

    cursor = connection.cursor()
    
    # Create Database
    create_database(cursor, "Stocks")
    
    # Connect to the newly created database
    connection.database = "Stocks"

    # Create Table
    table_query = """
        CREATE TABLE stock_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            symbol VARCHAR(10),
            date DATE,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT
        );
    """
    create_table(cursor, table_query)

    # Fetch and Insert Data
    stock_symbol = 'AAPL'  # Example stock symbol
    data = fetch_stock_data(stock_symbol)
    insert_stock_data(connection, stock_symbol, data)

except Error as e:
    print("Error while connecting to MySQL", e)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")
