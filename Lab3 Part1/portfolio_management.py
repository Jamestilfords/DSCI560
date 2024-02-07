import mysql.connector
import yfinance as yf
import datetime
import pandas as pd

def create_connection():
    return mysql.connector.connect(host='localhost', user='root', password='james', database='Stocks')

def create_table(cursor, create_table_sql):
    try:
        cursor.execute(create_table_sql)
    except mysql.connector.Error as err:
        print(f"Failed creating table: {err}")
        exit(1)

def setup_database():
    connection = create_connection()
    cursor = connection.cursor()
    # SQL for creating portfolios table
    sql_create_portfolios_table = """
    CREATE TABLE IF NOT EXISTS portfolios (
        portfolio_name VARCHAR(255) PRIMARY KEY,
        creation_date DATE NOT NULL DEFAULT (CURDATE())
    );
    """
    # SQL for creating portfolio_stocks table
    sql_create_portfolio_stocks_table = """
    CREATE TABLE IF NOT EXISTS portfolio_stocks (
        id INT AUTO_INCREMENT PRIMARY KEY,
        portfolio_name VARCHAR(255),
        stock_symbol VARCHAR(10),
        FOREIGN KEY (portfolio_name) REFERENCES portfolios(portfolio_name)
    );
    """
    create_table(cursor, sql_create_portfolios_table)
    create_table(cursor, sql_create_portfolio_stocks_table)
    cursor.close()
    connection.close()

def stock_exists(symbol):
    try:
        stock_info = yf.Ticker(symbol).info
        # Check if 'currentPrice' is in the info dictionary, the stock symbol is valid
        valid = 'currentPrice' in stock_info and stock_info['currentPrice'] is not None
        return valid
    except Exception as e:
        print(f"An error occurred while checking the stock symbol: {e}")
        return False

def add_portfolio(portfolio_name):
    connection = create_connection()
    cursor = connection.cursor()
    # Check if the portfolio already exists
    cursor.execute("SELECT 1 FROM portfolios WHERE portfolio_name = %s", (portfolio_name,))
    if cursor.fetchone() is not None:
        print(f"Portfolio '{portfolio_name}' already exists.")
    else:
        query = "INSERT INTO portfolios (portfolio_name) VALUES (%s)"
        cursor.execute(query, (portfolio_name,))
        connection.commit()
        print(f"Portfolio '{portfolio_name}' added successfully.")
    cursor.close()
    connection.close()

def delete_portfolio(portfolio_name):
    connection = create_connection()
    cursor = connection.cursor()

    # First, delete associated rows in portfolio_stocks table
    query = "DELETE FROM portfolio_stocks WHERE portfolio_name=%s"
    cursor.execute(query, (portfolio_name,))

    # Then, delete the portfolio from portfolios table
    query = "DELETE FROM portfolios WHERE portfolio_name=%s"
    cursor.execute(query, (portfolio_name,))

    connection.commit()
    print(f"Portfolio '{portfolio_name}' deleted.")
    cursor.close()
    connection.close()

def add_stock_to_portfolio(portfolio_name, stock_symbol):
    connection = create_connection()
    cursor = connection.cursor()

    # Check if stock already exists in portfolio
    cursor.execute("SELECT 1 FROM portfolio_stocks WHERE portfolio_name = %s AND stock_symbol = %s", (portfolio_name, stock_symbol))
    if cursor.fetchone():
        print(f"Stock {stock_symbol} already exists in portfolio {portfolio_name}.")
    else:
        if stock_exists(stock_symbol):
            query = "INSERT INTO portfolio_stocks (portfolio_name, stock_symbol) VALUES (%s, %s)"
            cursor.execute(query, (portfolio_name, stock_symbol))
            connection.commit()
            print(f"Stock {stock_symbol} added successfully to portfolio {portfolio_name}.")
        else:
            print(f"Invalid stock symbol: {stock_symbol}")

    cursor.close()
    connection.close()


    cursor.close()
    connection.close()

def remove_stock_from_portfolio(portfolio_name, stock_symbol):
    connection = create_connection()
    cursor = connection.cursor()
    query = "DELETE FROM portfolio_stocks WHERE portfolio_name=%s AND stock_symbol=%s"
    cursor.execute(query, (portfolio_name, stock_symbol))
    connection.commit()
    print(f"Stock {stock_symbol} removed from portfolio {portfolio_name}.")
    cursor.close()
    connection.close()

def view_portfolio(portfolio_name):
    connection = create_connection()
    cursor = connection.cursor()

    # Fetch and display portfolio creation date
    cursor.execute("SELECT creation_date FROM portfolios WHERE portfolio_name = %s", (portfolio_name,))
    creation_date = cursor.fetchone()
    if creation_date:
        print(f"\nPortfolio '{portfolio_name}' created on {creation_date[0]}")
    else:
        print(f"\nPortfolio '{portfolio_name}' does not exist.")
        return

    # Fetch and display detailed stock data for each stock in the portfolio
    query = """
    SELECT s.symbol, s.date, s.open, s.high, s.low, s.close, s.volume
    FROM stock_data s
    INNER JOIN portfolio_stocks ps ON s.symbol = ps.stock_symbol
    WHERE ps.portfolio_name = %s
    ORDER BY s.symbol, s.date
    """
    cursor.execute(query, (portfolio_name,))
    rows = cursor.fetchall()
    if rows:
        print("\nDetailed Stock Data in the Portfolio:")
        for row in rows:
            print(f"Symbol: {row[0]}, Date: {row[1]}, Open: {row[2]}, High: {row[3]}, Low: {row[4]}, Close: {row[5]}, Volume: {row[6]}")
    else:
        print("No detailed stock data found in this portfolio.")

    cursor.close()
    connection.close()

def fetch_and_store_stock_data(portfolio_name, symbol, start_date, end_date):
    # Fetch data from yfinance
    data = yf.download(symbol, start=start_date, end=end_date)

    if data.empty:
        print(f"No data fetched for {symbol}.")
        return

    # Store data in the database
    connection = create_connection()
    cursor = connection.cursor()

    for index, row in data.iterrows():
        date = index.strftime('%Y-%m-%d')
        values = (symbol, date, row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])
        query = ("INSERT INTO stock_data (symbol, date, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)")
        cursor.execute(query, values)

        # Link stock data to the portfolio
        query = "INSERT INTO portfolio_stocks (portfolio_name, stock_symbol) VALUES (%s, %s)"
        cursor.execute(query, (portfolio_name, symbol))

    connection.commit()
    cursor.close()
    connection.close()
    print(f"Data for {symbol} added to portfolio '{portfolio_name}'.")


def fetch_stock_data_for_date_range(symbol, start_date, end_date):
    try:
        data = yf.download(symbol, start=start_date, end=end_date)
        return data if not data.empty else pd.DataFrame()  # Return an empty DataFrame if no data
    except Exception as e:
        print(f"An error occurred while fetching the stock data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

def fetch_stock_data_from_db(symbol):
    connection = create_connection()
    cursor = connection.cursor()

    query = """
    SELECT date, open, high, low, close, volume 
    FROM stock_data 
    WHERE symbol = %s
    ORDER BY date
    """

    cursor.execute(query, (symbol,))
    rows = cursor.fetchall()
    cursor.close()
    connection.close()

    if rows:
        df = pd.DataFrame(rows, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        df.set_index('Date', inplace=True)
        return df
    else:
        return pd.DataFrame()  # Return an empty DataFrame if no data is found

def list_all_portfolios():
    connection = create_connection()
    cursor = connection.cursor()
    query = "SELECT portfolio_name FROM portfolios"
    cursor.execute(query)
    portfolios = cursor.fetchall()
    print("\nAvailable portfolios:")
    for portfolio_name in portfolios:
        print(f"Name: {portfolio_name}")
    cursor.close()
    connection.close()

def list_stocks_in_portfolio(portfolio_name, return_list=False):
    connection = create_connection()
    cursor = connection.cursor()
    query = "SELECT DISTINCT stock_symbol FROM portfolio_stocks WHERE portfolio_name=%s"
    cursor.execute(query, (portfolio_name,))
    stocks = cursor.fetchall()
    if not stocks:
        print(f"No stocks found in portfolio '{portfolio_name}'.")
        return [] if return_list else None
    else:
        if return_list:
            return [stock[0] for stock in stocks]
        else:
            print(f"\nList of stocks in portfolio '{portfolio_name}':")
            for stock in stocks:
                print(stock[0])
    cursor.close()
    connection.close()

def list_distinct_stocks_in_portfolio(portfolio_name):
    connection = create_connection()
    cursor = connection.cursor()
    query = "SELECT DISTINCT stock_symbol FROM portfolio_stocks WHERE portfolio_name=%s"
    cursor.execute(query, (portfolio_name,))
    stocks = cursor.fetchall()
    cursor.close()
    connection.close()

    if not stocks:
        print(f"No distinct stocks found in portfolio '{portfolio_name}'.")
    else:
        print(f"\nDistinct stocks in portfolio '{portfolio_name}':")
        for stock in stocks:
            print(stock[0])


#joins the portfolio_stocks table with itself and deletes entries where there are duplicates 
#(based on portfolio_name and stock_symbol) while keeping the first occurrence
def cleanup_portfolio_duplicates():
    connection = create_connection()
    cursor = connection.cursor()

    # SQL to find and delete duplicate stocks in each portfolio
    sql_cleanup_duplicates = """
    DELETE p1 FROM portfolio_stocks p1
    INNER JOIN portfolio_stocks p2 
    WHERE 
        p1.id > p2.id AND 
        p1.portfolio_name = p2.portfolio_name AND 
        p1.stock_symbol = p2.stock_symbol;
    """

    try:
        cursor.execute(sql_cleanup_duplicates)
        connection.commit()
        print("Cleaned up duplicate stocks in portfolios.")
    except mysql.connector.Error as err:
        print(f"Error cleaning up duplicates: {err}")
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    setup_database()
    cleanup_portfolio_duplicates()
