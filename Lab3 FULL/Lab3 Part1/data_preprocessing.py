import yfinance as yf
import pandas as pd
import portfolio_management as pm

def preprocess_stock_data(df):
    df.reset_index(inplace=True)  # Reset index to convert the DateTimeIndex to a column
    df.rename(columns={'Date': 'timestamp'}, inplace=True)  # Rename 'Date' column to 'timestamp'
    df['daily_return'] = (df['Close'] / df['Open']) - 1

    # Apply rolling calculations
    df['MA_20'] = df['Close'].rolling(window=20).mean() 
    df['volatility'] = df['daily_return'].rolling(window=20).std()  
    df['upper_band'] = df['MA_20'] + (df['volatility'] * 2)  
    df['lower_band'] = df['MA_20'] - (df['volatility'] * 2)  
    # Handling NaN values
    df['MA_20'].fillna(method='bfill', inplace=True)  
    df['volatility'].fillna(method='bfill', inplace=True)  
    df['upper_band'].fillna(df['MA_20'], inplace=True) 
    df['lower_band'].fillna(df['MA_20'], inplace=True)  

    return df


def display_stock_metrics(df):
    while True:
        print("\nStock Metrics Display")
        print("1. Show Daily Returns")
        print("2. Show Moving Averages")
        print("3. Show Volatility")
        print("4. Show Bollinger Bands")
        print("5. Exit")

        choice = input("Enter your choice: ")

        if choice == "1":
            print(df[['timestamp', 'daily_return']])
        elif choice == "2":
            print(df[['timestamp', 'MA_20']])
        elif choice == "3":
            print(df[['timestamp', 'volatility']])
        elif choice == "4":
            print(df[['timestamp', 'upper_band', 'lower_band']])
        elif choice == "5":
            break
        else:
            print("Invalid choice. Please try again.")
            
def preprocess_and_display_data():
    # Prompt user to choose a portfolio for preprocessing and displaying data
    portfolio_name = input("Enter portfolio name for data preprocessing: ")

    # Retrieve a list of stocks from the chosen portfolio
    stocks = pm.list_stocks_in_portfolio(portfolio_name, return_list=True)

    # Check if there are any stocks to preprocess
    if not stocks:
        print("No stocks to preprocess in the portfolio.")
        return

    # Ask the user to choose a specific stock from the list
    print("\nAvailable stocks in the portfolio:")
    for i, stock in enumerate(stocks):
        print(f"{i + 1}. {stock}")
    stock_choice = input("Enter the number of the stock to preprocess: ")
    try:
        selected_stock = stocks[int(stock_choice) - 1]
    except (IndexError, ValueError):
        print("Invalid selection. Please try again.")
        return

    # Fetch and preprocess data for the selected stock
    df = pm.fetch_stock_data_from_db(selected_stock)

    if df.empty:
        print(f"No data available for {selected_stock}.")
    else:
        df = preprocess_stock_data(df)
        display_stock_metrics(df)






