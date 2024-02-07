import portfolio_management as pm
import data_preprocessing as dp

def main_menu():
    print("\n1. Add new portfolio")
    print("2. Delete a portfolio")
    print("3. Add stock to portfolio")
    print("4. Remove stock from portfolio")
    print("5. View portfolio details")
    print("6. View distinct stocks in portfolio")
    print("7. Preprocess and Display Stock Data")
    print("8. Exit")
    return input("Enter your choice: ")

def handle_user_input():
    while True:
        pm.list_all_portfolios()  # Display available portfolios at the start
        choice = main_menu()
        if choice == '1':
            portfolio_name = input("Enter new portfolio name: ")
            pm.add_portfolio(portfolio_name)
        elif choice == '2':
            portfolio_name = input("Enter portfolio name to delete: ")
            pm.delete_portfolio(portfolio_name)
        elif choice == '3':
            portfolio_name = input("Enter portfolio name: ")
            stock_symbol = input("Enter stock symbol to add: ").upper()
            start_date = input("Enter start date (YYYY-MM-DD): ")
            end_date = input("Enter end date (YYYY-MM-DD): ")
            pm.fetch_and_store_stock_data(portfolio_name, stock_symbol, start_date, end_date)
        elif choice == '4':
            portfolio_id = input("Enter portfolio name: ")
            pm.list_stocks_in_portfolio(portfolio_id)  # Show current stocks before removing
            stock_symbol = input("\nEnter stock symbol to remove: ").upper()
            pm.remove_stock_from_portfolio(portfolio_id, stock_symbol)
        elif choice == '5':
            portfolio_name = input("Enter portfolio name to view details: ")
            pm.view_portfolio(portfolio_name)
        elif choice == '6':
            portfolio_name = input("Enter portfolio name to view distinct stocks: ")
            pm.list_distinct_stocks_in_portfolio(portfolio_name)
        elif choice == '7':
            dp.preprocess_and_display_data()
        elif choice == '8':
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    handle_user_input()
