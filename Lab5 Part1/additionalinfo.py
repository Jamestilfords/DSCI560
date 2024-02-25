import mysql.connector
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def setup_selenium_driver():
    firefox_options = Options()
    # firefox_options.add_argument("--headless")
    service = FirefoxService(executable_path=GeckoDriverManager().install())
    driver = webdriver.Firefox(service=service, options=firefox_options)
    return driver

def update_well_info(db_config, api_number, well_status, well_type, closest_city, barrels_of_oil, barrels_of_gas):
    db = mysql.connector.connect(**db_config)
    cursor = db.cursor()
    update_query = """
    UPDATE well_info
    SET well_status = %s, well_type = %s, closest_city = %s, barrels_of_oil = %s, barrels_of_gas = %s
    WHERE api_number = %s
    """
    cursor.execute(update_query, (well_status, well_type, closest_city, barrels_of_oil, barrels_of_gas, api_number))
    db.commit()
    cursor.close()
    db.close()

def scrape_additional_info(db_config):
    driver = setup_selenium_driver()
    db = mysql.connector.connect(**db_config)
    cursor = db.cursor()
    cursor.execute("SELECT api_number, well_name FROM well_info")
    for (api_number, well_name) in cursor:
        # Skip the iteration if both API number and well name are "Unknown"
        if api_number == "Unknown" and well_name == "Unknown":
            print(f"Skipping well with API number '{api_number}' and well name '{well_name}' because both are 'Unknown'.")
            continue

        driver.get("https://www.drillingedge.com/search")
        
        if api_number != "Unknown":
            api_search_box = driver.find_element(By.NAME, "api_no")
            api_search_box.clear()
            api_search_box.send_keys(api_number)
            search_button = driver.find_element(By.XPATH, "//input[@type='submit'][@value='Search Database']")
            search_button.click()
        elif well_name != "Unknown":  # Fall back to well name if API number is "Unknown"
            well_name_search_box = driver.find_element(By.NAME, "well_name")
            well_name_search_box.clear()
            well_name_search_box.send_keys(well_name)
            search_button = driver.find_element(By.XPATH, "//input[@type='submit'][@value='Search Database']")
            search_button.click()
        else:
            # Skip if both are "Unknown"
            continue

        # Wait for the search results to load
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".table")) 
        )
        # Click the first link in the search results
        try:
            first_result_link = WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".table > tbody:nth-child(1) > tr:nth-child(2) > td:nth-child(2) > a:nth-child(1)"))  # Adjusted selector
            )
            first_result_link.click()
            
            # Wait for the well's detail page to load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".skinny > tbody:nth-child(1)")) 
            )
            
            # Check if "No Production Data Available" is present on the page
            oil_production_present = driver.find_elements(By.XPATH, "//*[contains(text(), 'Barrels of Oil Produced in')]")
            gas_production_present = driver.find_elements(By.XPATH, "//*[contains(text(), 'MCF of Gas Produced in')]")

            # If there is no production data, the elements list will be empty
            if not oil_production_present or not gas_production_present:
                print(f"No production data available for well with API number '{api_number}'. Extracting other information only.")
                # Extract well status, type, and closest city
                well_status_xpath = "/html/body/section[3]/div/article[3]/article/div[2]/table/tbody/tr[3]/td[1]"
                well_status_element = driver.find_element(By.XPATH, well_status_xpath)
                well_status = well_status_element.text if well_status_element else 'N/A'

                well_type_xpath = "/html/body/section[3]/div/article[3]/article/div[2]/table/tbody/tr[3]/td[2]"
                well_type_element = driver.find_element(By.XPATH, well_type_xpath)
                well_type = well_type_element.text if well_type_element else 'N/A'

                closest_city_xpath = "/html/body/section[3]/div/article[3]/article/div[2]/table/tbody/tr[5]/td[2]"
                closest_city_element = driver.find_element(By.XPATH, closest_city_xpath)
                closest_city = closest_city_element.text if closest_city_element else 'N/A'

                # Set barrels of oil and gas to 'N/A' or some default value
                barrels_of_oil = 'N/A'
                barrels_of_gas = 'N/A'
            # Implement logic to navigate to the well page and scrape the required information
            else:
                well_status_xpath = "/html/body/section[3]/div/article[3]/article/div[2]/table/tbody/tr[3]/td[1]"
                well_status_element = driver.find_element(By.XPATH, well_status_xpath)
                well_status = well_status_element.text if well_status_element else 'N/A'

                well_type_xpath = "/html/body/section[3]/div/article[3]/article/div[2]/table/tbody/tr[3]/td[2]"
                well_type_element = driver.find_element(By.XPATH, well_type_xpath)
                well_type = well_type_element.text if well_type_element else 'N/A'

                closest_city_xpath = "/html/body/section[3]/div/article[3]/article/div[2]/table/tbody/tr[5]/td[2]"
                closest_city_element = driver.find_element(By.XPATH, closest_city_xpath)
                closest_city = closest_city_element.text if closest_city_element else 'N/A'

                barrels_of_oil_xpath = "/html/body/section[3]/div/article[1]/section[2]/p[1]/span"
                barrels_of_oil_element = driver.find_element(By.XPATH, barrels_of_oil_xpath)
                barrels_of_oil = barrels_of_oil_element.text if barrels_of_oil_element else 'N/A'

                barrels_of_gas_xpath = "/html/body/section[3]/div/article[1]/section[2]/p[2]/span"
                barrels_of_gas_element = driver.find_element(By.XPATH, barrels_of_gas_xpath)
                barrels_of_gas = barrels_of_gas_element.text if barrels_of_gas_element else 'N/A'

        except Exception as e:
            print(f"Error while navigating to well's detail page for API {api_number}: {e}")
            continue

        # Convert production numbers to integers
        try:
            barrels_of_oil = barrels_of_oil.lower().replace('k', '00').replace('.', '').replace(' ', '')
            barrels_of_oil = int(barrels_of_oil) if barrels_of_oil != 'n/a' else 0
        except ValueError as e:
            print(f"Error converting barrels_of_oil for API {api_number}: {e}")
            barrels_of_oil = 0

        try:
            barrels_of_gas = barrels_of_gas.lower().replace('k', '00').replace('.', '').replace(' ', '')
            barrels_of_gas = int(barrels_of_gas) if barrels_of_gas != 'n/a' else 0
        except ValueError as e:
            print(f"Error converting barrels_of_gas for API {api_number}: {e}")
            barrels_of_gas = 0

        update_well_info(db_config, api_number, well_status, well_type, closest_city, barrels_of_oil, barrels_of_gas)
    
    cursor.close()
    db.close()
    driver.quit()

if __name__ == "__main__":
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'james',
        'database': 'oil'
    }
    scrape_additional_info(db_config)
