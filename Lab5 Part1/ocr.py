import PyPDF2
import pytesseract
from PIL import Image
import pdf2image
import os
import re
import requests
from bs4 import BeautifulSoup
import mysql.connector
import cv2
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import gc

pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract'

# Database configuration
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'james',
    'database': 'oil'
}

# Establish database connection
db = mysql.connector.connect(**db_config)
cursor = db.cursor()

drop_table_query = "DROP TABLE IF EXISTS well_info"
cursor.execute(drop_table_query)

create_table_query = """
CREATE TABLE IF NOT EXISTS well_info (
    api_number VARCHAR(255),
    longitude VARCHAR(255),
    latitude VARCHAR(255),
    well_name VARCHAR(255),
    address VARCHAR(255),
    stimulation_data TEXT,
    well_status VARCHAR(255),
    well_type VARCHAR(255),
    closest_city VARCHAR(255),
    barrels_of_oil INT,
    barrels_of_gas INT
)
"""
cursor.execute(create_table_query)

insert_query = """
INSERT INTO well_info (api_number, longitude, latitude, well_name, address, stimulation_data, 
well_status, well_type, closest_city, barrels_of_oil, barrels_of_gas) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

def try_extract_text_with_pypdf2(pdf_path):
    text = ""
    try:
        with open(pdf_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            for page in pdf_reader.pages:
                text += page.extract_text() + "\n"
    except Exception as e:
        print(f"Failed to extract text with PyPDF2 for {pdf_path}: {e}")
    return text

def preprocess_image_for_ocr(page):
    gray = page.convert('L')
    contrast = cv2.convertScaleAbs(np.array(gray), alpha=1.5, beta=0)
    _, binary = cv2.threshold(contrast, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    kernel = np.ones((2, 2), np.uint8)
    binary = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel, iterations=1)
    blur = cv2.GaussianBlur(binary, (5, 5), 0)
    return Image.fromarray(blur)

def extract_text_with_ocr_if_needed(pdf_path):
    print(f"Using OCR for {pdf_path}...")
    ocr_text = ""
    pages = pdf2image.convert_from_path(pdf_path, dpi=200) 
    
    for page in pages:
        processed_image = preprocess_image_for_ocr(page)
        config = '--oem 3 --psm 6'
        page_text = pytesseract.image_to_string(processed_image, config=config)
        ocr_text += page_text + "\n"
        del processed_image, page
        gc.collect()  
    
    return ocr_text

def extract_text_from_pdf(pdf_path):
    text = try_extract_text_with_pypdf2(pdf_path)
    secondary_api_pattern = r'(\d{2}-\d{3}-\d{5})'

    if not re.search(secondary_api_pattern, text, re.IGNORECASE):
        text = extract_text_with_ocr_if_needed(pdf_path)
        if re.search(secondary_api_pattern, text, re.IGNORECASE):
            print("Found API number using OCR.")
        else:
            print("API number could not be found.")
    else:
        print(f"Found API number using the PyPDF2 extracted text.")
    
    return text


def extract_info_from_text(text):
    api_number = re.search(r'(\d{2}-\d{3}-\d{5})', text.replace('\n', ' '), re.IGNORECASE)
    well_name = re.search(r'Well\s*Name\s*[:#]?\s*(.+?)\s*(?:API|\Z)', text, re.IGNORECASE)
    address = 'N/A'
    invalid_addresses = ["City State", "City State Zip Code", "I State IZip Code", "lstate !Zip Code", "DETAILS OF WORK", "Transporter"]

    longitude_patterns = [
        r'Longitude\s*[:#]?\s*([-\d.]+°[\s\d.]\'[\s\d.]"?[WE]?)',
        r'Longitude\s*[:#]?\s*([-\d.]+\.\d+)',
    ]
    latitude_patterns = [
        r'Latitude\s*[:#]?\s*([-\d.]+°[\s\d.]\'[\s\d.]"?[NS]?)',
        r'Latitude\s*[:#]?\s*([-\d.]+\.\d+)',
    ]

    def find_coordinate(patterns):
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        return 'N/A'

    longitude = find_coordinate(longitude_patterns)
    latitude = find_coordinate(latitude_patterns)

    addresses = re.finditer(r'Address[:#\s]*([^\n]+)', text, re.IGNORECASE)
    
    for match in addresses:
        potential_address = match.group(1).strip()
        # Check for invalid symbols, excluding '.' and ','
        if (all(c.isalnum() or c.isspace() or c in ".,-" for c in potential_address) and
            "@" not in potential_address and
            potential_address not in invalid_addresses and
            len(potential_address) >= 10):
            address = potential_address
            break

    if address == 'N/A':
        print("Address not found in the text.")

    stimulation_data = 'Extracted stimulation data'

    return {
        'api_number': api_number.group(1) if api_number else 'N/A',
        'longitude': longitude,
        'latitude': latitude,
        'well_name': well_name.group(1).strip() if well_name else 'N/A',
        'address': address,
        'stimulation_data': stimulation_data
    }


def scrape_additional_info(api, well_name):
    return {
        'well_status': 'Unknown',
        'well_type': 'Unknown',
        'closest_city': 'Unknown',
        'barrels_of_oil': 0,
        'barrels_of_gas': 0
    }

def process_pdf_file(filename, folder_path):
    try:
        pdf_path = os.path.join(folder_path, filename)
        print(f"Processing '{filename}'...")
        text = extract_text_from_pdf(pdf_path)
        info = extract_info_from_text(text)
        api_number = info['api_number'] if info['api_number'] != 'N/A' else 'Unknown'
        well_name = info['well_name'] if info['well_name'] != 'N/A' else 'Unknown'
        additional_info = scrape_additional_info(api_number, well_name)
        data = (
            api_number,
            info['longitude'] if info['longitude'] not in ['N/A', None] else 0.0,
            info['latitude'] if info['latitude'] not in ['N/A', None] else 0.0,
            well_name,
            info['address'] if info['address'] != 'N/A' else 'Unknown Address',
            info['stimulation_data'] if info['stimulation_data'] != 'N/A' else 'Unknown Stimulation Data',
            additional_info['well_status'],
            additional_info['well_type'],
            additional_info['closest_city'],
            additional_info['barrels_of_oil'],
            additional_info['barrels_of_gas']
        )
        cursor.execute(insert_query, data)
        db.commit()
        print(f"Successfully processed and stored data from '{filename}'.")
    except Exception as e:
        print(f"Error processing file '{filename}': {e}")

def process_pdf_files(folder_path):
    pdf_files = [f for f in os.listdir(folder_path) if f.endswith(".pdf")]
    tasks = [(filename, folder_path) for filename in pdf_files]
    with ThreadPoolExecutor(max_workers=5) as executor:  # change as needed
        executor.map(lambda task: process_pdf_file(*task), tasks)
    print("All files have been processed.")

if __name__ == "__main__":
    folder_path = './DSCI560_Lab5'
    process_pdf_files(folder_path)
    cursor.close()
    db.close()
    print("Processing complete.")