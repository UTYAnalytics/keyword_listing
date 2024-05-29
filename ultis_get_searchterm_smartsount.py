# main.py
import os
import tempfile
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import traceback
import numpy as np
from config import config, format_header, get_newest_file

# Initialize Supabase client
supabase = config.supabase

# Get timezone offset and calculate current time in GMT+7
current_time_gmt7 = config.current_time_gmt7

# # Path to your extension .crx, extension_id file
extension_path, extension_id = config.get_paths_config()

db_config = config.get_database_config()


def scrap_data_smartcount_relevant_product(driver, asin, download_dir):
    print("searchterm")
    wait = WebDriverWait(driver, 30)
    try:
        print("scroll")
        element = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located(
                (By.XPATH, '//*[@id="navSearchTerms"]')
            )  # Replace "element_id" with the actual ID of the element
        )
        driver.execute_script("arguments[0].scrollIntoView();", element)
        print("searchtermbutton")
        searchterm_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="navSearchTerms"]'))
        )
        print("searchtermbutton_click")
        searchterm_button.click()
        time.sleep(2)
        print("asininput")
        asin_input = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located(
                (By.CSS_SELECTOR, '.mat-form-field-infix input[formcontrolname="asin"]')
            )
        )
        # You can also set the maximum value if needed
        asin_input.clear()
        asin_input.send_keys(asin)
        time.sleep(12)
        print("searchbutton")
        search_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="btnSearchProducts"]'))
        )
        search_button.click()
        time.sleep(2)
        # Find the "Products" element using CSS Selector
        print("relevant_products_button")
        relevant_products_button = wait.until(
            EC.visibility_of_element_located(
                (
                    By.XPATH,
                    "//div[contains(@class, 'fixed-tab') and contains(text(), 'Relevant Products')]",
                )
            )
        )
        # Click on the "Products" element
        relevant_products_button.click()
        time.sleep(5)
        # Wait for the button to be clickable
        wait = WebDriverWait(driver, 10)
        print("excel_button")
        excel_button = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//span[text()='Excel']/ancestor::button")
            )
        )

        # Click the "Excel" button
        excel_button.click()
        print("image")
        image = wait.until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, 'img[mattooltip="Export as CSV"]')
            )
        )

        # Click the image
        image.click()
        time.sleep(10)
        print("newest_file")

        file_path = download_dir

        newest_file_path = get_newest_file(file_path)

        if newest_file_path:
            data = pd.read_csv(newest_file_path)
            data["sys_run_date"] = current_time_gmt7.strftime("%Y-%m-%d")
            # Proceed with the database insertion
        else:
            print("No files found in the specified directory.")

        # Extract the header row
        headers = [
            "amazon_image",
            "asin_relevant",
            "title",
            "brand",
            "common_search_terms",
            "relevancy_score",
            "sys_run_date",
        ]

        integer_columns = [
            "relevancy_score",
        ]
        data = data.drop(data.columns[0], axis=1)
        # Concatenate the URL with the data in the second column
        data.rename(columns={data.columns[0]: "amazon_image"}, inplace=True)
        data[data.columns[0]] = (
            "https://images-na.ssl-images-amazon.com/images/I/"
            + data[data.columns[0]].astype(str)
        )
        data.columns = headers
        data.insert(0, "asin", "")
        for col in integer_columns:
            data[format_header(col)] = (
                data[format_header(col)].astype(float).fillna(0.00)
            )

        print(data.head())
        try:
            # Convert rows to list of dictionaries and handle NaN values
            rows_list = data.replace({np.nan: None}).to_dict(orient="records")

            # Generate MD5 hash as the primary key for each row
            for row_dict in rows_list:
                row_dict["asin"] = str(asin)

            # Insert the rows into the database using executemany
            response = (
                supabase.table("products_relevant_smartscounts")
                .upsert(rows_list)
                .execute()
            )
            if hasattr(response, "error") and response.error is not None:
                raise Exception(f"Error inserting rows: {response.error}")
            print(f"Rows inserted successfully")
        except Exception as e:
            print(f"Error with rows: {e}")
    except Exception as e:
        print(e)
        traceback.print_exc()
