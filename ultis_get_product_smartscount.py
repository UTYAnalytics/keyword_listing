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
from config import config, format_header, get_newest_file
import psycopg2
import numpy as np

# Initialize Supabase client
supabase = config.supabase

# Get timezone offset and calculate current time in GMT+7
current_time_gmt7 = config.current_time_gmt7

db_config = config.get_database_config()


def fetch_existing_relevant_asin(asin):
    conn = None
    try:
        # Connect to your database
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
        )
        cur = conn.cursor()
        # Execute a query
        cur.execute(
            "SELECT distinct asin_relevant FROM products_relevant_smartscounts a left join (select distinct sys_run_date,asin from products_smartscount) b on a.asin_relevant=b.asin and a.sys_run_date=b.sys_run_date where a.sys_run_date = %s and a.asin= %s and b.asin is null",
            (
                str(current_time_gmt7.strftime("%Y-%m-%d")),
                asin,
            ),
        )

        # Fetch all results
        asins = cur.fetchall()
        # Convert list of tuples to list
        asins = [item[0] for item in asins]
        subset_size = 50
        subsets = [
            "\n".join(asins[i : i + subset_size])
            for i in range(0, len(asins), subset_size)
        ]
        return "\n".join(asins)
    except Exception as e:
        print(f"Database error: {e}")
        return []
    finally:
        if conn:
            conn.close()


def scrap_data_smartcount_product(driver, asin, download_dir):
    print("Products")
    wait = WebDriverWait(driver, 300000)
    try:
        driver.get("https://app.smartscout.com/app/products")
        filter_button = wait.until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    '//button[./mat-icon[@svgicon="filter"] and span[text()="Filters"]]',
                )
            )
        )
        filter_button.click()

        # 2. Wait for the overlay to be visible
        overlay_present = EC.presence_of_element_located(
            (By.CLASS_NAME, "cdk-overlay-backdrop")
        )
        wait.until(overlay_present)

        icon = wait.until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//mat-icon[@tooltip='Click here to input multiple ASINs']",
                )
            )
        )
        # Click the icon
        icon.click()

        # Find the textarea using its attributes
        textarea = driver.find_element(By.CSS_SELECTOR, "textarea[mdinput]")
        # Paste text into the textarea
        # Clear the existing text in the textarea
        textarea.clear()

        # Paste the text into the textarea
        textarea.send_keys(asin)
        time.sleep(5)
        # Find the button using its attributes
        apply_button = wait.until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    '//button[@mat-button and @mat-raised-button and @color="primary"]',
                )
            )
        )
        # Click the button
        apply_button.click()
        time.sleep(5)
        search_button = wait.until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="btnSearchBrands"]'))
        )
        search_button.click()
        time.sleep(5)
        # Assuming 'sideBarButtons' is the reference for the parent div
        side_buttons_div = driver.find_element(By.CLASS_NAME, "ag-side-buttons")

        # Wait for the button to be clickable
        wait = WebDriverWait(driver, 10)
        column_button = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//span[text()='Columns']/ancestor::button")
            )
        )
        # Click the "Excel" button
        column_button.click()

        # Wait for the checkbox to be clickable
        checkbox = wait.until(
            EC.element_to_be_clickable(
                (By.CLASS_NAME, "ag-column-select-header-checkbox")
            )
        )

        # Click the checkbox
        checkbox.click()

        # Wait for the button to be clickable
        wait = WebDriverWait(driver, 10)
        excel_button = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//span[text()='Export']/ancestor::button")
            )
        )

        # Click the "Excel" button
        excel_button.click()

        image = wait.until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, 'img[mattooltip="Export as CSV"]')
            )
        )

        # Click the image
        image.click()
        time.sleep(15)
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
            "asin",
            "title",
            "brand",
            "category",
            "rank",
            "primary_subcategory",
            "subcategory_rank",
            "variation_page",
            "parent_asin",
            "amazon_in_stock_rate",
            "number_of_sellers",
            "out_of_stock",
            "estimated_monthly_units_sold",
            "buy_box_price",
            "estimated_monthly_revenue",
            "trailing_12_months",
            "one_month_growth",
            "twelve_month_growth",
            "launch_date",
            "child_ratings_count",
            "total_ratings_count",
            "rating",
            "number_of_fba_sellers",
            "buy_box_equity",
            "revenue_equity",
            "margin_equity",
            "product_page_score",
            "number_of_items",
            "model",
            "part_number",
            "manufacturer",
            "upc",
            "notes",
            "sys_run_date",
        ]

        integer_columns = [
            "rank",
            "subcategory_rank",
            "amazon_in_stock_rate",
            "number_of_sellers",
            "estimated_monthly_units_sold",
            "child_ratings_count",
            "total_ratings_count",
            "number_of_fba_sellers",
            "number_of_items",
        ]
        data = data.drop(data.columns[0], axis=1)
        # Concatenate the URL with the data in the second column
        data.rename(columns={data.columns[0]: "amazon_image"}, inplace=True)
        data[data.columns[0]] = (
            "https://images-na.ssl-images-amazon.com/images/I/"
            + data[data.columns[0]].astype(str)
        )
        data.columns = headers
        for col in integer_columns:
            data[format_header(col)] = (
                data[format_header(col)].astype(float).fillna(0).astype(int)
            )
        print(data.head())
        try:
            # Convert rows to list of dictionaries and handle NaN values
            rows_list = data.replace({np.nan: None}).to_dict(orient="records")

            # Insert the rows into the database using executemany
            response = (
                supabase.table("products_smartscount").upsert(rows_list).execute()
            )
            if hasattr(response, "error") and response.error is not None:
                raise Exception(f"Error inserting rows: {response.error}")
            print(f"Rows inserted successfully")
        except Exception as e:
            print(f"Error with rows: {e}")
    except Exception as e:
        print(e)
        traceback.print_exc()
