import tempfile
import psycopg2.extras
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time
import pandas as pd
import psycopg2
import glob
from supabase import create_client, Client
import unicodedata
from datetime import datetime, timedelta
import numpy as np
from selenium.webdriver.chrome.service import Service
import traceback
from webdriver_manager.chrome import ChromeDriverManager
from multiprocessing import Pool
from selenium.common.exceptions import TimeoutException
import traceback
from selenium.webdriver.common.action_chains import ActionChains
from config import config, format_header, get_newest_file
from ultis_get_product_smartscount import (
    fetch_existing_relevant_asin,
    scrap_data_smartcount_product,
)
from ultis_get_searchterm_smartsount import scrap_data_smartcount_relevant_product
from ultis_scrap_helium_cerebro import (
    fetch_asin_tokeyword,
    captcha_solver,
    scrap_helium_asin_keyword,
)

# Initialize Supabase client
supabase = config.supabase

# Get timezone offset and calculate current time in GMT+7
current_time_gmt7 = config.current_time_gmt7

# Get Selenium configuration
chrome_options_list = config.get_selenium_config()

# # Path to your extension .crx, extension_id file
extension_path, extension_id = config.get_paths_config()

db_config = config.get_database_config()

username, password = config.get_smartscount()

# Create a temporary directory for downloads
with tempfile.TemporaryDirectory() as download_dir:
    # Chrome options
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    for option in chrome_options_list:
        chrome_options.add_argument(option)

    dir_path = os.path.dirname(os.path.realpath(__file__))
    chrome_options.add_extension(os.path.join(dir_path, extension_path))


def fetch_existing_relevant_asin_main():
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
            "SELECT distinct asin_relevant FROM products_relevant_smartscounts a where a.sys_run_date = %s ",
            (str(current_time_gmt7.strftime("%Y-%m-%d")),),
        )

        # Fetch all results
        asins = cur.fetchall()
        # Convert list of tuples to list
        asins = [item[0] for item in asins]
        return asins
    except Exception as e:
        print(f"Database error: {e}")
        return []
    finally:
        if conn:
            conn.close()


def smartscouts_next_login(driver, username=username, password=password):
    driver.get("https://app.smartscout.com/sessions/signin")
    wait = WebDriverWait(driver, 30)
    # Login process
    try:
        username_field = wait.until(
            EC.visibility_of_element_located((By.ID, "username"))
        )
        username_field.send_keys(username)

        password_field = driver.find_element(By.ID, "password")
        password_field.send_keys(password)
        password_field.send_keys(Keys.RETURN)
        time.sleep(2)
    except Exception as e:
        # raise Exception
        print("Error during login:", e)


def start_driver(asin):
    # chromedriver_path = os.path.join(dir_path, 'chromedriver.exe')  # Ensure this path is correct
    chrome_service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
    user_asins = []
    try:
        user_asins = [
            asin for asin in user_asins if not fetch_existing_relevant_asin_main()
        ]
        if user_asins:
            smartscouts_next_login(driver, username, password)
            scrap_data_smartcount_relevant_product(driver, asin, download_dir)
            time.sleep(5)
            relevant_asins = fetch_existing_relevant_asin(asin)
            scrap_data_smartcount_product(driver, relevant_asins, download_dir)
        captcha_solver(driver, chrome_options)
        scrap_helium_asin_keyword(driver, fetch_asin_tokeyword(asin), download_dir)
    finally:
        driver.quit()


def main(asins):
    with Pool(processes=len(asins)) as pool:
        pool.map(start_driver, asins)


if __name__ == "__main__":
    # Example list of ASINs input by the user
    user_asins = ["B07VPWR7YY"]
    main(user_asins)
