# Remember to close the browser
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
import re
import unicodedata
import imaplib
import email
import re
from datetime import datetime, timedelta
import numpy as np
from selenium.webdriver.chrome.service import Service
import traceback
from webdriver_manager.chrome import ChromeDriverManager
from multiprocessing import Pool
from selenium.common.exceptions import TimeoutException
import traceback
import subprocess
from psycopg2.extras import DictCursor

SUPABASE_URL = "https://sxoqzllwkjfluhskqlfl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN4b3F6bGx3a2pmbHVoc2txbGZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDIyODE1MTcsImV4cCI6MjAxNzg1NzUxN30.FInynnvuqN8JeonrHa9pTXuQXMp9tE4LO0g5gj0adYE"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

current_utc_time = datetime.utcnow()
# Calculate the time difference for GMT+7
gmt7_offset = timedelta(hours=7)
current_time_gmt7 = current_utc_time + gmt7_offset

# Replace these with your Keepa username and password
username = "uty.tra@thebargainvillage.com"
password = "D8RLPA7$kxG!9zh"

dir_path = os.path.dirname(os.path.realpath(__file__))
# Create a temporary directory for downloads
with tempfile.TemporaryDirectory() as download_dir:
    # and if it doesn't exist, download it automatically,
    # then add chromedriver to path
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    }
    options = [
        # Define window size here
        "--ignore-certificate-errors",
        "--headless=new",
        "--disable-gpu",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--window-size=1920,1080",
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "accept-language=en-US",
    ]

    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_extension(
        dir_path + "/CapMonster-Cloud-—-automated-captcha-solver.crx"
    )
    for option in options:
        chrome_options.add_argument(option)


def fetch_existing_relevant_asin(asin):
    conn = None
    try:
        # Connect to your database
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres.sxoqzllwkjfluhskqlfl",
            password="5giE*5Y5Uexi3P2",
            host="aws-0-us-west-1.pooler.supabase.com",
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
        return asins
    except Exception as e:
        print(f"Database error: {e}")
        return []
    finally:
        if conn:
            conn.close()

def fetch_existing_relevant_asin_main():
    conn = None
    try:
        # Connect to your database
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres.sxoqzllwkjfluhskqlfl",
            password="5giE*5Y5Uexi3P2",
            host="aws-0-us-west-1.pooler.supabase.com",
        )
        cur = conn.cursor()
        # Execute a query
        cur.execute(
            "SELECT distinct asin_relevant FROM products_relevant_smartscounts a where a.sys_run_date = %s ",
            (
                str(current_time_gmt7.strftime("%Y-%m-%d")),
            ),
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

def format_header(header):
    # Convert to lowercase
    header = header.lower()
    # Replace spaces with underscores
    header = header.replace(" ", "_")
    # Remove Vietnamese characters by decomposing and keeping only ASCII
    header = (
        unicodedata.normalize("NFKD", header).encode("ASCII", "ignore").decode("ASCII")
    )
    return header


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
    try:
        smartscouts_next_login(driver, username, password)
        scrap_data_smartcount_relevant_product(driver, asin)
        time.sleep(5)
        relevant_asins = fetch_existing_relevant_asin(asin)
        scrap_data_smartcount_product(driver, relevant_asins)
        time.sleep(5)
        captcha_solver(driver)
        time.sleep(2)
        scrap_helium_asin_keyword(driver, fetch_asin_tokeyword(asin))
    finally:
        driver.quit()


def get_newest_file(directory):
    files = glob.glob(os.path.join(directory, "*"))
    if not files:  # Check if the files list is empty
        return None
    newest_file = max(files, key=os.path.getmtime)
    return newest_file


def scrap_data_smartcount_relevant_product(driver, asin):
    print("searchterm")
    wait = WebDriverWait(driver, 30)
    try:
        print("scroll")
        element = WebDriverWait(driver, 10).until(
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


def scrap_data_smartcount_product(driver, asin):
    print("Products")
    wait = WebDriverWait(driver, 30)
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
        text_to_paste = "\n".join(asin)
        # Paste the text into the textarea
        textarea.send_keys(text_to_paste)
        time.sleep(2)
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


# def connect_vpn(config_file):
#     try:
#         subprocess.run(["openvpn", "--config", config_file], check=True)
#         print(f"Connected to VPN using {config_file}")
#     except subprocess.CalledProcessError as e:
#         print(f"Error connecting to VPN: {e}")


# def disconnect_vpn():
#     try:
#         subprocess.run(["pkill", "openvpn"], check=True)
#         print("Disconnected from VPN")
#     except subprocess.CalledProcessError as e:
#         print(f"Error disconnecting from VPN: {e}")


def fetch_asin_tokeyword(asin):
    conn = None
    try:
        # Connect to your database
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres.sxoqzllwkjfluhskqlfl",
            password="5giE*5Y5Uexi3P2",
            host="aws-0-us-west-1.pooler.supabase.com",
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Execute a query
        cur.execute(
            """
            SELECT a.*
            FROM products_smartscount a
            LEFT JOIN products_relevant_smartscounts b
            ON a.asin = b.asin_relevant AND a.sys_run_date = b.sys_run_date
            WHERE a.sys_run_date = %s AND b.asin = %s
            ORDER BY a.estimated_monthly_revenue DESC
            LIMIT 20
            """,
            (
                str(current_time_gmt7.strftime("%Y-%m-%d")),
                asin,
            ),
        )

        # Fetch all results
        results = cur.fetchall()
        # Extract the asin values from the results
        asins = [item["asin"] for item in results]
        subset_size = 10
        subsets = [
            ", ".join(asins[i : i + subset_size])
            for i in range(0, len(asins), subset_size)
        ]

        return subsets
    except Exception as e:
        print(f"Database error: {e}")
        return []
    finally:
        if conn:
            conn.close()


def captcha_solver(driver, API="7f97e318653cc85d2d7bc5efdfb1ea9f"):
    # Path to your extension .crx file
    extension_path = "CapMonster-Cloud-—-automated-captcha-solver.crx"
    extension_id = (
        "pabjfbciaedomjjfelfafejkppknjleh"  # Replace with your actual extension ID
    )

    # Create a temporary Chrome user data directory
    user_data_dir = os.path.join(os.getcwd(), "temp_user_data_dir")
    os.makedirs(user_data_dir, exist_ok=True)
    chrome_options.add_extension(extension_path)
    chrome_options.add_argument(f"user-data-dir={user_data_dir}")
    # Navigate to the extension's URL
    extension_url = f"chrome-extension://{extension_id}/popup.html"  # Replace 'popup.html' with your extension's specific page if different
    driver.get(extension_url)

    # Interact with the extension's elements
    try:
        # Example: Input text into a text field
        wait = WebDriverWait(driver, 10)
        input_field = wait.until(
            EC.visibility_of_element_located(
                (By.CSS_SELECTOR, "input#client-key-input")
            )
        )

        # Enter text into the input field
        input_text = API
        input_field.clear()
        input_field.send_keys(input_text)

        # Wait for the save button to be clickable, then click it
        save_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button#client-key-save-btn"))
        )
        save_button.click()
    except Exception as e:
        # raise Exception
        print("Error during captcha:", e)


def scrap_helium_asin_keyword(
    driver, asin, username="forheliumonly@gmail.com", password="qz6EvRm65L3HdjM2!!@#$"
):
    # connect_vpn("/us-hou.prod.surfshark.comsurfshark_openvpn_tcp.ovpn")
    # Open Helium10
    driver.get("https://members.helium10.com/cerebro?accountId=1544526096")
    wait = WebDriverWait(driver, 30)
    print("login")
    # Login process
    try:

        # driver.get("https://members.helium10.com/cerebro?accountId=1544526096")
        username_field = wait.until(
            EC.visibility_of_element_located((By.ID, "loginform-email"))
        )
        username_field.send_keys(username)
        password_field = driver.find_element(By.ID, "loginform-password")
        password_field.send_keys(password)
        time.sleep(50)
        password_field.send_keys(Keys.RETURN)
    except Exception as e:
        # raise Exception
        print("Error during login:", e)
    # Navigate to the Reverse Asin
    try:
        print("asininput")

        asin_input = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located(
                (
                    By.CSS_SELECTOR,
                    'input[placeholder="Enter up to 10 product identifiers for keyword comparison."]',
                )
            )
        )

        # You can also set the maximum value if needed
        asin_input.clear()
        asin_input.send_keys(asin)
        time.sleep(5)
        asin_input.send_keys(Keys.SPACE)
        print("Get Keyword Button")
        getkeyword_button = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[@data-testid='getkeywords']")
            )
        )
        print("Get Keyword Button_click")
        getkeyword_button.click()
        time.sleep(2)

        timeout = 10
        try:
            # Wait for the popup to be visible
            popup_visible = WebDriverWait(driver, timeout).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, ".sc-yRUbj.iYFpRQ"))
            )
            # If the popup is visible, find and click the "Run New Search" button
            if popup_visible:
                run_new_search_button = driver.find_element(
                    By.CSS_SELECTOR, "button[data-testid='runnewsearch']"
                )
                run_new_search_button.click()
                print("Clicked on 'Run New Search'.")
        except TimeoutException:
            # If the popup is not found within the timeout, handle it (e.g., by logging or skipping)
            print("Popup not found within the timeout period.")

        time.sleep(25)
        driver.get_screenshot_as_file("screenshot.png")

        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.XPATH, "//button[@data-testid='export']")
            )  # Replace "element_id" with the actual ID of the element
        )
        driver.execute_script("arguments[0].scrollIntoView();", element)

        print("Click Export data")
        export_data_button = driver.find_element(
            By.CSS_SELECTOR, "button[data-testid='exportdata']"
        )
        # Use JavaScript to click on the element
        driver.execute_script("arguments[0].click();", export_data_button)
        time.sleep(2)

        print("Clicked the '...as a CSV file' option.")
        data_testid = "csv"
        csv_option = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, f'div[data-testid="{data_testid}"]')
            )
        )
        csv_option.click()

        time.sleep(5)
        print("newest_file")

        file_path = download_dir

        newest_file_path = get_newest_file(file_path)

        if newest_file_path:
            data = pd.read_csv(newest_file_path)
            # data["sys_run_date"] = current_time_gmt7.strftime("%Y-%m-%d %H:%M:%S")

            data = data.replace("-", None)
            data["sys_run_date"] = current_time_gmt7
            # Proceed with the database insertion
        else:
            print("No files found in the specified directory.")
        # Extract the header row
        headers = [
            "keyword_phrase",
            "aba_total_click_share",
            "aba_total_conv_share",
            "keyword_sales",
            "cerebro_iq_score",
            "search_volume",
            "search_volume_trend",
            "h10_ppc_sugg_bid",
            "h10_ppc_sugg_min_bid",
            "h10_ppc_sugg_max_bid",
            "sponsored_asins",
            "competing_products",
            "cpr",
            "title_density",
            "organic",
            "sponsored_product",
            "amazon_recommended",
            "editorial_recommendations",
            "amazon_choice",
            "highly_rated",
            "sponsored_brand_header",
            "sponsored_brand_video",
            "top_rated_from_our_brand",
            "trending_now",
            "amazon_rec_rank",
            "sponsored_rank",
            "organic_rank",
            "sys_run_date",
        ]

        data.columns = headers
        data.insert(0, "asin", "")

        try:
            # Convert rows to list of dictionaries and handle NaN values
            rows_list = data.replace({np.nan: None}).to_dict(orient="records")

            # Generate MD5 hash as the primary key for each row
            for row_dict in rows_list:
                row_dict["asin"] = str(asin)

            # Insert the rows into the database using executemany
            response = (
                supabase.table("reverse_product_lookup_helium")
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
    # finally:
    #     disconnect_vpn()


def main(asins):
    with Pool(processes=len(asins)) as pool:
        pool.map(start_driver, asins)


if __name__ == "__main__":
    # Example list of ASINs input by the user
    user_asins = ["B07VPWR7YY"]
    user_asins = [asin for asin in user_asins if not fetch_existing_relevant_asin_main()]
    if user_asins:
        main(user_asins)
