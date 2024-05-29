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
from datetime import datetime
import numpy as np
import traceback
from multiprocessing import Pool
from selenium.common.exceptions import TimeoutException
import traceback
from selenium.webdriver.common.action_chains import ActionChains
from config import config, get_newest_file
import glob

# Initialize Supabase client
supabase = config.supabase

# Get timezone offset and calculate current time in GMT+7
current_time_gmt7 = config.current_time_gmt7

# # Path to your extension .crx, extension_id file
extension_path, extension_id = config.get_paths_config()

db_config = config.get_database_config()


def fetch_asin_tokeyword(asin):
    conn = None
    try:
        # Connect to your database
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
        )
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Execute a query
        cur.execute(
            """
            WITH split_asins AS (
                SELECT unnest(string_to_array(asin, ',')) AS asin_element
                FROM reverse_product_lookup_helium
            )
            SELECT a.*
            FROM products_smartscount a
            LEFT JOIN products_relevant_smartscounts b
            ON a.asin = b.asin_relevant AND a.sys_run_date = b.sys_run_date
            WHERE a.sys_run_date = %s AND b.asin = %s 
            AND a.asin not in (select distinct asin_element from split_asins)
            AND b.relevancy_score > 9
            ORDER BY a.estimated_monthly_revenue DESC
            LIMIT 10
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
        # subset_size = 10
        subsets = ", ".join(asins)
        asin_parent = asin
        return asin_parent, subsets
    except Exception as e:
        print(f"Database error: {e}")
        return []
    finally:
        if conn:
            conn.close()


def captcha_solver(driver, chrome_options, API="7f97e318653cc85d2d7bc5efdfb1ea9f"):
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
        time.sleep(1)
        # Interact with the radio buttons
        token_radio_button = wait.until(
            EC.element_to_be_clickable((By.CLASS_NAME, "ant-radio-button-wrapper"))
        )
        token_radio_button.click()
    except Exception as e:
        # raise Exception
        print("Error during captcha:", e)


def wait_for_download_complete(download_dir, keyword, timeout=60):
    """Wait for the file with the given keyword in the name to be fully downloaded in the directory."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            files = [
                f
                for f in glob.glob(os.path.join(download_dir, "*"))
                if keyword in f and not f.endswith(".crdownload")
            ]
            if files:
                latest_file = max(files, key=lambda f: os.path.getctime(f))
                return latest_file
        except Exception as e:
            print(f"Error occurred: {e}, retrying...")
        time.sleep(1)
    return None


def scrap_helium_asin_keyword(
    driver,
    asin,
    download_dir,
    username="forheliumonly@gmail.com",
    password="qz6EvRm65L3HdjM2!!@#$",
):
    asin_parent, subsets = asin
    # Open Helium10
    driver.get("https://members.helium10.com/cerebro?accountId=1544526096")
    wait = WebDriverWait(driver, 30)
    print("login")

    # Login process
    try:
        username_field = wait.until(
            EC.visibility_of_element_located((By.ID, "loginform-email"))
        )
        username_field.send_keys(username)
        password_field = driver.find_element(By.ID, "loginform-password")
        password_field.send_keys(password)
        # Find the button by its class name (assuming class name is unique enough here)
        login_button = driver.find_element(By.CLASS_NAME, "btn-secondary")
        status_ready = False
        status_login = False
        while not status_login:
            while not status_ready:
                try:
                    status_element = wait.until(
                        EC.visibility_of_element_located(
                            (By.CSS_SELECTOR, "div.cm-addon-inner span")
                        )
                    )
                    status_text = status_element.text
                    if status_text == "Ready!":
                        print("Status: Ready")
                        status_ready = True
                    elif status_text == "In Process...":
                        print("Status: In Progress")
                    else:
                        print("Status: Unknown -", status_text)
                        time.sleep(1)
                except:
                    print("Error checking status")
                    time.sleep(1)
                    login_button.click()
                if status_ready == True:
                    status_login = True
                else:
                    try:
                        # Wait up to 10 seconds for the element to be present and visible
                        element = WebDriverWait(driver, 10).until(
                            EC.visibility_of_element_located(
                                (
                                    By.XPATH,
                                    "//a[@title='Dashboard' and @href='https://members.helium10.com/?accountId=1544526096']",
                                )
                            )
                        )
                        print("Element is visible")
                        status_login = True
                    except:
                        print("Element not visible")
        time.sleep(2)
        login_button = driver.find_element(By.CLASS_NAME, "btn-secondary")
        login_button.click()
        time.sleep(2)
    except Exception as e:
        print(f"Error during login: {e}")
        traceback.print_exc()
        return

    # driver.refresh("https://members.helium10.com/cerebro?accountId=1544526096")
    # time.sleep(5)
    try:
        print("asininput")
        asin_input = WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located(
                (
                    By.XPATH,
                    '//*[contains(@placeholder, "Enter up to ") and contains(@placeholder, " product identifiers for keyword comparison")]',
                )
            )
        )
        asin_input.clear()
        asin_input.send_keys(subsets)
        time.sleep(1)
        asin_input.send_keys(Keys.SPACE)

        print("Get Keyword Button")
        getkeyword_button = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//button[@data-testid='getkeywords']")
            )
        )
        print("Get Keyword Button_click")
        getkeyword_button.click()
        time.sleep(1)

        timeout = 10
        try:
            popup_visible = WebDriverWait(driver, timeout).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, ".sc-yRUbj.iYFpRQ"))
            )
            if popup_visible:
                run_new_search_button = driver.find_element(
                    By.CSS_SELECTOR, "button[data-testid='runnewsearch']"
                )
                run_new_search_button.click()
                print("Clicked on 'Run New Search'.")
        except TimeoutException:
            print("Popup not found within the timeout period.")
        driver.get_screenshot_as_file("screenshot.png")
        element = WebDriverWait(driver, 60000).until(
            EC.presence_of_element_located(
                (By.XPATH, "//button[@data-testid='export']")
            )
        )
        driver.execute_script("arguments[0].scrollIntoView();", element)

        print("Click Export data")
        export_data_button = driver.find_element(
            By.CSS_SELECTOR, "button[data-testid='exportdata']"
        )
        driver.execute_script("arguments[0].click();", export_data_button)
        print("Clicked the '...as a CSV file' option.")
        data_testid = "csv"
        actions = ActionChains(driver)
        csv_option = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, f'div[data-testid="{data_testid}"]')
            )
        )
        actions.move_to_element(csv_option).click().perform()

        print("newest_file")

        # Wait for the file with "US_AMAZON_cerebro" in its name to appear in the download directory
        newest_file_path = wait_for_download_complete(
            download_dir, "US_AMAZON_cerebro", timeout=60000
        )
        if newest_file_path:
            driver.quit()
            data_df = pd.read_csv(newest_file_path)
            data_df = data_df.replace("-", None)
            data_df["sys_run_date"] = datetime.now().strftime("%Y-%m-%d")
        else:
            print("No files found in the specified directory.")

        columns_to_extract = [
            "Keyword Phrase",
            "ABA Total Click Share",
            "ABA Total Conv. Share",
            "Keyword Sales",
            "Cerebro IQ Score",
            "Search Volume",
            "Search Volume Trend",
            "H10 PPC Sugg. Bid",
            "H10 PPC Sugg. Min Bid",
            "H10 PPC Sugg. Max Bid",
            "Sponsored ASINs",
            "Competing Products",
            "CPR",
            "Title Density",
            "Organic",
            "Sponsored Product",
            "Amazon Recommended",
            "Editorial Recommendations",
            "Amazon Choice",
            "Highly Rated",
            "Sponsored Brand Header",
            "Sponsored Brand Video",
            "Top Rated From Our Brand",
            "Trending Now",
            "Sponsored Rank (avg)",
            "Sponsored Rank (count)",
            "Amazon Recommended Rank (avg)",
            "Amazon Recommended Rank (count)",
            "Position (Rank)",
            "Relative Rank",
            "Competitor Rank (avg)",
            "Ranking Competitors (count)",
            "Competitor Performance Score",
            "sys_run_date",
        ]
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
            "sponsored_rank_avg",
            "sponsored_rank_count",
            "amazon_recommended_rank_avg",
            "amazon_recommended_rank_count",
            "position_rank",
            "relative_rank",
            "competitor_rank_avg",
            "ranking_competitors_count",
            "competitor_performance_score",
            "sys_run_date",
        ]

        data = data_df[columns_to_extract]
        data.columns = headers

        # Convert search_volume to numeric, forcing errors to NaN
        data["aba_total_click_share"] = pd.to_numeric(
            data["aba_total_click_share"], errors="coerce"
        )
        data["search_volume"] = pd.to_numeric(data["search_volume"], errors="coerce")
        data["keyword_sales"] = pd.to_numeric(data["keyword_sales"], errors="coerce")
        data["search_volume_trend"] = pd.to_numeric(
            data["search_volume_trend"], errors="coerce"
        )
        data["competing_products"] = pd.to_numeric(
            data["competing_products"], errors="coerce"
        )
        # Apply the filters
        filtered_data = data[
            (data["aba_total_click_share"].fillna(0.0) > 0)
            & (data["search_volume"] >= 1000)
            & (data["keyword_sales"] >= 100)
            & (data["search_volume_trend"] > 0)
            & (data["competing_products"] < 35)
        ]

        # Insert ASIN and ASIN Parent columns
        filtered_data.insert(0, "asin", "")
        filtered_data.insert(0, "asin_parent", "")
        try:
            rows_list = (
                filtered_data.replace({np.nan: None})
                .replace({"-": None})
                .to_dict(orient="records")
            )

            for row_dict in rows_list:
                row_dict["asin"] = str(subsets)
                row_dict["asin_parent"] = str(asin_parent)

            response = (
                supabase.table("reverse_product_lookup_helium")
                .upsert(rows_list)
                .execute()
            )

            if hasattr(response, "error") and response.error is not None:
                raise Exception(f"Error inserting rows: {response.error}")
            print("Rows inserted successfully")
        except Exception as e:
            print(f"Error with rows: {e}")
    except Exception as e:
        print(e)
        traceback.print_exc()
    finally:
        driver.quit()
