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
                    password_field.send_keys(Keys.RETURN)
                if status_ready == True:
                    status_login = True
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
        password_field.send_keys(Keys.RETURN)
        time.sleep(2)
    except Exception as e:
        print(f"Error during login: {e}")
        traceback.print_exc()
        return

    for subset in subsets:
        driver.refresh()
        time.sleep(5)
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
            asin_input.send_keys(subset)
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
                    EC.visibility_of_element_located(
                        (By.CSS_SELECTOR, ".sc-yRUbj.iYFpRQ")
                    )
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

            time.sleep(25)
            print("newest_file")
            file_path = download_dir
            newest_file_path = get_newest_file(file_path)

            if newest_file_path:
                data_df = pd.read_csv(newest_file_path)
                data_df = data_df.replace("-", None)
                data_df["sys_run_date"] = datetime.now().strftime("%Y-%m-%d")
            else:
                print("No files found in the specified directory.")
                continue

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
            data.insert(0, "asin", "")
            data.insert(0, "asin_parent", "")
            try:
                rows_list = (
                    data.replace({np.nan: None})
                    .replace({"-": None})
                    .to_dict(orient="records")
                )

                for row_dict in rows_list:
                    row_dict["asin"] = str(subset)
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
