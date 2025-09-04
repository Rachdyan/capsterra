from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import time
import random
from functools import partial
import curl_cffi
from seleniumbase import SB
from selenium.common.exceptions import TimeoutException
import numpy as np
import multiprocessing
import os
from urllib3.exceptions import ReadTimeoutError

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
# Create result directory if it doesn't exist
result_dir = os.path.join(script_dir, "result")
os.makedirs(result_dir, exist_ok=True)

user = os.environ['PROXY_USER']
password = os.environ['PROXY_PASSWORD']
proxy_host = os.environ['PROXY_HOST']
proxy_port = os.environ['PROXY_PORT']

proxy_string = f"{user}:{password}@{proxy_host}:{proxy_port}"

# proxies = { 
#               "http"  : proxy_string, 
#               "https" : proxy_string, 
#             }


def sb_uc_open_with_retry(sb, url, max_attempts=3, sleep_time=4):
    for attempt in range(1, max_attempts + 1):
        try:
            sb.uc_open_with_reconnect(url, 5)
            sb.sleep(sleep_time)
            return True
        except Exception as e:
            print(f"Attempt {attempt} failed for {url}: {e}")
            if attempt == max_attempts:
                print(f"Max attempts reached for {url}. Skipping.")
                return False
            sb.sleep(sleep_time)
    return False


def get_product_overview(product_div, row):
        
    result = dict(row)
    try:
        product_info_div = product_div.select_one('div')
    except Exception as e:
        print(f"Error extracting product_info_div: {e}")
        product_info_div = None

    try:
        product_id = product_info_div['data-prodid']
    except Exception as e:
        print(f"Error extracting product_id: {e}")
        product_id = None

    try:
        product_name = product_info_div['data-prodname']
    except Exception as e:
        print(f"Error extracting product_name: {e}")
        product_name = None

    try:
        product_price = product_info_div['data-price']
    except Exception as e:
        print(f"Error extracting product_price: {e}")
        product_price = None

    try:
        product_link = product_info_div.select_one('div').select_one('a')['href']
        product_link = "https://www.shi.com" + product_link
    except Exception as e:
        print(f"Error extracting product_link: {e}")
        product_link = None

    try:
        raw_ul_div = product_info_div.select_one('div').select_one('ul')
        li_texts = [li.get_text(strip=True) for li in raw_ul_div.select('li')]
        product_short_description = ', '.join(li_texts)
    except Exception as e:
        #print(f"Error extracting product short description: {e}")
        product_short_description = ''

    try:
        raw_partnum = product_div.select_one('div[class*="partNumWrapper"]')
    except Exception as e:
        print(f"Error extracting raw_partnum: {e}")
        raw_partnum = None

    try:
        raw_mfr_part = raw_partnum.select_one('small[class*="srh_pr.mfrn"]')
        for tag in raw_mfr_part.find_all('strong'):
            tag.decompose()
        product_mfr_part = raw_mfr_part.get_text(strip=True)
    except Exception as e:
        print(f"Error extracting product_mfr_part: {e}")
        product_mfr_part = None

    try:
        raw_shi_part = raw_partnum.select_one('small[class*="srh_pr.shin"]')
        for tag in raw_shi_part.find_all('strong'):
            tag.decompose()
        product_shi_part = raw_shi_part.get_text(strip=True)
    except Exception as e:
        print(f"Error extracting product_shi_part: {e}")
        product_shi_part = None

    result.update({
        "Product ID": product_id,
        "Product Name": product_name,
        "Product Price": product_price,
        "Product Link": product_link,
        "Product Short Description": product_short_description,
        "Product Manufacturer Part": product_mfr_part,
        "Product SHI Part": product_shi_part
    })

    return result


def scrape_app_overview_from_categories(row):
    link = row['Last Category Link']
    print(f"Starting to Scrape Category: {row['Last Category Name']}")
    with SB(uc=True, 
            headless=False, 
            maximize=True,
            proxy=proxy_string
            ) as sb:
        try:
            success = sb_uc_open_with_retry(sb, link, max_attempts=3, sleep_time=4)
            sb.sleep(3)
            if not success:
                print(f"Failed to load {link} after 3 attempts. Skipping this category.")
                product_overview_result = []
            else:
                html = sb.get_page_source()
                soup = BeautifulSoup(html, 'html.parser')

                try:
                    raw_pagination = soup.select('div[class*="searchPages"]')[-1]
                    last_page_raw = raw_pagination.find_all("a")[-1]
                    n_pages = int(last_page_raw.get_text(strip=True))
                except Exception as e:
                    print(f"{row['Last Category Name']} only have 1 page")
                    n_pages = 1

                all_products_raw = soup.select('div[id="srResultsDiv"] div.row.srProduct')
                product_overview_result = [get_product_overview(product_div, row) for product_div in all_products_raw]

                for i in range(2, n_pages + 1):
                    print(f"{row['Last Category Name']} - Processing page {i} of {n_pages}")
                    n_start = 20 * (i - 1)
                    page_url = link + f"?p={20 * (i - 1)}, 20"
                    success = sb_uc_open_with_retry(sb, page_url, max_attempts=3, sleep_time=4)
                    sb.sleep(3)
                    if not success:
                        print(f"Failed to load {page_url} after 3 attempts. Skipping this page.")
                        continue
                    html = sb.get_page_source()
                    soup = BeautifulSoup(html, 'html.parser')
                    current_page_products_raw = soup.select('div[id="srResultsDiv"] div.row.srProduct')
                    current_page_products_result = [get_product_overview(product_div, row) for product_div in current_page_products_raw]
                    product_overview_result.extend(current_page_products_result)
        except Exception as e:
            print(f"Exception occurred while loading {link}: {e}")
            product_overview_result = []
    
    return product_overview_result


if __name__ == "__main__":
    url = "https://www.shi.com/shop/search/software"
    print("Getting All Categories")

    with SB(uc=True, 
            #headless=False, 
            #xvfb=True,
            #maximize=True,
            test=True,
            proxy=proxy_string
            ) as sb:

        sb.uc_open_with_reconnect(url, 5)
        #sb.sleep(4)

        #sb.activate_cdp_mode(url)
        sb.sleep(4)
        sb.uc_gui_click_captcha()
        sb.sleep(10)
        sb.uc_gui_handle_captcha()

        html = sb.get_page_source()

        soup = BeautifulSoup(html, 'html.parser')
        print(soup)

        cat_list = soup.select_one('div[class*="categoryList"]').select_one('ol').select_one("li")
        cat1_link = cat_list.select_one('a')['href']
        cat1_link = "https://www.shi.com" + cat1_link
        cat1_name = cat_list.select_one('a').get_text(strip=True)

        cat2_raw_list = cat_list.select_one('ol[id*="ctgy1software"]').select("li[class*='srCat']")

        cat2_results = []
        for cat2_list in cat2_raw_list:
            cat_2_a_tag = cat2_list.select_one('a')
            cat2_name = cat_2_a_tag.find(string=True, recursive=False)
            cat2_link = cat_2_a_tag['href']
            cat2_link = "https://www.shi.com" + cat2_link

            cur_result = {
                'Category 1 Name': cat1_name,
                'Category 1 Link': cat1_link,
                'Category 2 Name': cat2_name,
                'Category 2 Link': cat2_link
            }
            cat2_results.append(cur_result)

        cat2_df = pd.DataFrame(cat2_results)

        cat3_result = []
        for i, row in cat2_df.iterrows():
            print(f"Processing row {i+1} of {len(cat2_df)}")
            print(row['Category 2 Name'])
            print(row['Category 2 Link'])
            print()
            print(f"Getting Category 3 for {row['Category 2 Name']}")
            link = row['Category 2 Link']

            sb.uc_open(link)
            sb.sleep(3)

            html = sb.get_page_source()

            soup = BeautifulSoup(html, 'html.parser')
            cat_list = soup.select_one('div[class*="categoryList"]').select_one('ol').select_one("li")

            try:
                cat3_raw = cat_list.select_one('ol[id*="ctgy1software"]').select_one('li[class*="srCat"]').select_one('ol[id*="ctgy1software"]').select('li[class*="srCat"]')
                print(f"{row['Category 2 Name']} have Category 3")

                for cat3_list in cat3_raw:
                    cat_3_a_tag = cat3_list.select_one('a')
                    cat3_name = cat_3_a_tag.find(string=True, recursive=False)
                    cat3_link = cat_3_a_tag['href']
                    cat3_link = "https://www.shi.com" + cat3_link

                    cur_result = {
                        'Category 1 Name': row['Category 1 Name'],
                        'Category 1 Link': row['Category 1 Link'],
                        'Category 2 Name': row['Category 2 Name'],
                        'Category 2 Link': row['Category 2 Link'],
                        'Category 3 Name': cat3_name,
                        'Category 3 Link': cat3_link
                    }
                    cat3_result.append(cur_result)

            except Exception as e:
                print(f"{row['Category 2 Name']} does not have Category 3")
                cat3_result.append({
                    'Category 1 Name': row['Category 1 Name'],
                    'Category 1 Link': row['Category 1 Link'],
                    'Category 2 Name': row['Category 2 Name'],
                    'Category 2 Link': row['Category 2 Link'],
                    'Category 3 Name': None,
                    'Category 3 Link': None
                })

    cat3_df = pd.DataFrame(cat3_result)
    cat3_df['Last Category Name'] = cat3_df[['Category 3 Name', 'Category 2 Name', 'Category 1 Name']].bfill(axis=1).iloc[:, 0]
    cat3_df['Last Category Link'] = cat3_df[['Category 3 Link', 'Category 2 Link', 'Category 1 Link']].bfill(axis=1).iloc[:, 0]

    #cat3_df = cat3_df.iloc[8:10]

    list_of_rows = []
    for _, row in cat3_df.iterrows():
        clean_row = {
            'Category 1 Name': str(row['Category 1 Name']) if row['Category 1 Name'] is not None else None,
            'Category 1 Link': str(row['Category 1 Link']) if row['Category 1 Link'] is not None else None,
            'Category 2 Name': str(row['Category 2 Name']) if row['Category 2 Name'] is not None else None,
            'Category 2 Link': str(row['Category 2 Link']) if row['Category 2 Link'] is not None else None,
            'Category 3 Name': str(row['Category 3 Name']) if row['Category 3 Name'] is not None else None,
            'Category 3 Link': str(row['Category 3 Link']) if row['Category 3 Link'] is not None else None,
            'Last Category Name': str(row['Last Category Name']) if row['Last Category Name'] is not None else None,
            'Last Category Link': str(row['Last Category Link']) if row['Last Category Link'] is not None else None,
        }
        list_of_rows.append(clean_row)
        
    num_processes = 4
    all_results = []

    print("Starting to Scrape All Categories Overview..")

    with multiprocessing.Pool(processes=num_processes) as pool:
        # Pass the list of dictionaries to the pool
        # The scrape_app_overview_from_categories function will receive a dictionary
        split_results = pool.map(scrape_app_overview_from_categories, list_of_rows)

        all_results = [item for sublist in split_results for item in sublist]
    
    all_overview_result_df = pd.DataFrame(all_results)

    all_overview_result_df.to_csv(os.path.join(result_dir, "SHI All Product Overview.csv"), index=False)
    # breakpoint()