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

proxies = { 
              "http"  : proxy_string, 
              "https" : proxy_string, 
            }

def scrape_tables(products_div, row, sb):
    results = []
    for product_div in products_div:
            header = product_div.select_one('div[data-testid *= "header"]')
            application_name = header.select_one('h2').get_text(strip=True)
            print(f"Getting Data for {application_name}")
            try:
                    last_header_button = header.select('span[role="button"]')[-1]
                    button_text = last_header_button.get_text(strip=True)
                    if 'visit' in button_text.lower():
                            evt_id = last_header_button.get('data-evt-id')
                            button_selector = f'span[role="button"][data-evt-id="{evt_id}"]'
                            sb.click(button_selector)
                            sb.switch_to_window(1)
                            sb.sleep(3)
                            sb.refresh_page()
                            sb.sleep(2)
                            website_url = sb.get_current_url()
                            print("Website URL:", website_url)
                            sb.driver.close()
                            sb.switch_to_window(0)
                    else:
                            website_url = ''
            except Exception as e:
                    # print(f"{application_name} does not contains External Link")
                    website_url = header.select_one('a')['href']
                    website_url = "https://www.getapp.com" + website_url

            description_raw = product_div.select_one("div[data-testid*='description']")
            description = description_raw.get_text(strip=True) if description_raw else ''

            results.append({
                    'Parent Category': row['Parent Category'],
                    'Category Name': row['Category Name'],
                    'Category Link': row['Web-Based Link'],
                    'Application Name': application_name,
                    'Website URL': website_url,
                    'Description': description
            })
    return results




def scrape_category(row, retries=3, delay=5):
    link = row['Web-Based Link']
    print(f"Starting to Scrape Category: {row['Category Name']}")

    for attempt in range(retries):
        try:
            with SB(uc=True, 
                    headless=True, 
                    maximize=True,
                    proxy=proxy_string) as sb:
                print(f"Attempt {attempt + 1}/{retries} for {row['Category Name']}")
                sb.uc_open(link)
                sb.sleep(3)

                html = sb.get_page_source()
                        # Parsing HTML menggunakan BeautifulSoup
                soup = BeautifulSoup(html, 'html.parser')

                pagination_raw = soup.select_one('div[class*="Pagination"]')
                pagination_text = pagination_raw.select_one('p').get_text(strip=True) if pagination_raw else ""
                match = re.findall(r'\d+', pagination_text)
                last_page = int(match[-1]) if match else 1

                all_product_divs = soup.select('div[data-evt-name*="product"]')
                result_list = scrape_tables(all_product_divs, row, sb)

                for i in range(2, last_page + 1):
                    url = link + f"?page={i}"
                    print(f"Scraping {url}")
                    # sb.cdp.open(url)
                    sb.uc_open(url)
                    sb.sleep(5)
                    html = sb.get_page_source()
                    soup = BeautifulSoup(html, 'html.parser')
                    all_product_divs = soup.select('div[data-evt-name*="product"]')
                    result_list.extend(scrape_tables(all_product_divs, row, sb))
                
                products_df = pd.DataFrame(result_list)
                print(f"Finished Scraping Category: {row['Category Name']}")
                print(f"Total {row['Category Name']} Products Found: {len(products_df)}")
                print(products_df.head(5))
                return products_df

        except (TimeoutException, ReadTimeoutError) as e:
            print(f"TimeoutException on attempt {attempt + 1} for {row['Category Name']}: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"All {retries} attempts failed for {row['Category Name']}. Moving to next category.")
                return pd.DataFrame()

        except Exception as e:
            print("An error occurred for ", row['Category Name'], ": ", e)
            return pd.DataFrame()
        
    return pd.DataFrame()


def clean_illegal_chars(df):
    # Remove illegal characters from all string columns
    illegal_pattern = re.compile(r'[\x00-\x1F\x7F-\x9F]')
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].astype(str).apply(lambda x: illegal_pattern.sub('', x))
    return df



if __name__ == "__main__":
    url = "https://www.getapp.com/browse/"

    with SB(uc=True, headless=True, 
            xvfb=True,
            maximize=True,
            proxy=proxy_string) as sb:
        sb.uc_open(url)
        sb.sleep(5)
        html = sb.get_page_source()
        soup = BeautifulSoup(html, 'html.parser')

    # response = curl_cffi.get(url, impersonate="chrome",
    #                          proxies=proxies)

    # print("Getting All Categories for GetApp..")
    # html = response.text
    # # Parsing HTML menggunakan BeautifulSoup
    # soup = BeautifulSoup(html, 'html.parser')
    # print(soup)

    categories_div = soup.select_one('div[class*="Categories"]').find_all('div', recursive=False)
    categories_div = categories_div[1:]

    results = []
    for category_div in categories_div:
        category_1 = category_div.select_one('div').get_text(strip=True)
        cat_name_raw = category_div.select('div')[1].find_all('a')
        cat_names = [cat.select_one('span').get_text(strip=True) for cat in cat_name_raw]
        cat_links = ["https://www.getapp.com" + cat['href'] for cat in cat_name_raw]
        web_based_links = [link + "os/web-based" for link in cat_links]
        
        for name, link, web_link in zip(cat_names, cat_links, web_based_links):
            results.append({
                "Parent Category": category_1,
                "Category Name": name,
                "Category Link": link,
                "Web-Based Link": web_link
            })

    all_categories_df = pd.DataFrame(results)
    print(f"Total Categories Found: {len(all_categories_df)}")

    # all_categories_df
    split_dfs = np.array_split(all_categories_df, 5)
    all_results = []

    for idx, split_df in enumerate(split_dfs):
        print(f"Processing split {idx + 1}/{len(split_dfs)}")

        scrape_with_retries = partial(scrape_category, retries=3, delay=10)
        num_processes = 4
        print(f"Starting pool with {num_processes} processes...")

        with multiprocessing.Pool(processes=num_processes) as pool:
            split_results = pool.map(scrape_with_retries, [row for _, row in split_df.iterrows()])
            all_results.extend(split_results)
        
        split_results = [df for df in split_results if not df.empty]
        if split_results:
             split_products_df = pd.concat(split_results, ignore_index=True)
             split_products_df = clean_illegal_chars(split_products_df)  # Clean before saving
             output_path = os.path.join(result_dir, f"GetApp Products Results Part {idx + 1}.xlsx")
             split_products_df.to_excel(output_path, index=False)
             print(f"Saved split {idx + 1} results to {output_path}")
        

    final_results_df = pd.concat([df for df in all_results if not df.empty], ignore_index=True)
    final_results_df = clean_illegal_chars(final_results_df)  # Clean before saving
    final_results_df.to_excel(os.path.join(result_dir, "GetApp All Products Results.xlsx"), index=False)
    print("Finished processing all splits.")
