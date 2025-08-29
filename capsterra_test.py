import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import time
import requests
import random
import json
import concurrent.futures
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from seleniumbase import SB
import asyncio
import multiprocessing
import numpy as np
import os

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
# Create result directory if it doesn't exist
result_dir = os.path.join(script_dir, "result")
os.makedirs(result_dir, exist_ok=True)

HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'priority': 'u=1, i',
    'referer': "https://www.capterra.com/categories/",
    'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
}

def get_with_retries(scraper, url, headers, retries=2, delay=5):
    """Attempts to fetch a URL with a specified number of retries upon failure."""
    for attempt in range(retries + 1):
        try:
            response = scraper.get(url=url, headers=headers)
            response.raise_for_status()  # Will raise an HTTPError for bad responses (4xx or 5xx)
            return response
        except (requests.exceptions.RequestException, cloudscraper.exceptions.CloudflareException) as e:
            print(f"Request to {url} failed on attempt {attempt + 1}: {e}")
            if attempt < retries:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"All retries failed for {url}.")
                raise  # Re-raise the last exception if all retries fail


def scrape_tables(products_div, row):
    result_list = []
    for container in products_div:
        product_link = container.select_one("a").attrs['href']
        product_link = "https://www.capterra.com" + product_link
        product_name = container.select_one("h2").get_text(strip=True)
        product_description_tag = container.select_one("p")
        product_description = product_description_tag.get_text(strip=True) if product_description_tag else ""
        product_description = product_description.split('Learn More')[0].strip()
        result_list.append({
            'Product Category': row['category_name'],
            'Product Category Link': row['cloud_link'],
            'Product Name': product_name,
            'Product Link': product_link,
            'Product Description': product_description
        })
    return result_list


def scrape_category(row):

    with SB(uc=True, headless=True, maximize=True) as sb:
        link = row['cloud_link']
        # sb.cdp.open(link)
        sb.uc_open(link)
        print(f"Scraping Category: {row['category_name']}")
        sb.sleep(5)
        html = sb.get_page_source()
        soup = BeautifulSoup(html, 'html.parser')
        try:
            page_raw = soup.select_one('div[data-test-id = "current-page-display"]').get_text(strip=True)
            match = re.search(r'of(\d+)', page_raw, re.IGNORECASE)
            last_page = int(match.group(1)) if match else 1
        except:
            last_page = 0

        # Scrape first page only once
        product_card_containers = soup.select("div[id*='product-card-container']")
        result_list = scrape_tables(product_card_containers, row)

        # Scrape remaining pages (start from 2)
        for i in range(2, last_page + 1):
            url = link + f"&page={i}"
            print(f"Scraping {url}")
            # sb.cdp.open(url)
            sb.uc_open(url)
            sb.sleep(5)
            html = sb.get_page_source()
            soup = BeautifulSoup(html, 'html.parser')
            product_card_containers = soup.select("div[id*='product-card-container']")
            result_list.extend(scrape_tables(product_card_containers, row))

        products_df = pd.DataFrame(result_list)
        expected_cols = ['Product Category', 'Product Category Link']
        actual_cols = [col for col in expected_cols if col in products_df.columns]
        other_cols = [col for col in products_df.columns if col not in actual_cols]
        products_df = products_df[actual_cols + other_cols]
        return products_df

user = os.environ['PROXY_USER']
password = os.environ['PROXY_PASSWORD']
proxy_host = os.environ['PROXY_HOST']
proxy_port = os.environ['PROXY_PORT']

proxy_string = f"{user}:{password}@{proxy_host}:{proxy_port}"

if __name__ == "__main__":
    with SB(uc=True, headless=True, xvfb=False, maximize=True,
            proxy=proxy_string) as sb:
        url = "https://www.capterra.com/categories/"
        sb.activate_cdp_mode(url)
        print("Getting All Categories...")
        sb.sleep(4)
        html = sb.get_page_source()
        soup = BeautifulSoup(html, 'html.parser')
        list_raw = soup.select_one("div[data-testid*='alphabetical-list']")
        list_all = list_raw.select("li[data-testid*='group-list-item']")
        categories_data = []
        for list_item in list_all:
            try:
                category_name = list_item.select_one("a").get_text(strip=True) 
            except:
                category_name = "" 
            try:
                category_link = list_item.select_one("a").get("href")
                category_link = "https://www.capterra.com" + category_link
                cloud_link = category_link + "?deployment=CLOUD_SAAS_WEB_BASED"
            except:
                category_link = ""
                cloud_link = ""
            categories_data.append({
                'category_name': category_name,
                'category_link': category_link,
                'cloud_link': cloud_link
            })
        all_categories_df = pd.DataFrame(categories_data)
        print(all_categories_df)

        all_categories_df = all_categories_df.iloc[:20]
        # Split DataFrame into 4 parts
        split_dfs = np.array_split(all_categories_df, 4)
        all_results = []

        # Example: process each part in a loop (can also use multiple pools if needed)
        for idx, part_df in enumerate(split_dfs):
            print(f"Processing part {idx+1} with {len(part_df)} categories")
            rows = [row.to_dict() for _, row in part_df.iterrows()]
            with multiprocessing.Pool(processes=4) as pool:  # adjust processes as needed
                results = pool.map(scrape_category, rows)
                all_results.extend(results)
            part_products_df = pd.concat(results, ignore_index=True)
            part_products_df.to_excel(os.path.join(result_dir, f"Capsterra Results Part{idx+1}.xlsx"), index=False)
        # breakpoint()
        all_products_df = pd.concat(all_results, ignore_index=True)
        all_products_df.to_excel(os.path.join(result_dir, "Capsterra Results.xlsx"), index=False)