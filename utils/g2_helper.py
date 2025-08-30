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
import nest_asyncio

HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'priority': 'u=1, i',
    'referer': "https://www.g2.com/categories",
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

def extract_categories(table):
    categories = []
    rows = table.select('tbody tr')
    # Get top-level category from thead
    category_1_tag = table.select_one('thead tr td.l3')
    category_1 = category_1_tag.get_text(strip=True) if category_1_tag else None

    # Build lookup for name -> (parent, link)
    name_to_parent = {}
    name_to_link = {}
    for row in rows:
        name_tag = row.select_one('.categories__name a')
        parent_div = row.select_one('.categories__parent')
        if name_tag:
            name = name_tag.get_text(strip=True)
            link = "https://www.g2.com" + name_tag.get('href')
            parent = parent_div.get_text(strip=True) if parent_div else None
            name_to_parent[name] = parent
            name_to_link[name] = link

    for row in rows:
        name_tag = row.select_one('.categories__name a')
        if name_tag:
            current = name_tag.get_text(strip=True)
            current_link = name_to_link.get(current)
            # Build the full chain from current up to top-level
            chain = []
            links = []
            node = current
            while node and node != category_1:
                chain.append(node)
                links.append(name_to_link.get(node))
                node = name_to_parent.get(node)
            # Add top-level
            chain.append(category_1)
            links.append(None)
            # Reverse so left is top-level, right is deepest
            chain = chain[::-1]
            links = links[::-1]
            # Pad to 4 levels
            while len(chain) < 5:
                chain.append(None)
                links.append(None)
            categories.append({
                'category_1': chain[0],
                'category_1_link': links[0],
                'category_2': chain[1],
                'category_2_link': links[1],
                'category_3': chain[2],
                'category_3_link': links[2],
                'category_4': chain[3],
                'category_4_link': links[3]
            })
    return categories

def get_product_table(product_div):
    product_name = product_div.select_one("div[class *= 'product-name']").get_text(strip=True)
    product_link = product_div.select_one("a[href]").get("href")

    product_description = product_div.select_one("p").get_text(strip=True)
    product_description = product_description.replace("...", "").replace("Show More", "")

    return {
        'Product Name': product_name,
        'Product Link': product_link,
        'Product Description': product_description,
    }


def scrape_categories(row):
    with SB(uc=True, headless=False, maximize=True) as sb:
        link = row['last_category_link']
        print("Scraping:", link)
        sb.uc_open(link)
        sb.sleep(5)
        html = sb.get_page_source()
        soup = BeautifulSoup(html, 'html.parser')

        raw_pagination = soup.select_one("ul[aria-label *= 'Pagination']")
        li_elements = raw_pagination.find_all('li') if raw_pagination else []
        last_li = li_elements[-1] if li_elements else None
        last_href = None
        page_number = 1
        try:
            last_href = last_li.find('a')['href'] if last_li and last_li.find('a') else None
            match = re.search(r'page=(\d+)', last_href)
            page_number = int(match.group(1)) if match else 1
        except Exception as e:
            print(f"Pagination extraction failed: {e}")
            last_href = link
            page_number = 1

        info = {'Category 1': row['category_1'],
                'Category 2': row['category_2'],
                'Category 3': row['category_3'],
                'Category 4': row['category_4'],
                'Last Category Link': last_href}

        raw_product_divs = soup.select("div[data-ordered-events-item*='product']")
        result = [get_product_table(div) for div in raw_product_divs]

        for i in range(2, page_number+1):
            paged_url = f"{link}?order=g2_score&page={i}"
            print("Scraping page:", paged_url)
            sb.uc_open(paged_url)
            sb.sleep(5)
            html = sb.get_page_source()
            soup = BeautifulSoup(html, 'html.parser')
            raw_product_divs = soup.select("div[data-ordered-events-item*='product']")
            result.extend([get_product_table(div) for div in raw_product_divs])

        combined = [{**info, **product} for product in result]
        result_df = pd.DataFrame(combined)
        return result_df