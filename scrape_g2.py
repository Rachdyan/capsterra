import os
from utils.g2_helper import *

# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
# Create result directory if it doesn't exist
result_dir = os.path.join(script_dir, "result")
os.makedirs(result_dir, exist_ok=True)

script_dir = os.path.dirname(os.path.abspath(__file__))
# Create result directory if it doesn't exist
result_dir = os.path.join(script_dir, "result")
os.makedirs(result_dir, exist_ok=True)

def scrape_row(row):
    return scrape_categories(row)

user = os.environ['PROXY_USER']
password = os.environ['PROXY_PASSWORD']
proxy_host = os.environ['PROXY_HOST']
proxy_port = os.environ['PROXY_PORT']

proxy_string = f"{user}:{password}@{proxy_host}:{proxy_port}"

if __name__ == "__main__":
    with SB(uc=True, headless=False, 
            xvfb=True, maximize=True,
            proxy=proxy_string
            ) as sb:
        print("Getting G2 Categories...")
        url = "https://www.g2.com/categories/"

        sb.activate_cdp_mode(url)
        sb.sleep(5)

        html = sb.get_page_source()
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.select("table")

        categories = []
        for table in tables:
            categories.extend(extract_categories(table))

    categories_df = pd.DataFrame(categories)
    print("Succesfully extracted len:", len(categories_df) )
    print(categories_df.head(5))

    print("Filtering categories..")

    parent_cat1 = categories_df.loc[categories_df['category_2'].notna(), 'category_1'].unique()
    parent_cat2 = categories_df.loc[categories_df['category_3'].notna(), 'category_2'].unique()
    parent_cat3 = categories_df.loc[categories_df['category_4'].notna(), 'category_3'].unique()

    mask1 = categories_df['category_1'].isin(parent_cat1) & categories_df['category_2'].isna()
    mask2 = categories_df['category_2'].isin(parent_cat2) & categories_df['category_3'].isna()
    mask3 = categories_df['category_3'].isin(parent_cat3) & categories_df['category_4'].isna()

    # Combine the individual masks into a single mask.
    combined_mask = mask1 | mask2 | mask3

    filtered_df = categories_df[~combined_mask].reset_index(drop=True)

    filtered_df['last_category_link'] = filtered_df[
        ['category_4_link', 'category_3_link', 'category_2_link', 'category_1_link']
    ].bfill(axis=1).iloc[:, 0]

    print("Succesfully filtered the df")
    #filtered_df
    
    rows = [row for _, row in filtered_df.iterrows()]
    with multiprocessing.Pool(processes=4) as pool:  # Adjust 'processes' as needed
        results = pool.map(scrape_row, rows)

    final_df = pd.concat(results, ignore_index=True)
    final_df.to_excel(os.path.join(result_dir, "G2 Result.xlsx"), index=False)

