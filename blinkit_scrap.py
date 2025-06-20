import requests
from bs4 import BeautifulSoup
import json
import logging
import asyncio
from playwright.async_api import async_playwright
import re # Import regex to extract IDs from URL
import time # Import time for delays
import random 

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Function to parse category HTML (slightly simplified to just get links) ---
def parse_categories_html(html_content):
    """
    Parses the HTML content of the categories page to extract subcategory URLs.
    Returns a flat list of subcategories (name and URL).
    """
    subcategories_list = []
    base_url = "https://blinkit.com"

    logging.info("Starting HTML parsing for categories.")
    try:
        soup = BeautifulSoup(html_content, 'lxml')

        # Find all subcategory links (a tags) with the specific class pattern
        subcategory_links = soup.find_all('a', class_=lambda value: value and 'Category__PageLink' in value)

        if not subcategory_links:
            logging.warning("No subcategory links with 'Category__PageLink' class found in HTML.")
            return [] # Return empty if no links are found

        logging.info(f"Found {len(subcategory_links)} subcategory links.")

        for link in subcategory_links:
            subcategory_name = link.text.strip()
            relative_url = link.get('href')
            if relative_url:
                 # Ensure the URL is complete
                subcategory_url = base_url + relative_url if relative_url.startswith('/') else relative_url
                subcategories_list.append({
                    'name': subcategory_name,
                    'url': subcategory_url
                })
            else:
                logging.warning(f"Subcategory link for '{subcategory_name}' has no href attribute.")

        logging.info("Finished HTML parsing for categories.")
    except Exception as e:
        logging.error(f"An unexpected error occurred during HTML parsing: {e}")
        return [] # Return empty list on parsing error

    return subcategories_list

# --- Function to fetch category HTML using Playwright ---
async def get_categories_with_playwright(url="https://blinkit.com/categories"):
    """
    Fetches the Blinkit categories page using Playwright to handle dynamic content.
    Set headless=False for visual debugging if needed.
    """
    logging.info(f"Launching browser to fetch: {url}")
    html_content = None
    async with async_playwright() as p:
        browser = None
        try:
            # Launch Chromium in headless mode (default)
            # Set headless=False for visual debugging if the script hangs here
            browser = await p.chromium.launch(headless=True) # Changed back to headless=True for efficiency
            page = await browser.new_page()

            # Set a realistic User-Agent (Playwright does this by default)
            # await page.set_user_agent('...') # Can set explicitly if needed

            logging.info(f"Navigating to {url}...")
            # Wait for the DOM to be fully loaded
            await page.goto(url, wait_until='domcontentloaded', timeout=60000) # Increased timeout

            # After DOM is loaded, wait briefly for potential JS rendering, but don't rely on specific elements
            # A short fixed delay or waiting for a more general indicator might be needed if content isn't immediately in DOMContentLoaded
            # Let's try waiting for a common element that should be present if categories loaded
            try:
                await page.wait_for_selector('a[href*="/cn/"]', timeout=10000) # Wait for any link containing /cn/
                logging.info("Found potential category links.")
            except Exception:
                logging.warning("Did not find expected category links within 10 seconds after DOMContentLoaded. Proceeding anyway.")


            # Get the full HTML content after the page is rendered
            html_content = await page.content()
            logging.info(f"Fetched {len(html_content)} bytes of rendered HTML.")

        except Exception as e:
            logging.error(f"An error occurred during Playwright execution: {e}")
        finally:
            if browser:
                await browser.close()
                logging.info("Browser closed.")

    return html_content

# --- Function to scrape product links from a category URL using the API ---
def scrape_products_from_category_api(category_url):
    """
    Scrapes product data from a single category URL using the internal API.

    Args:
        category_url (str): The URL of the category listing page (PLP).

    Returns:
        list: A list of dictionaries, where each dictionary contains basic
              product info and IDs ({'product_id': ..., 'merchant_id': ..., 'name': ..., ...}).
              Returns an empty list if fetching or parsing fails.
    """
    all_products_in_category = []
    current_api_url = None

    # Extract l0_cat and l1_cat from the category URL using regex
    # Example URL: https://blinkit.com/cn/chips-crisps/cid/1237/940
    match = re.search(r'/cid/(\d+)/(\d+)', category_url)
    if not match:
        logging.error(f"Could not extract category IDs from URL: {category_url}")
        return []

    l0_cat = match.group(1)
    l1_cat = match.group(2)
    initial_offset = 0
    limit = 20 # Can adjust limit, 15-20 seems common

    # Construct the initial API URL
    # Based on your network log: /v1/layout/listing_widgets?offset=...&limit=...&exclude_combos=false&l0_cat=...&l1_cat=...&oos_visibility=true&page_index=...&total_entities_processed=...&total_pagination_items=...
    # We'll start with basic params and let next_url provide the rest
    base_api_endpoint = "https://blinkit.com/v1/layout/listing_widgets"
    current_api_url = f"{base_api_endpoint}?offset={initial_offset}&limit={limit}&l0_cat={l0_cat}&l1_cat={l1_cat}&exclude_combos=false&oos_visibility=true"
    # Note: page_index, total_entities_processed, total_pagination_items might be needed,
    # but often the next_url handles this. Let's try minimal first.

    logging.info(f"Starting API scraping for category: {category_url}")
    logging.info(f"Initial API URL: {current_api_url}")

    # Use the same browser-like headers for API calls
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*', # Expect JSON response
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': category_url, # Referer should be the PLP URL
        'DNT': '1',
        'Connection': 'keep-alive',
         'X-Requested-With': 'XMLHttpRequest', # Often sent by JS fetch/XHR
    }

    product_ids_in_category = set() # Use a set to store unique product IDs

    while current_api_url:
        logging.info(f"Fetching API page: {current_api_url}")
        try:
            response = requests.get(current_api_url, headers=headers, timeout=20) # Increased timeout for API
            response.raise_for_status() # Raise an exception for bad status codes

            data = response.json()
            # logging.info(f"Received API response with {len(data.get('response', {}).get('snippets', []))} snippets.") # Verbose

            # Process snippets
            snippets = data.get('response', {}).get('snippets', [])
            if not snippets:
                 logging.info("No snippets found in API response, likely end of category or error.")
                 break # No more products or an issue

            for snippet in snippets:
                # We are looking for product snippets, identified by widget_type
                if snippet.get('widget_type') == 'product_card_snippet_type_2':
                    product_data = snippet.get('data', {})
                    product_id = product_data.get('identity', {}).get('id') or product_data.get('product_id')
                    merchant_id = product_data.get('meta', {}).get('merchant_id')

                    if product_id and merchant_id:
                         # Store the unique product ID (or a tuple of ID and merchant ID)
                         if product_id not in product_ids_in_category:
                             product_ids_in_category.add(product_id)
                             # Optionally, store some basic info here too if needed before PDP fetch
                             # product_info = {
                             #    'product_id': product_id,
                             #    'merchant_id': merchant_id,
                             #    'name': product_data.get('name', {}).get('text'),
                             #    'brand': product_data.get('brand_name', {}).get('text'),
                             #    'unit': product_data.get('variant', {}).get('text'),
                             #    'price': product_data.get('normal_price', {}).get('text'),
                             #    'mrp': product_data.get('mrp', {}).get('text'),
                             #    'image_url': product_data.get('image', {}).get('url'),
                             # }
                             # all_products_in_category.append(product_info)

                    else:
                        logging.warning(f"Could not extract product_id or merchant_id from snippet: {snippet.get('identity')}")


            # Get the URL for the next page
            next_url_path = data.get('response', {}).get('pagination', {}).get('next_url')

            if next_url_path:
                # The next_url is a path, not a full URL
                current_api_url = f"https://blinkit.com{next_url_path}"
                logging.info(f"Found next page URL: {current_api_url}")
                # Add a small delay between requests to be polite and avoid triggering blocks
                time.sleep(random.uniform(1, 3)) # Random delay between 1 and 3 seconds
            else:
                logging.info("No next_url found. End of category.")
                current_api_url = None # Stop the loop

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching API page {current_api_url}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                 logging.error(f"HTTP Status Code: {e.response.status_code}")
                 # Decide if you want to stop or retry on certain status codes
            current_api_url = None # Stop on error for now
        except Exception as e:
            logging.error(f"An unexpected error occurred during API processing for {current_api_url}: {e}")
            current_api_url = None # Stop on error for now


    logging.info(f"Finished API scraping for category: {category_url}. Found {len(product_ids_in_category)} unique product IDs.")
    return list(product_ids_in_category) # Return the list of unique product IDs

# --- Main execution block ---
async def main():
    categories_url = "https://blinkit.com/categories"
    # Step 1: Get HTML for categories page using Playwright
    html_content = await get_categories_with_playwright(categories_url)

    if not html_content:
        logging.error("Could not get HTML content for categories page. Exiting.")
        return # Exit if getting categories HTML failed

    # Step 1 (cont.): Parse HTML to get subcategory URLs
    subcategory_urls = parse_categories_html(html_content)

    if not subcategory_urls:
        logging.error("No subcategory URLs found on the categories page. Exiting.")
        return # Exit if no subcategory URLs were found

    logging.info(f"Successfully extracted {len(subcategory_urls)} subcategory URLs.")
    # logging.info("Subcategory URLs: " + json.dumps(subcategory_urls, indent=4)) # Uncomment to see list of URLs

    all_unique_product_ids = set()

    # Step 2: Scrape product IDs from each category's API
    logging.info("Starting to scrape product IDs from category APIs...")
    for i, subcategory in enumerate(subcategory_urls):
        logging.info(f"Scraping category {i+1}/{len(subcategory_urls)}: {subcategory['name']} ({subcategory['url']})")
        product_ids_in_current_category = scrape_products_from_category_api(subcategory['url'])
        all_unique_product_ids.update(product_ids_in_current_category) # Add unique IDs to the master set
        logging.info(f"Total unique product IDs found so far: {len(all_unique_product_ids)}")
        time.sleep(random.uniform(2, 5)) # Add a longer delay between categories

    logging.info(f"\nFinished scraping product IDs from all categories. Total unique product IDs: {len(all_unique_product_ids)}")

    # Step 3: (Next step - Fetch detailed data for each unique product ID)
    # We will add the code for fetching PDP data in the next iteration.
    # For now, let's just save the list of product IDs found.
    if all_unique_product_ids:
        output_filename = "blinkit_product_ids.json"
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(list(all_unique_product_ids), f, indent=4)
        logging.info(f"Saved {len(all_unique_product_ids)} unique product IDs to {output_filename}")
    else:
        logging.warning("No product IDs were found to save.")


if __name__ == "__main__":
    asyncio.run(main())