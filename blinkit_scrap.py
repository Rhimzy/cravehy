import requests
from bs4 import BeautifulSoup
import json
import logging
import asyncio
from playwright.async_api import async_playwright
import re
import time
import random

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Function to parse category HTML (finding links by href prefix) ---
def parse_categories_html_v2(html_content):
    """
    Parses the HTML content of the categories page to extract subcategory URLs
    by looking for links with href starting with '/cn/'.
    Returns a flat list of subcategories (name and URL).
    """
    subcategories_list = []
    base_url = "https://blinkit.com"

    logging.info("Starting HTML parsing for categories (v2).")
    try:
        soup = BeautifulSoup(html_content, 'lxml')

        # Find all <a> tags whose 'href' attribute starts with '/cn/'
        subcategory_links = soup.select('a[href^="/cn/"]')

        if not subcategory_links:
            logging.warning("No subcategory links with href starting with '/cn/' found in HTML.")
            return []

        logging.info(f"Found {len(subcategory_links)} potential subcategory links.")

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

        logging.info("Finished HTML parsing for categories (v2).")
    except Exception as e:
        logging.error(f"An unexpected error occurred during HTML parsing (v2): {e}")
        return []

    # Remove duplicates based on URL
    unique_subcategories = list({subcat['url']: subcat for subcat in subcategories_list}.values())
    logging.info(f"Reduced to {len(unique_subcategories)} unique subcategory URLs.")
    return unique_subcategories


# --- Function to fetch category HTML using Playwright (with automated location setting) ---
async def get_categories_with_playwright_v4(url="https://blinkit.com/categories", location_query="Mumbai"):
    """
    Fetches the Blinkit categories page using Playwright, automates setting location.
    Set headless=False for visual debugging.
    """
    logging.info(f"Launching browser to fetch: {url}")
    html_content = None
    async with async_playwright() as p:
        browser = None
        try:
            # Launch Chromium in NON-headless mode for debugging
            browser = await p.chromium.launch(headless=False) # <--- Set to False
            page = await browser.new_page()

            logging.info(f"Navigating to {url}...")
            # Wait for the initial page to load
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)

            logging.info("Page loaded. Attempting to set location...")

            # --- Automated Location Setting Logic ---
            try:
                # 1. Click on the element that opens the location picker in the header
                # Selector based on previous observation and HTML
                location_picker_selector = 'div.LocationBar__Container-sc-x8ezho-6'
                logging.info(f"Waiting for location picker trigger: {location_picker_selector}")
                await page.wait_for_selector(location_picker_selector, timeout=15000)
                logging.info("Location picker trigger found. Clicking...")
                await page.click(location_picker_selector)
                logging.info("Clicked location picker trigger.")

                # 2. Wait for the location search input field to appear in the dialog
                # Selector based on provided dialog HTML
                location_input_selector = 'input[name="select-locality"][placeholder*="search delivery location"]'
                logging.info(f"Waiting for location input field: {location_input_selector}")
                await page.wait_for_selector(location_input_selector, timeout=15000)
                logging.info("Location input field found.")

                # 3. Type the desired location
                logging.info(f"Typing location query: {location_query}")
                await page.fill(location_input_selector, location_query)
                logging.info("Typed location query.")

                # 4. Wait for location suggestions to appear and click the first one
                # Selector based on provided dialog HTML
                suggestion_selector = 'div.LocationSearchList__LocationListContainer-sc-93rfr7-0'
                logging.info(f"Waiting for location suggestions to appear (e.g., '{suggestion_selector}')...")
                await page.wait_for_selector(suggestion_selector, timeout=15000) # Wait for the first suggestion item
                logging.info("Suggestions appeared. Clicking the first one.")
                await page.click(suggestion_selector) # Click the first suggestion item
                logging.info("Clicked the first suggestion.")

                # 5. Wait for the page to update/reload after setting location
                # Wait specifically for the category links to appear, as this indicates success
                category_links_selector = 'a[href^="/cn/"]'
                logging.info(f"Waiting for category links to appear ({category_links_selector})...")
                await page.wait_for_selector(category_links_selector, timeout=30000) # Wait up to 30 seconds for categories to load
                logging.info("Category links appear to be loaded after setting location.")


            except Exception as e:
                logging.error(f"An error occurred during automated location setting: {e}")
                logging.warning("Proceeding to get HTML content anyway, but category loading might have failed.")
            # --- End Automated Location Setting Logic ---


            # Get the full HTML content after the page is rendered and location is hopefully set
            html_content = await page.content()
            logging.info(f"Fetched {len(html_content)} bytes of rendered HTML.")

        except Exception as e:
            logging.error(f"An error occurred during Playwright execution: {e}")
        finally:
            if browser:
                # Keep the browser open briefly for inspection if needed
                logging.info("Keeping browser open for 10 seconds for inspection...")
                await asyncio.sleep(10)
                await browser.close()
                logging.info("Browser closed.")

    return html_content

# --- Function to scrape product links from a category URL using the API (no changes) ---
# (Keep the scrape_products_from_category_api function as is)
def scrape_products_from_category_api(category_url):
    """
    Scrapes product data from a single category URL using the internal API.
    """
    current_api_url = None

    match = re.search(r'/cid/(\d+)/(\d+)', category_url)
    if not match:
        logging.error(f"Could not extract category IDs from URL: {category_url}")
        return set()

    l0_cat = match.group(1)
    l1_cat = match.group(2)
    initial_offset = 0
    limit = 20

    base_api_endpoint = "https://blinkit.com/v1/layout/listing_widgets"
    current_api_url = f"{base_api_endpoint}?offset={initial_offset}&limit={limit}&l0_cat={l0_cat}&l1_cat={l1_cat}&exclude_combos=false&oos_visibility=true"

    logging.info(f"Starting API scraping for category: {category_url}")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': category_url,
        'DNT': '1',
        'Connection': 'keep-alive',
         'X-Requested-With': 'XMLHttpRequest',
    }

    product_ids_in_category = set()

    while current_api_url:
        try:
            response = requests.get(current_api_url, headers=headers, timeout=20)
            response.raise_for_status()

            data = response.json()

            snippets = data.get('response', {}).get('snippets', [])
            if not snippets:
                 logging.info("No snippets found in API response, likely end of category or error.")
                 break

            for snippet in snippets:
                if snippet.get('widget_type') == 'product_card_snippet_type_2':
                    product_data = snippet.get('data', {})
                    product_id = product_data.get('identity', {}).get('id') or product_data.get('product_id')

                    if product_id:
                         product_ids_in_category.add(product_id)

            next_url_path = data.get('response', {}).get('pagination', {}).get('next_url')

            if next_url_path:
                current_api_url = f"https://blinkit.com{next_url_path}"
                time.sleep(random.uniform(1, 3))
            else:
                current_api_url = None

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching API page {current_api_url}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                 logging.error(f"HTTP Status Code: {e.response.status_code}")
            current_api_url = None
        except Exception as e:
            logging.error(f"An unexpected error occurred during API processing for {current_api_url}: {e}")
            current_api_url = None


    logging.info(f"Finished API scraping for category: {category_url}. Found {len(product_ids_in_category)} unique product IDs in this category.")
    return product_ids_in_category


# --- Main execution block ---
async def main():
    categories_url = "https://blinkit.com/categories"
    # Step 1: Get HTML for categories page using Playwright, including location setting attempt
    html_content = await get_categories_with_playwright_v4(categories_url, location_query="Mumbai") # Use v4, specify location

    if not html_content:
        logging.error("Could not get HTML content for categories page after attempting location setting. Exiting.")
        return

    # Step 1 (cont.): Parse HTML to get subcategory URLs
    subcategory_urls = parse_categories_html_v2(html_content)

    if not subcategory_urls:
        logging.error("No subcategory URLs found on the categories page using v2 parser after location attempt. Exiting.")
        # Save the HTML content if we couldn't parse links, might help diagnose
        with open("failed_categories_page_html.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        logging.info("Saved fetched HTML to failed_categories_page_html.html for inspection.")
        return

    logging.info(f"Successfully extracted {len(subcategory_urls)} unique subcategory URLs.")
    # logging.info("Subcategory URLs: " + json.dumps(subcategory_urls, indent=4)) # Uncomment to see list of URLs

    all_unique_product_ids = set()

    # Step 2: Scrape product IDs from each category's API (Still using requests, expected to fail)
    # We will replace this with Playwright scraping in the next step
    logging.info("Starting to scrape product IDs from category APIs (EXPECTING 403 errors).")
    logging.warning("NOTE: The API scraping step is expected to fail with 403. We will replace this with Playwright scraping in the next iteration.")
    for i, subcategory in enumerate(subcategory_urls):
        # Limit the number of categories for faster testing if needed
        # if i >= 5: # Uncomment to test with only the first 5 categories
        #     logging.info("Limiting to first 5 categories for testing.")
        #     break

        logging.info(f"Scraping category {i+1}/{len(subcategory_urls)}: {subcategory['name']} ({subcategory['url']})")
        # Call the requests-based API scraper - this is expected to fail
        product_ids_in_current_category = scrape_products_from_category_api(subcategory['url'])
        all_unique_product_ids.update(product_ids_in_current_category)
        logging.info(f"Total unique product IDs found so far: {len(all_unique_product_ids)}")
        # Add a delay even though API calls fail quickly, useful for real scraping
        time.sleep(random.uniform(2, 5))

    logging.info(f"\nFinished attempting to scrape product IDs from all categories via API. Total unique product IDs found: {len(all_unique_product_ids)}")
    if len(all_unique_product_ids) == 0:
        logging.warning("No product IDs were found during the API scraping phase, as expected due to 403 errors.")


    # Step 3: (Next step - Fetch detailed data for each unique product ID)
    # We will add the code for fetching PDP data in the next iteration.
    # For now, we don't have product IDs to save from the failing API step.
    # We need to replace the API scraping with Playwright scraping of PLP pages.

    # --- The next step is to replace the scrape_products_from_category_api function
    # --- with a Playwright function that visits the category URL, scrolls, and extracts
    # --- the PDP links from the rendered HTML.
    # --- Then, we'll use those PDP links to scrape the detailed data.

    logging.info("\nNext step: Implement Playwright scraping for product links on PLP pages.")


if __name__ == "__main__":
    asyncio.run(main())