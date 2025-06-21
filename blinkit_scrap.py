import requests
from bs4 import BeautifulSoup
import json
import logging
import asyncio
from playwright.async_api import async_playwright
import re
import time
import random
import os

# --- Configuration ---
LOG_FILE = "scraper_log.log"
# Removed PRODUCT_IDS_FILE as PIDs will be stored categorically in JSON
FULL_PRODUCT_DATA_FILE = "blinkit_all_product_data.json"
CATEGORIZED_PIDS_FILE = "blinkit_categorized_pids.json" # New file for categorical PIDs
LOCATION_QUERY = "Mumbai"
MAX_PLP_SCROLL_ATTEMPTS = 60
PLP_CONCURRENCY = 2
PDP_CONCURRENCY = 10

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler()
    ]
)

# --- Helper functions (no changes to parse_categories_html_v2) ---
def parse_categories_html_v2(html_content):
    subcategories_list = []
    base_url = "https://blinkit.com"
    logging.info("Starting HTML parsing for categories (v2).")
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        subcategory_links = soup.select('a[href^="/cn/"]')
        if not subcategory_links:
            logging.warning("No subcategory links with href starting with '/cn/' found in HTML.")
            return []
        logging.info(f"Found {len(subcategory_links)} potential subcategory links.")
        for link in subcategory_links:
            subcategory_name = link.text.strip()
            relative_url = link.get('href')
            if relative_url:
                subcategory_url = base_url + relative_url if relative_url.startswith('/') else relative_url
                subcategories_list.append({'name': subcategory_name, 'url': subcategory_url})
            else:
                logging.warning(f"Subcategory link for '{subcategory_name}' has no href attribute.")
        logging.info("Finished HTML parsing for categories (v2).")
    except Exception as e:
        logging.error(f"An unexpected error occurred during HTML parsing (v2): {e}")
        return []
    unique_subcategories = list({subcat['url']: subcat for subcat in subcategories_list}.values())
    logging.info(f"Reduced to {len(unique_subcategories)} unique subcategory URLs.")
    return unique_subcategories


# --- Function to handle initial load and location setting on the homepage (v15) ---
async def handle_initial_load_and_location_v15(p_instance, location_query=LOCATION_QUERY):
    logging.info("Starting initial load and location handling.")
    context = None
    user_data_dir = "./tmp_user_data"
    os.makedirs(user_data_dir, exist_ok=True)

    try:
        context = await p_instance.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=False, # Keep False for initial runs to debug location
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        page = await context.new_page()

        homepage_url = "https://blinkit.com/"
        logging.info(f"Navigating to homepage: {homepage_url}")
        await page.goto(homepage_url, wait_until='domcontentloaded', timeout=90000)
        await page.wait_for_load_state('load', timeout=30000)
        await page.wait_for_load_state('networkidle', timeout=30000)
        logging.info("Homepage loaded and network idle achieved.")

        location_input_selector = 'input[name="select-locality"][placeholder*="search delivery location"]'
        location_picker_trigger_selector = 'div.LocationBar__Container-sc-x8ezho-6'

        if await page.is_visible(location_input_selector, timeout=5000):
            logging.info("Location input field is already visible. Assuming location modal is open.")
        elif await page.is_visible(location_picker_trigger_selector, timeout=5000):
            logging.info(f"Location input field not visible, but trigger '{location_picker_trigger_selector}' is. Clicking trigger.")
            try:
                await page.locator(location_picker_trigger_selector).click(timeout=10000)
                await page.locator(location_input_selector).wait_for(state='visible', timeout=15000)
                logging.info("Location modal opened by clicking trigger.")
            except Exception as e:
                logging.error(f"Failed to open location modal via trigger: {e}")
                logging.warning("Cannot set location. Subsequent scraping may fail.")
                await page.close()
                return None
        else:
            logging.info("Neither location input nor trigger found. Assuming location is already set or page state is unexpected.")
            if await page.is_visible(r'text=/Delivery in \d+ minutes/', timeout=10000):
                logging.info("Detected 'Delivery in X minutes' text in header, assuming location is set.")
            else:
                logging.warning("Could not confirm location set based on header text. Proceeding anyway, but expect issues.")

        if await page.is_visible(location_input_selector, timeout=1000):
            logging.info(f"Typing location query: {location_query}")
            await page.locator(location_input_selector).fill(location_query)
            logging.info("Typed location query.")

            suggestion_selector = 'div.LocationSearchList__LocationListContainer-sc-93rfr7-0'
            try:
                suggestion_item = page.locator(suggestion_selector).first
                await suggestion_item.wait_for(state='visible', timeout=15000)
                await suggestion_item.click(timeout=10000)
                logging.info("Clicked the first suggestion.")
            except Exception as e:
                logging.error(f"Failed to click location suggestion: {e}")
                logging.warning("Location selection failed. Subsequent scraping may fail.")
                await page.close()
                return None

            await page.wait_for_load_state('networkidle', timeout=45000)
            if not await page.is_visible(r'text=/Delivery in \d+ minutes/', timeout=10000):
                 logging.warning("Did not detect 'Delivery in X minutes' text after location selection. Location might not be fully set.")
            logging.info("Location setting process completed.")
        else:
            logging.info("Location input field not active. Skipping typing/clicking location.")

        await page.close()
        logging.info("Initial page closed. Browser context is ready for categories/PLPs.")
        return context

    except Exception as e:
        logging.error(f"An unhandled error occurred during initial load or location handling: {e}")
        if context:
            await context.close()
        logging.info("Browser context closed due to error.")
        return None


# --- Function to scrape product IDs from a category PLP using Playwright (v15) ---
async def scrape_product_ids_from_plp_v15(page, category_url):
    """
    Navigates to a category URL using an existing Playwright page,
    scrolls within the product list container to load all products,
    and extracts product IDs from the product card divs.
    Includes robust error handling for "Oops" messages and improved scrolling logic.
    """
    logging.info(f"Navigating to category PLP: {category_url}")
    product_ids = set()
    scroll_attempts = 0

    PRODUCT_LIST_CONTAINER_SELECTOR = '#plpContainer'
    PRODUCT_CARD_SELECTOR = f'{PRODUCT_LIST_CONTAINER_SELECTOR} div[data-pf="reset"][tabindex="0"][role="button"]'
    error_message_selector = r'text="Oops! Something went wrong. Please try again later."' # Raw string for regex
    
    try:
        await page.goto(category_url, wait_until='domcontentloaded', timeout=90000) 
        logging.info("Category PLP DOM loaded. Checking for errors and content...")

        if await page.is_visible(error_message_selector, timeout=5000):
            logging.error(f"PLP {category_url} showed an 'Oops' error message. Skipping this category.")
            await page.screenshot(path=f"error_plp_{category_url.replace('/', '_').replace(':', '_')}.png")
            return set()

        logging.info(f"Waiting for product list container: {PRODUCT_LIST_CONTAINER_SELECTOR} and products to appear.")
        try:
            await page.locator(PRODUCT_LIST_CONTAINER_SELECTOR).wait_for(state='visible', timeout=20000)
            logging.info("Product list container found.")

            await page.locator(PRODUCT_CARD_SELECTOR).first.wait_for(state='visible', timeout=15000)
            logging.info("Initial product cards appeared within container.")
        except Exception as e:
            logging.error(f"PLP {category_url} did not become ready (container/products not found) within timeout. Error: {e}")
            await page.screenshot(path=f"plp_not_ready_{category_url.replace('/', '_').replace(':', '_')}.png")
            return set()

        previous_scroll_height = -1
        while scroll_attempts < MAX_PLP_SCROLL_ATTEMPTS:
            if await page.is_visible(error_message_selector, timeout=1000):
                logging.error(f"PLP {category_url} showed an 'Oops' error message during scrolling. Stopping for this category.")
                await page.screenshot(path=f"error_scrolling_plp_{category_url.replace('/', '_').replace(':', '_')}.png")
                break

            current_scroll_height = await page.evaluate(f"document.querySelector('{PRODUCT_LIST_CONTAINER_SELECTOR}').scrollHeight")
            
            if current_scroll_height == previous_scroll_height and scroll_attempts > 0:
                logging.info(f"No new scroll height detected ({current_scroll_height}px). Assuming end of category.")
                break

            previous_scroll_height = current_scroll_height

            await page.evaluate(f"""() => {{
                const container = document.querySelector('{PRODUCT_LIST_CONTAINER_SELECTOR}');
                if (container) {{
                    container.scrollTo(0, container.scrollHeight);
                }}
            }}""")
            
            try:
                await page.wait_for_load_state('networkidle', timeout=10000)
            except Exception:
                logging.warning(f"Network idle timeout after scroll for {category_url}. Continuing but may indicate slow loading.")
            
            await asyncio.sleep(random.uniform(1, 3))

            current_product_count = len(await page.query_selector_all(PRODUCT_CARD_SELECTOR))
            logging.info(f"Scroll attempt {scroll_attempts + 1}: Current product count: {current_product_count}")

            scroll_attempts += 1


        logging.info(f"Finished scrolling after {scroll_attempts} attempts (max {MAX_PLP_SCROLL_ATTEMPTS}).")

        final_cards = await page.query_selector_all(PRODUCT_CARD_SELECTOR)
        logging.info(f"Total product cards found after scrolling: {len(final_cards)}")

        current_category_pids = set()
        for card in final_cards:
            product_id = await card.get_attribute('id')
            if product_id:
                current_category_pids.add(product_id)

        logging.info(f"Extracted {len(current_category_pids)} unique product IDs from {category_url}")
        return current_category_pids

    except Exception as e:
        logging.error(f"An error occurred while scraping PLP {category_url}: {e}")
        await page.screenshot(path=f"general_error_plp_{category_url.replace('/', '_').replace(':', '_')}.png")
        return set()


# --- Function to scrape detailed data from a PDP using requests and JSON (no change) ---
def scrape_detailed_product_data(product_id):
    product_data = []
    logging.info(f"Fetching detailed data for PDP ID: {product_id}")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://blinkit.com/',
        'DNT': '1',
        'Connection': 'keep-alive',
    }
    try:
        product_url = f"https://blinkit.com/prn/product/prid/{product_id}"
        response = requests.get(product_url, headers=headers, timeout=30)
        response.raise_for_status()
        if '/prid/' not in response.url:
             logging.warning(f"Redirected away from expected PDP URL pattern for ID {product_id}. Final URL: {response.url}")
             return []
        soup = BeautifulSoup(response.content, 'lxml')
        script_tag = soup.find('script', string=re.compile(r'window\.grofers\.PRELOADED_STATE = \{'))
        if not script_tag:
            logging.error(f"Could not find PRELOADED_STATE script tag on {product_url} (ID: {product_id})")
            return []
        json_string = script_tag.string
        json_string = json_string.replace('window.grofers.PRELOADED_STATE = ', '').strip()
        if json_string.endswith(';'):
            json_string = json_string[:-1]
        state_data = json.loads(json_string)
        variants_info = state_data.get('data', {}).get('ui', {}).get('pdp', {}).get('rawData', {}).get('data', {}).get('variants_info', [])
        if not variants_info:
             single_product_data = state_data.get('data', {}).get('ui', {}).get('pdp', {}).get('rawData', {}).get('data', {}).get('product')
             if single_product_data:
                  variants_info = [single_product_data]
                  logging.warning(f"variants_info not found for ID {product_id}, using single product data as fallback.")
             else:
                logging.error(f"No variant or single product data found in PRELOADED_STATE for ID {product_id}")
                return []
        for variant in variants_info:
            variant_product_id = variant.get('id') or variant.get('product_id') or product_id
            if not variant_product_id:
                 logging.warning(f"Skipping variant due to missing product_id for group ID {product_id}: {variant.get('name')}")
                 continue
            variant_data = {
                'product_id': variant_product_id,
                'group_id': variant.get('group_id'),
                'name': variant.get('name'),
                'brand': variant.get('brand'),
                'category_l0': variant.get('level0_category', [{}])[0].get('name') if variant.get('level0_category') else None,
                'category_l1': variant.get('level1_category', [{}])[0].get('name') if variant.get('level1_category') else None,
                'unit': variant.get('unit'),
                'price': variant.get('price'),
                'original_price': variant.get('mrp'),
                'inventory': variant.get('inventory'),
                'product_url': response.url,
                'image_urls': [item.get('image', {}).get('url') for item in variant.get('assets', []) if item and item.get('media_type') == 'image' and item.get('image', {}).get('url')],
                'nutrition_info': None,
                'ingredients': None,
                'key_features': None
            }
            attribute_collections = variant.get('attribute_collection', [])
            for collection in attribute_collections:
                attributes = collection.get('attributes', [])
                for attr in attributes:
                    if attr.get('title') == 'Nutrition Information' and attr.get('value'):
                         variant_data['nutrition_info'] = attr.get('value').strip()
                    elif attr.get('title') == 'Ingredients' and attr.get('value'):
                         variant_data['ingredients'] = attr.get('value').strip()
                    elif attr.get('title') == 'Key Features' and attr.get('value'):
                         variant_data['key_features'] = attr.get('value').strip()
            parsed_nutrition = {}
            nutrition_text = variant_data.get('nutrition_info')
            if nutrition_text:
                lines = nutrition_text.split('\n')
                if lines:
                    serving_size_line = lines[0]
                    serving_size_match = re.match(r'Per (.*)', serving_size_line)
                    if serving_size_match:
                        parsed_nutrition['serving_size'] = serving_size_match.group(1).strip()
                        lines = lines[1:]
                for line in lines:
                    if ':' in line:
                        key, value = line.split(':', 1)
                        parsed_nutrition[key.strip()] = value.strip()
            variant_data['nutrition_info'] = parsed_nutrition
            product_data.append(variant_data)
        logging.info(f"Successfully extracted data for {len(product_data)} variants from PDP ID {product_id}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching PDP URL {product_url} (ID: {product_id}): {e}")
        if hasattr(e, 'response') and e.response is not None:
             logging.error(f"HTTP Status Code: {e.response.status_code}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from PRELOADED_STATE on PDP URL {product_url} (ID: {product_id}): {e}")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred during PDP scraping for URL {product_url} (ID: {product_id}): {e}")
        return []
    return product_data


# --- Main execution block ---
async def main():
    # Clear incremental PID file at start of new full run (or comment out to resume)
    # This should be done only if you want a fresh run.
    # If you want to resume, comment out these lines.
    if os.path.exists(CATEGORIZED_PIDS_FILE):
        os.remove(CATEGORIZED_PIDS_FILE)
        logging.info(f"Cleared previous categorized PIDs file: {CATEGORIZED_PIDS_FILE}")
    # Also clear the old flat PIDs file if it exists, to avoid confusion
    # if os.path.exists(PRODUCT_IDS_FILE):
    #     os.remove(PRODUCT_IDS_FILE)
    #     logging.info(f"Cleared old flat PIDs file: {PRODUCT_IDS_FILE}")


    # Use async_playwright context manager at the top level for a single Playwright instance
    async with async_playwright() as p:
        # Step 0: Handle initial load and location setting, get a persistent browser context
        browser_context_for_all_scraping = await handle_initial_load_and_location_v15(p, location_query=LOCATION_QUERY)

        if not browser_context_for_all_scraping:
            logging.error("Failed to get a persistent browser context with location set. Exiting.")
            return

        # Now, use this single persistent context for all subsequent Playwright operations
        # (categories page and PLP scraping)

        # Step 1: Get HTML for categories page (now that location is set)
        categories_url = "https://blinkit.com/categories"
        logging.info(f"Navigating to categories page: {categories_url}")
        page_for_categories = await browser_context_for_all_scraping.new_page()
        try:
            await page_for_categories.goto(categories_url, wait_until='domcontentloaded', timeout=90000)
            await page_for_categories.wait_for_load_state('load', timeout=30000)
            await page_for_categories.wait_for_load_state('networkidle', timeout=30000)
            html_content = await page_for_categories.content()
            logging.info(f"Fetched {len(html_content)} bytes of rendered HTML for categories page.")
        except Exception as e:
            logging.error(f"Error navigating to or fetching categories page: {e}")
            html_content = None
        finally:
            await page_for_categories.close()

        if not html_content:
            logging.error("Could not get HTML content for categories page. Exiting.")
            await browser_context_for_all_scraping.close()
            return

        subcategory_urls = parse_categories_html_v2(html_content)

        if not subcategory_urls:
            logging.error("No subcategory URLs found on the categories page. Exiting.")
            with open("failed_categories_page_html.html", "w", encoding="utf-8") as f:
                f.write(html_content)
            logging.info("Saved fetched HTML to failed_categories_page_html.html for inspection.")
            await browser_context_for_all_scraping.close()
            return

        logging.info(f"Successfully extracted {len(subcategory_urls)} unique subcategory URLs.")


        # New structure to store PIDs categorically
        all_categorized_product_ids = {} # { "category_url": ["pid1", "pid2"], ... }
        total_unique_pids_overall = set() # To keep track of overall unique PIDs

        # Load existing categorized PIDs if file exists (for resuming)
        if os.path.exists(CATEGORIZED_PIDS_FILE):
            try:
                with open(CATEGORIZED_PIDS_FILE, 'r', encoding='utf-8') as f:
                    loaded_data = json.load(f)
                    for cat_url, pids in loaded_data.items():
                        all_categorized_product_ids[cat_url] = set(pids) # Store as sets for efficient updates
                        total_unique_pids_overall.update(pids)
                logging.info(f"Loaded {len(all_categorized_product_ids)} categories from {CATEGORIZED_PIDS_FILE}. Total {len(total_unique_pids_overall)} unique PIDs.")
            except json.JSONDecodeError as e:
                logging.warning(f"Could not load {CATEGORIZED_PIDS_FILE} (JSON error: {e}). Starting fresh for PIDs.")
            except Exception as e:
                logging.warning(f"Could not load {CATEGORIZED_PIDS_FILE} (Error: {e}). Starting fresh for PIDs.")


        # Step 2: Scrape product IDs from each category PLP concurrently using Playwright
        logging.info(f"Starting to scrape product IDs from category PLPs using Playwright (Concurrency: {PLP_CONCURRENCY})...")
        
        semaphore_plp = asyncio.Semaphore(PLP_CONCURRENCY)

        async def scrape_plp_task_with_categorization(context_for_task, subcategory_data):
            category_url = subcategory_data['url']
            
            # Skip if this category was already processed and has PIDs
            if category_url in all_categorized_product_ids and all_categorized_product_ids[category_url]:
                logging.info(f"Skipping already scraped category: {subcategory_data['name']} ({category_url})")
                return {category_url: all_categorized_product_ids[category_url]} # Return existing PIDs for this category

            page = await context_for_task.new_page()
            pids_for_this_category = set()
            try:
                pids_for_this_category = await scrape_product_ids_from_plp_v15(page, category_url) 
                
                # Update main categorized storage and overall unique PIDs
                all_categorized_product_ids[category_url] = pids_for_this_category
                total_unique_pids_overall.update(pids_for_this_category)

                # Save categorized PIDs incrementally after each category is done
                # Convert sets to lists for JSON serialization
                temp_dict_for_save = {k: list(v) for k, v in all_categorized_product_ids.items()}
                with open(CATEGORIZED_PIDS_FILE, 'w', encoding='utf-8') as f: # Overwrite with updated data
                    json.dump(temp_dict_for_save, f, indent=4)
                logging.info(f"Saved incremental categorized PIDs to {CATEGORIZED_PIDS_FILE}")

            except Exception as e:
                logging.error(f"Error scraping PLP for {subcategory_data['name']} ({category_url}): {e}")
                try: # Attempt to save screenshot/HTML on error
                    await page.screenshot(path=f"task_error_plp_{category_url.replace('/', '_').replace(':', '_')}.png")
                    with open(f"task_error_plp_{category_url.replace('/', '_').replace(':', '_')}.html", "w", encoding="utf-8") as f:
                        f.write(await page.content())
                except Exception as screenshot_e:
                    logging.warning(f"Could not save screenshot/HTML for error: {screenshot_e}")
            finally:
                await page.close()
                logging.info(f"Finished scraping PLP: {subcategory_data['name']}. Total unique PIDs found so far: {len(total_unique_pids_overall)}")
                await asyncio.sleep(random.uniform(2, 5)) # Delay after each category task completes
            
            return {category_url: pids_for_this_category} # Return for gather results


        tasks_plp = [
            scrape_plp_task_with_categorization(browser_context_for_all_scraping, subcategory)
            for subcategory in subcategory_urls
        ]
        await asyncio.gather(*tasks_plp)

        await browser_context_for_all_scraping.close()
        logging.info("Browser context closed after all PLP scraping tasks.")


    logging.info(f"\nFinished scraping product IDs from all categories. Overall unique product IDs found: {len(total_unique_pids_overall)}")

    # Step 3: Fetch detailed data for each unique product ID (from PDP URLs)
    logging.info(f"Starting to scrape detailed data from PDP IDs (Concurrency: {PDP_CONCURRENCY})...")
    all_detailed_product_data = []

    product_ids_list = list(total_unique_pids_overall) # Use the overall unique PIDs

    semaphore_pdp = asyncio.Semaphore(PDP_CONCURRENCY)

    async def scrape_pdp_task_wrapper(product_id):
        async with semaphore_pdp:
            loop = asyncio.get_running_loop()
            detailed_data = await loop.run_in_executor(None, scrape_detailed_product_data, product_id)
            return detailed_data

    pdp_tasks = [scrape_pdp_task_wrapper(pid) for pid in product_ids_list]
    results = await asyncio.gather(*pdp_tasks)

    for data_list in results:
        all_detailed_product_data.extend(data_list)

    logging.info(f"\nFinished scraping detailed data from {len(product_ids_list)} PDPs. Total variants/products scraped: {len(all_detailed_product_data)}")


    if all_detailed_product_data:
        output_filename = FULL_PRODUCT_DATA_FILE
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(all_detailed_product_data, f, indent=4)
        logging.info(f"Saved all scraped product data to {output_filename}")
    else:
        logging.warning("No detailed product data was scraped.")


if __name__ == "__main__":
    asyncio.run(main())