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
# (Keep parse_categories_html_v2 as is)
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

    unique_subcategories = list({subcat['url']: subcat for subcat in subcategories_list}.values())
    logging.info(f"Reduced to {len(unique_subcategories)} unique subcategory URLs.")
    return unique_subcategories


# --- Function to fetch category HTML using Playwright (with automated location setting - v10) ---
async def get_categories_with_playwright_v10(url="https://blinkit.com/categories", location_query="Mumbai"):
    """
    Fetches the Blinkit categories page using Playwright, automates setting location.
    Uses the previously working class-based selector for the location trigger.
    Increased timeout for location trigger.
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
            await page.goto(url, wait_until='domcontentloaded', timeout=60000)

            # --- Generic initial wait for page body content ---
            try:
                 logging.info("Waiting for page body to contain some content...")
                 await page.wait_for_selector('body > div:first-child', state='visible', timeout=10000)
                 logging.info("Page body content appears loaded.")
            except Exception:
                 logging.warning("Initial wait for body content timed out. Proceeding.")
            # --- End generic initial wait ---


            logging.info("Attempting to set location...")

            # --- Automated Location Setting Logic ---
            try:
                # 1. Use the previously working class-based selector for the location picker trigger
                location_picker_selector = 'div.LocationBar__Container-sc-x8ezho-6'
                logging.info(f"Waiting for location picker trigger: {location_picker_selector}")
                # Increased timeout for this specific selector
                await page.wait_for_selector(location_picker_selector, state='visible', timeout=20000) # Increased timeout
                logging.info("Location picker trigger found. Clicking...")

                await asyncio.sleep(1) # Small delay before clicking

                await page.click(location_picker_selector)
                logging.info("Clicked location picker trigger.")

                # 2. Wait for the location search input field to appear in the dialog
                location_input_selector = 'input[name="select-locality"][placeholder*="search delivery location"]'
                logging.info(f"Waiting for location input field: {location_input_selector}")
                await page.wait_for_selector(location_input_selector, state='visible', timeout=15000)
                logging.info("Location input field found.")

                # 3. Type the desired location
                logging.info(f"Typing location query: {location_query}")
                await page.fill(location_input_selector, location_query)
                logging.info("Typed location query.")

                # 4. Wait for location suggestions to appear and click the first one
                suggestion_selector = 'div.LocationSearchList__LocationListContainer-sc-93rfr7-0'
                logging.info(f"Waiting for location suggestions to appear (e.g., '{suggestion_selector}')...")
                await page.wait_for_selector(suggestion_selector, state='visible', timeout=15000) # Wait for the first suggestion item
                logging.info("Suggestions appeared. Clicking the first one.")
                await page.click(suggestion_selector) # Click the first suggestion item
                logging.info("Clicked the first suggestion.")

                # 5. Wait for the page to update/reload after setting location
                # Wait for network idle and then confirm presence of category links
                category_links_selector = 'a[href^="/cn/"]'
                logging.info(f"Waiting for page update and category links to appear ({category_links_selector})...")
                await page.wait_for_load_state('networkidle', timeout=45000)
                await page.wait_for_selector(category_links_selector, state='visible', timeout=15000) # Increased timeout

                logging.info("Category links appear to be loaded after setting location.")


            except Exception as e:
                logging.error(f"An error occurred during automated location setting: {e}")
                logging.warning("Proceeding to get HTML content anyway, but category loading might have failed.")
            # --- End Automated Location Setting Logic ---


            html_content = await page.content()
            logging.info(f"Fetched {len(html_content)} bytes of rendered HTML.")

        except Exception as e:
            logging.error(f"An error occurred during Playwright execution: {e}")
        finally:
            if browser:
                logging.info("Keeping browser open for 10 seconds for inspection...")
                await asyncio.sleep(10) # Keep browser open for 10 seconds
                await browser.close()
                logging.info("Browser closed.")

    return html_content

# --- Function to scrape product IDs from a category PLP using Playwright (v10 - scrolling container & ID extraction) ---
async def scrape_product_ids_from_plp_v10(page, category_url):
    """
    Navigates to a category URL using an existing Playwright page,
    scrolls within the product list container to load all products,
    and extracts product IDs from the product card divs.

    Args:
        page: An existing Playwright Page object.
        category_url (str): The URL of the category listing page (PLP).

    Returns:
        set: A set of unique product IDs (strings).
             Returns an empty set if fetching or scraping fails.
    """
    logging.info(f"Navigating to category PLP: {category_url}")
    product_ids = set()
    last_count = -1
    scroll_attempts = 0
    max_scroll_attempts = 40 # Increased max scroll attempts

    # --- PRODUCT LIST CONTAINER SELECTOR ---
    # Based on the HTML snippet you provided
    PRODUCT_LIST_CONTAINER_SELECTOR = '#plpContainer'
    # --- END SELECTOR ---

    # --- PRODUCT CARD SELECTOR ---
    # Based on the HTML snippet - divs with id, tabindex="0", role="button"
    PRODUCT_CARD_SELECTOR = f'{PRODUCT_LIST_CONTAINER_SELECTOR} div[id][tabindex="0"][role="button"]'
    # --- END SELECTOR ---


    try:
        await page.goto(category_url, wait_until='domcontentloaded', timeout=60000)
        logging.info("Category PLP DOM loaded. Waiting for product container and products...")

        # Wait for the scrollable container to be visible
        try:
            logging.info(f"Waiting for product list container: {PRODUCT_LIST_CONTAINER_SELECTOR}")
            await page.wait_for_selector(PRODUCT_LIST_CONTAINER_SELECTOR, state='visible', timeout=20000)
            logging.info("Product list container found.")
        except Exception:
            logging.error(f"Product list container '{PRODUCT_LIST_CONTAINER_SELECTOR}' not found within 20 seconds. Cannot scroll.")
            return set()


        # --- Initial wait for products and retry logic ---
        initial_wait_attempts = 5
        for attempt in range(initial_wait_attempts):
            try:
                logging.info(f"Attempt {attempt + 1}/{initial_wait_attempts}: Waiting for initial product cards within container: {PRODUCT_CARD_SELECTOR}")
                # Wait for at least 1 product card to be visible
                await page.wait_for_selector(PRODUCT_CARD_SELECTOR, state='visible', timeout=10000)
                logging.info("Initial product cards appeared.")
                break # Exit retry loop if successful
            except Exception:
                logging.warning(f"Attempt {attempt + 1} failed: Did not find initial product cards within 10 seconds. Retrying...")
                await asyncio.sleep(random.uniform(2, 4)) # Wait before retry
        else: # This block executes if the loop completes without 'break' (all attempts failed)
             logging.error(f"Failed to find any product cards after {initial_wait_attempts} attempts. Proceeding with scrolling, but likely no products are loaded.")
        # --- End initial wait and retry ---


        while scroll_attempts < max_scroll_attempts:
            # Get the current number of product cards within the container
            current_cards = await page.query_selector_all(PRODUCT_CARD_SELECTOR)
            current_count = len(current_cards)
            logging.info(f"Scroll attempt {scroll_attempts + 1}: Found {current_count} product cards so far in container.")

            # Check if new products loaded since last scroll
            if current_count > 0 and current_count == last_count:
                logging.info("No new products loaded after scrolling within container. Reached end of category or dynamic loading stopped.")
                break

            last_count = current_count

            # Scroll down the *container*
            await page.evaluate(f"""() => {{
                const container = document.querySelector('{PRODUCT_LIST_CONTAINER_SELECTOR}');
                if (container) {{
                    container.scrollTo(0, container.scrollHeight);
                }}
            }}""")
            logging.info("Scrolled container down.")

            # Wait briefly for new content to appear after scrolling the container
            try:
                # Wait for the number of product cards *within the container* to increase
                await asyncio.sleep(1) # Small delay before checking the count
                await page.wait_for_function(f"""() => {{
                    const container = document.querySelector('{PRODUCT_LIST_CONTAINER_SELECTOR}');
                    if (!container) return false;
                    return container.querySelectorAll('{PRODUCT_CARD_SELECTOR}').length > {current_count};
                }}""", timeout=15000) # Increased timeout for wait_for_function
                logging.info("Detected new product cards after scrolling container.")
            except Exception:
                logging.warning("Timeout or no new product cards detected after scrolling container. May have reached the end.")
                # If we timed out waiting for new products, it might mean the end, so we break the scroll loop
                # Re-query the cards to make sure the count hasn't actually increased just after the wait
                final_check_cards = await page.query_selector_all(PRODUCT_CARD_SELECTOR)
                if len(final_check_cards) > 0 and len(final_check_cards) == last_count:
                     logging.info("Confirmed no new product cards after waiting. Ending scroll.")
                     break


            scroll_attempts += 1
            await asyncio.sleep(random.uniform(1, 3)) # Delay between scrolls

        logging.info(f"Finished scrolling after {scroll_attempts} attempts (max {max_scroll_attempts}).")

        # Extract all product IDs from the product cards after scrolling is complete
        final_cards = await page.query_selector_all(PRODUCT_CARD_SELECTOR)
        logging.info(f"Total product cards found after scrolling: {len(final_cards)}")

        for card in final_cards:
            product_id = await card.get_attribute('id')
            if product_id:
                product_ids.add(product_id) # Add to set for uniqueness

        logging.info(f"Extracted {len(product_ids)} unique product IDs from {category_url}")

    except Exception as e:
        logging.error(f"An error occurred while scraping PLP {category_url}: {e}")

    return product_ids # Return set of product IDs


# --- Function to scrape detailed data from a PDP using requests and JSON ---
# (Keep scrape_detailed_product_data as is, it will now be called with IDs from the set)
def scrape_detailed_product_data(product_id): # Now takes product_id
    """
    Fetches a single product detail page (using product ID) and extracts detailed information
    from the embedded JSON (window.grofers.PRELOADED_STATE).
    """
    # Construct the PDP URL using the product ID
    # Based on previous PDP URLs, format seems to be /prn/{slug}/prid/{id}
    # The slug might not be strictly necessary, let's use a generic one
    product_url = f"https://blinkit.com/prn/product/prid/{product_id}" # Constructed URL

    product_data = [] # List to hold data for all variants of this product group

    logging.info(f"Fetching detailed data for PDP: {product_url} (ID: {product_id})")

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://blinkit.com/', # Referer could be categories or a PLP (using base URL)
        'DNT': '1',
        'Connection': 'keep-alive',
    }

    try:
        response = requests.get(product_url, headers=headers, timeout=30)
        response.raise_for_status() # Check for 4xx/5xx errors

        # Check if the final URL after redirects is still a valid PDP URL
        # Sometimes sites redirect if the slug is wrong, but the ID might lead to the right page
        # If it redirects away from a PDP pattern, it might be an invalid ID or product
        if '/prid/' not in response.url:
             logging.warning(f"Redirected away from expected PDP URL pattern for ID {product_id}. Final URL: {response.url}")
             return [] # Skip if it didn't land on a PDP


        soup = BeautifulSoup(response.content, 'lxml')

        script_tag = soup.find('script', string=re.compile(r'window\.grofers\.PRELOADED_STATE = \{'))

        if not script_tag:
            logging.error(f"Could not find PRELOADED_STATE script tag on {product_url} (ID: {product_id})")
            # Optionally save the HTML content for inspection if this happens unexpectedly
            # with open(f"failed_json_id_{product_id}.html", "w", encoding="utf-8") as f:
            #     f.write(response.text)
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
            # Use the product_id from the outer scope if variant's id is missing, or use variant's id
            variant_product_id = variant.get('id') or variant.get('product_id') or product_id
            if not variant_product_id:
                 logging.warning(f"Skipping variant due to missing product_id for group ID {product_id}: {variant.get('name')}")
                 continue

            variant_data = {
                'product_id': variant_product_id, # Use the ID found in the variant data
                'group_id': variant.get('group_id'), # Group ID links variants together
                'name': variant.get('name'),
                'brand': variant.get('brand'),
                'category_l0': variant.get('level0_category', [{}])[0].get('name') if variant.get('level0_category') else None,
                'category_l1': variant.get('level1_category', [{}])[0].get('name') if variant.get('level1_category') else None,
                'unit': variant.get('unit'),
                'price': variant.get('price'),
                'original_price': variant.get('mrp'),
                'inventory': variant.get('inventory'),
                'product_url': response.url, # Store the final URL after redirects
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
        # Optionally save the HTML content for inspection if this happens unexpectedly
        # with open(f"failed_json_id_{product_id}.html", "w", encoding="utf-8") as f:
        #     f.write(response.text)
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred during PDP scraping for URL {product_url} (ID: {product_id}): {e}")
        return []

    return product_data


# --- Main execution block ---
async def main():
    categories_url = "https://blinkit.com/categories"
    # Step 1: Get HTML for categories page using Playwright, including location setting attempt
    # Keep headless=False here initially to ensure location setting is visually confirmed
    html_content = await get_categories_with_playwright_v10(categories_url, location_query="Mumbai") # Use v10

    if not html_content:
        logging.error("Could not get HTML content for categories page after attempting location setting. Exiting.")
        return

    # Step 1 (cont.): Parse HTML to get subcategory URLs
    subcategory_urls = parse_categories_html_v2(html_content)

    if not subcategory_urls:
        logging.error("No subcategory URLs found on the categories page using v2 parser after location attempt. Exiting.")
        with open("failed_categories_page_html.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        logging.info("Saved fetched HTML to failed_categories_page_html.html for inspection.")
        return

    logging.info(f"Successfully extracted {len(subcategory_urls)} unique subcategory URLs.")
    # logging.info("Subcategory URLs: " + json.dumps(subcategory_urls, indent=4)) # Uncomment to see list of URLs


    all_unique_product_ids = set() # Collect product IDs, not full URLs yet

    # Step 2: Scrape product IDs from each category PLP using Playwright
    logging.info("Starting to scrape product IDs from category PLPs using Playwright...")
    async with async_playwright() as p:
         browser = None
         try:
              # Launch browser for scraping PLPs
              # Set headless=False here to watch PLP scraping and scrolling
              browser = await p.chromium.launch(headless=False) # <--- Set headless=False to debug PLP scraping
              page = await browser.new_page()

              for i, subcategory in enumerate(subcategory_urls):
                  # Limit the number of categories for faster testing if needed
                  # if i >= 2: # Uncomment to test with only the first 2 categories
                  #     logging.info("Limiting to first 2 categories for testing.")
                  #     break

                  logging.info(f"Scraping PLP {i+1}/{len(subcategory_urls)}: {subcategory['name']} ({subcategory['url']})")
                  # Call the Playwright function to scrape product IDs from this PLP
                  product_ids_in_category = await scrape_product_ids_from_plp_v10(page, subcategory['url']) # Use v10
                  all_unique_product_ids.update(product_ids_in_category) # Add unique IDs to the master set
                  logging.info(f"Total unique product IDs found so far: {len(all_unique_product_ids)}")
                  await asyncio.sleep(random.uniform(2, 5)) # Add a longer delay between categories

         except Exception as e:
            logging.error(f"An error occurred during PLP scraping loop: {e}")
         finally:
            if browser:
                await browser.close()
                logging.info("Browser closed after PLP scraping.")


    logging.info(f"\nFinished scraping product IDs from all categories. Total unique product IDs found: {len(all_unique_product_ids)}")

    # Step 3: Fetch detailed data for each unique product ID (from PDP URLs)
    logging.info("Starting to scrape detailed data from PDP URLs...")
    all_detailed_product_data = []

    # Convert set to list to iterate
    product_ids_list = list(all_unique_product_ids)

    # For faster testing, you might want to limit the number of PDPs scraped
    # product_ids_list = product_ids_list[:10] # Uncomment to scrape only the first 10 PDPs
    # logging.info(f"Limiting detailed scrape to {len(product_ids_list)} PDPs for testing.")


    for i, product_id in enumerate(product_ids_list): # Iterate through product IDs
        logging.info(f"Scraping PDP {i+1}/{len(product_ids_list)} for Product ID: {product_id}")
        # Pass the product ID to the detailed scraping function
        detailed_data = scrape_detailed_product_data(product_id)
        if detailed_data:
            all_detailed_product_data.extend(detailed_data) # Add data for all variants

        # Add a delay between PDP requests
        time.sleep(random.uniform(0.5, 2)) # Shorter delay for individual pages

    logging.info(f"\nFinished scraping detailed data from {len(product_ids_list)} PDPs. Total variants/products scraped: {len(all_detailed_product_data)}")


    if all_detailed_product_data:
        output_filename = "blinkit_all_product_data.json"
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(all_detailed_product_data, f, indent=4)
        logging.info(f"Saved all scraped product data to {output_filename}")
    else:
        logging.warning("No detailed product data was scraped.")


if __name__ == "__main__":
    asyncio.run(main())