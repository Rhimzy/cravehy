import asyncio
from playwright.async_api import async_playwright
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def open_browser_for_manual_interaction(url="https://blinkit.com/"):
    """
    Opens a Playwright Chromium browser in headful mode using a persistent context,
    allows manual interaction, and keeps the context open until you press Enter.
    """
    user_data_dir = "./tmp_user_data"
    os.makedirs(user_data_dir, exist_ok=True)
    
    logging.info(f"Opening browser for manual interaction. User data will be saved to: {user_data_dir}")
    logging.info("Navigate to the desired page, resolve any challenges (like Cloudflare), or set location.")
    logging.info("Once done, close the browser window OR press Enter in this console to proceed.")

    context = None
    try:
        async with async_playwright() as p:
            context = await p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=False,  # Essential: opens a visible browser window
                args=['--no-sandbox', '--disable-setuid-sandbox'] # Good practice args
            )
            page = await context.new_page()
            await page.goto(url)
            await page.bring_to_front() # Bring the browser window to the foreground

            # Keep the browser open until user presses Enter in the console
            input("\nBrowser is open for manual interaction. Press Enter here to close the browser and continue script (or close browser manually).\n")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if context:
            await context.close()
            logging.info("Browser context closed.")

if __name__ == "__main__":
    asyncio.run(open_browser_for_manual_interaction())