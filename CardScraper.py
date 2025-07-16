import asyncio
import nest_asyncio
import re
import json
from playwright.async_api import async_playwright  # Async browser automation
from DetailsScraper import DetailsScraping         # Custom scraper to extract card details
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class CardScraper:
    def __init__(self, url):
        self.url = url  # Base URL for the category page containing brand listings
        self.data = []  # List to store scraped data for each brand

    # Main scraping function to get brands and their associated card types
    async def scrape_brands_and_types(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Launch headless browser (no UI)
            page = await browser.new_page()  # Create a new tab
            await page.goto(self.url)  # Navigate to the given category URL

            # Select all anchor tags inside brand cards
            brand_elements = await page.query_selector_all('.styles_itemWrapper__MTzPB a')

            # If no brand cards were found, exit early
            if not brand_elements:
                print(f"No brand elements found on {self.url}")
                return self.data

            # Loop through each brand element
            for element in brand_elements:
                title = await element.get_attribute('title')         # Get the brand name/title
                brand_link = await element.get_attribute('href')     # Get the relative or absolute link

                if brand_link:
                    # Build the full URL by extracting scheme and domain from original URL
                    base_url = self.url.split('/', 3)[0] + '//' + self.url.split('/', 3)[2]
                    # Construct full URL in case it's a relative path
                    full_brand_link = base_url + brand_link if brand_link.startswith('/') else brand_link

                    # Debug: Print full link to this brand's page
                    print(f"Full brand link: {full_brand_link}")

                    # Open a new tab for this brand to extract its cards
                    new_page = await browser.new_page()
                    await new_page.goto(full_brand_link)

                    # Use DetailsScraping class to extract card-level details for this brand
                    details_scraper = DetailsScraping(full_brand_link)
                    card_details = await details_scraper.get_card_details()  # Custom method in your scraper
                    await new_page.close()  # Close tab after scraping

                    # Store all extracted info in the data list
                    self.data.append({
                        'brand_title': title,  # The name of the brand
                        'brand_link': full_brand_link.rsplit('/', 1)[0] + '/{}',  # Prepare paginated URL
                        'available_cards': card_details,  # List of cards scraped from the brand page
                    })

                    # Debug: Print brand info
                    print(f"Found brand: {title}, Link: {full_brand_link}")

            await browser.close()  # Close the browser after all brands are processed

        return self.data  # Return the list of all brands with their data
