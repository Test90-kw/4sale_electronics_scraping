import asyncio
import nest_asyncio
import re
import json
from playwright.async_api import async_playwright
from DetailsScraper import DetailsScraping
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

class CardScraper:
    def __init__(self, url):
        self.url = url
        self.data = []

    async def scrape_brands_and_types(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            await page.goto(self.url)

            # Get all brand links and titles
            brand_elements = await page.query_selector_all('.styles_itemWrapper__MTzPB a')

            if not brand_elements:
                print(f"No brand elements found on {self.url}")
                return self.data

            for element in brand_elements:
                title = await element.get_attribute('title')
                brand_link = await element.get_attribute('href')

                if brand_link:
                    base_url = self.url.split('/', 3)[0] + '//' + self.url.split('/', 3)[2]
                    full_brand_link = base_url + brand_link if brand_link.startswith('/') else brand_link

                    # Print the full brand link
                    print(f"Full brand link: {full_brand_link}")

                    # Create a new page to scrape brand types
                    new_page = await browser.new_page()
                    await new_page.goto(full_brand_link)

                    details_scraper = DetailsScraping(full_brand_link)
                    card_details = await details_scraper.get_card_details()
                    await new_page.close()

                    self.data.append({
                        'brand_title': title,
                        'brand_link': full_brand_link.rsplit('/', 1)[0] + '/{}',  # Prepare template for pagination
                        'available_cards': card_details,
                    })

                    # Print the details for each brand
                    print(f"Found brand: {title}, Link: {full_brand_link}")

            await browser.close()
        return self.data

# # Correctly run the async function with an instance of the class
# if __name__ == "__main__":
#     # Initialize the scraper with the main page URL
#     scraper = CarScraper("https://www.q84sale.com/ar/automotive/automotive-services")
#
#     # Use asyncio.run to execute the async function
#     cars = asyncio.run(scraper.scrape_brands_and_types())
#
#     # Print the extracted details
#     for car in cars:
#         print(f"Brand: {car['brand_title']}, Link: {car['brand_link']}, available cars: {car['available_cars']}")
