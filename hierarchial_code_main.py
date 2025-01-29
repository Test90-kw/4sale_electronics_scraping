import asyncio
import pandas as pd
import os
import json
import logging
from datetime import datetime, timedelta
from DetailsScraper import DetailsScraping
from CarScraper import CarScraper
from pathlib import Path
from typing import Dict, List, Tuple
from playwright.async_api import async_playwright


class HierarchialMainScraper:
    def __init__(self):
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    def filter_yesterday_data(self, cars_data):
        """Filter car data to only include entries from yesterday"""
        filtered_data = []
        for car in cars_data:
            try:
                car_date = datetime.strptime(car['date_published'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
                if car_date == self.yesterday:
                    filtered_data.append(car)
            except (ValueError, TypeError):
                continue
        print(f"Filtered data for {self.yesterday}: {filtered_data}")
        return filtered_data

    def save_to_excel(self, category_name, brand_data):
        """Save data to Excel file with multiple sheets based on brand titles"""
        try:
            filename = f"{category_name}.xlsx"

            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                if not brand_data:
                    pd.DataFrame().to_excel(writer, sheet_name='No Data', index=False)
                    print(f"Created empty Excel file for {category_name}")
                    return

                for brand in brand_data:
                    try:
                        yesterday_cars = self.filter_yesterday_data(brand.get('available_cars', []))

                        if yesterday_cars:
                            df = pd.DataFrame(yesterday_cars)
                            sheet_name = "".join(x for x in brand['brand_title'] if x.isalnum())[:31]
                            df.to_excel(writer, sheet_name=sheet_name, index=False)
                            print(f"Added sheet {sheet_name} to {filename}")
                        else:
                            print(f"No data for brand {brand['brand_title']} from yesterday")

                    except Exception as e:
                        print(f"Error saving sheet for brand {brand.get('brand_title', 'unknown')}: {str(e)}")
                        continue

            print(f"Successfully created Excel file: {filename}")

        except Exception as e:
            print(f"Error saving Excel file for {category_name}: {str(e)}")

    async def process_category(self, category_name, urls):
        """Process a category of URLs and save results to Excel"""
        all_brand_data = []

        for url in urls:
            try:
                print(f"Scraping data from URL: {url}")
                scraper = CarScraper(url)
                brand_data = await scraper.scrape_brands_and_types()
                if brand_data:
                    all_brand_data.extend(brand_data)
                    print(f"Successfully scraped data from {url}")
                else:
                    print(f"No data retrieved from {url}")

            except Exception as e:
                print(f"Error processing URL {url}: {str(e)}")
                continue

        self.save_to_excel(category_name, all_brand_data)
        return all_brand_data

    async def run(self, automotives_data):
        """Main method to run the scraper"""
        print("Starting the scraping process...")

        for category_name, urls in automotives_data.items():
            print(f"\nProcessing category: {category_name}")
            await self.process_category(category_name, urls)

        print("\nScraping process completed!")


def main():
    automotives_data = {
        "الوكالات": ["https://www.q84sale.com/ar/automotive/dealerships"],
        "دراجات": ["https://www.q84sale.com/ar/automotive/bikes"],
        "مكاتب تأجير السيارات": ["https://www.q84sale.com/ar/automotive/car-rental"],
        "مكاتب السيارات": ["https://www.q84sale.com/ar/automotive/car-offices"],
        "خدمات المحركات": ["https://www.q84sale.com/ar/automotive/automotive-services"],
    }

    scraper = HierarchialMainScraper()
    asyncio.run(scraper.run(automotives_data))


if __name__ == "__main__":
    main()