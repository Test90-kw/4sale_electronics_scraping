# Import required libraries
import asyncio
import pandas as pd
import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from pathlib import Path
from DetailsScraper import DetailsScraping  # Scraper for individual listing pages
from SavingOnDrive import SavingOnDrive  # Google Drive helper for saving files


class ElectronicsMainScraper:
    def __init__(self, electronics_data: Dict[str, List[Tuple[str, int]]]):
        self.electronics_data = electronics_data  # Dictionary with category name → [(URL template, number of pages)]
        self.chunk_size = 2  # Number of categories to process at a time
        self.max_concurrent_links = 2  # Maximum concurrent category scrapes
        self.logger = logging.getLogger(__name__)  # Logger instance
        self.setup_logging()  # Configure logging
        self.temp_dir = Path("temp_files")  # Directory to save temporary Excel files
        self.temp_dir.mkdir(exist_ok=True)
        self.upload_retries = 3  # Retry count for Drive upload
        self.upload_retry_delay = 15  # Seconds to wait before retrying upload
        self.page_delay = 3  # Delay between page requests
        self.chunk_delay = 10  # Delay between chunk batches

    def setup_logging(self):
        """Initialize logging configuration."""
        stream_handler = logging.StreamHandler()
        file_handler = logging.FileHandler("scraper.log")
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[stream_handler, file_handler],
        )
        self.logger.setLevel(logging.INFO)
        print("Logging setup complete.")

    async def scrape_electronic(self, electronic_name: str, urls: List[Tuple[str, int]], semaphore: asyncio.Semaphore) -> List[Dict]:
        """Scrape data for a single category (with semaphore limiting concurrency)."""
        self.logger.info(f"Starting to scrape {electronic_name}")
        card_data = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")  # Filter date

        async with semaphore:
            for url_template, page_count in urls:
                for page in range(1, page_count + 1):
                    url = url_template.format(page)  # Format the URL for current page
                    scraper = DetailsScraping(url)  # Create scraping instance
                    try:
                        cards = await scraper.get_card_details()  # Scrape listing cards
                        for card in cards:
                            # Filter for only yesterday's listings
                            if card.get("date_published") and card.get("date_published", "").split()[0] == yesterday:
                                card_data.append(card)
                        await asyncio.sleep(self.page_delay)  # Respectful delay between pages
                    except Exception as e:
                        self.logger.error(f"Error scraping {url}: {e}")
                        continue
        return card_data

    async def save_to_excel(self, electronic_name: str, card_data: List[Dict]) -> str:
        """Save scraped data to an Excel file."""
        if not card_data:
            self.logger.info(f"No data to save for {electronic_name}, skipping Excel file creation.")
            return None

        safe_name = electronic_name.replace('/', '_').replace('\\', '_')  # Sanitize filename
        excel_file = Path(f"{safe_name}.xlsx")
        try:
            df = pd.DataFrame(card_data)  # Convert to DataFrame
            df.to_excel(excel_file, index=False)  # Save to Excel
            self.logger.info(f"Successfully saved data for {electronic_name}")
            return str(excel_file)
        except Exception as e:
            self.logger.error(f"Error saving Excel file {excel_file}: {e}")
            return None

    async def upload_files_with_retry(self, drive_saver, files: List[str]) -> List[str]:
        """Upload files to Google Drive with retry mechanism."""
        uploaded_files = []
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            self.logger.info(f"Checking local files before upload: {files}")
            for file in files:
                self.logger.info(f"File {file} exists: {os.path.exists(file)}, size: {os.path.getsize(file) if os.path.exists(file) else 'N/A'}")

            folder_id = drive_saver.get_folder_id(yesterday)  # Find yesterday's folder
            if not folder_id:
                self.logger.info(f"Creating new folder for date: {yesterday}")
                folder_id = drive_saver.create_folder(yesterday)  # Create if not exist
                if not folder_id:
                    raise Exception("Failed to create or get folder ID")
                self.logger.info(f"Created new folder '{yesterday}' with ID: {folder_id}")

            for file in files:
                for attempt in range(self.upload_retries):
                    try:
                        if os.path.exists(file):
                            file_id = drive_saver.upload_file(file, folder_id)  # Upload file
                            if not file_id:
                                raise Exception("Upload returned no file ID")
                            uploaded_files.append(file)
                            self.logger.info(f"Successfully uploaded {file} with ID: {file_id}")
                            break
                        else:
                            self.logger.error(f"File not found for upload: {file}")
                            break
                    except Exception as e:
                        self.logger.error(f"Upload attempt {attempt + 1} failed for {file}: {e}")
                        if attempt < self.upload_retries - 1:
                            self.logger.info(f"Retrying after {self.upload_retry_delay} seconds...")
                            await asyncio.sleep(self.upload_retry_delay)
                            drive_saver.authenticate()  # Re-authenticate before retry
                        else:
                            self.logger.error(f"Failed to upload {file} after {self.upload_retries} attempts")
        except Exception as e:
            self.logger.error(f"Error in upload process: {e}")
            raise

        return uploaded_files

    async def scrape_all_electronics(self):
        """Main entry point to scrape and upload all electronic categories."""
        self.temp_dir.mkdir(exist_ok=True)

        # Setup and authenticate Google Drive access
        try:
            credentials_json = os.environ.get("ELECTRONICS_GCLOUD_KEY_JSON")
            if not credentials_json:
                raise EnvironmentError("ELECTRONICS_GCLOUD_KEY_JSON environment variable not found")
            else:
                self.logger.info("Environment variable ELECTRONICS_GCLOUD_KEY_JSON is set.")

            credentials_dict = json.loads(credentials_json)
            drive_saver = SavingOnDrive(credentials_dict)
            drive_saver.authenticate()
            self.logger.info("Testing Drive API access...")
            try:
                drive_saver.service.files().get(fileId=drive_saver.parent_folder_id).execute()
                self.logger.info("Successfully accessed parent folder")
            except Exception as e:
                self.logger.error(f"Failed to access parent folder: {e}")
                return
        except Exception as e:
            self.logger.error(f"Failed to setup Google Drive: {e}")
            return

        # Split categories into chunks
        electronics_chunks = [
            list(self.electronics_data.items())[i : i + self.chunk_size]
            for i in range(0, len(self.electronics_data), self.chunk_size)
        ]

        semaphore = asyncio.Semaphore(self.max_concurrent_links)

        # Process each chunk sequentially
        for chunk_index, chunk in enumerate(electronics_chunks, 1):
            self.logger.info(f"Processing chunk {chunk_index}/{len(electronics_chunks)}")

            tasks = []
            for electronic_name, urls in chunk:
                task = asyncio.create_task(self.scrape_electronic(electronic_name, urls, semaphore))
                tasks.append((electronic_name, task))
                await asyncio.sleep(2)  # Space out task creation slightly

            pending_uploads = []
            for electronic_name, task in tasks:
                try:
                    card_data = await task
                    if card_data:
                        excel_file = await self.save_to_excel(electronic_name, card_data)
                        if excel_file:
                            pending_uploads.append(excel_file)
                except Exception as e:
                    self.logger.error(f"Error processing {electronic_name}: {e}")

            # Upload Excel files
            if pending_uploads:
                await self.upload_files_with_retry(drive_saver, pending_uploads)

                # Cleanup local files
                for file in pending_uploads:
                    try:
                        os.remove(file)
                        self.logger.info(f"Cleaned up local file: {file}")
                    except Exception as e:
                        self.logger.error(f"Error cleaning up {file}: {e}")

            # Wait before next chunk
            if chunk_index < len(electronics_chunks):
                self.logger.info(f"Waiting {self.chunk_delay} seconds before next chunk...")
                await asyncio.sleep(self.chunk_delay)


# Main entry point
if __name__ == "__main__":
    # Define electronics categories and URLs (each with # of pages)
    electronics_data = {
        "موبايلات و إكسسوارات": [("https://www.q84sale.com/ar/electronics/mobile-phones-and-accessories/{}", 3)],
        "تابلت / ايباد": [("https://www.q84sale.com/ar/electronics/tablets/{}", 1)],
        "أجهزة منزلية/مكتبية": [("https://www.q84sale.com/ar/electronics/homeoffice-appliances/{}", 3)],
        "أرقام موبايلات": [("https://www.q84sale.com/ar/electronics/mobile-numbers/{}", 2)],
        "الصوت و السماعات": [("https://www.q84sale.com/ar/electronics/audio-and-headphones/{}", 1)],
        "لابتوب وكمبيوتر": [("https://www.q84sale.com/ar/electronics/laptop-and-computer/{}", 4)],
        "ساعات ذكية": [("https://www.q84sale.com/ar/electronics/smartwatches/{}", 1)],
        "تلفزيونات ذكية": [("https://www.q84sale.com/ar/electronics/smart-tv/{}", 2)],
        "ريسيفرات": [("https://www.q84sale.com/ar/electronics/satellite-receiver/{}", 2)],
        "مطلوب و نشتري": [("https://www.q84sale.com/ar/electronics/wanted-devices/{}", 1)],
        "خدمات إلكترونية": [("https://www.q84sale.com/ar/electronics/electronics-services/{}", 1)],
        "أجهزة أخرى": [("https://www.q84sale.com/ar/electronics/other-electronics/{}", 2)],
    }

    # Run main async function
    async def main():
        scraper = ElectronicsMainScraper(electronics_data)
        await scraper.scrape_all_electronics()

    asyncio.run(main())
