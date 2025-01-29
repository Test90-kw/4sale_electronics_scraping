import pandas as pd
import json
import asyncio
import nest_asyncio
import re
import json
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Allow nested event loops (useful in Jupyter)
nest_asyncio.apply()

class DetailsScraping:
    def __init__(self, url, retries=3):
        self.url = url
        self.retries = retries  # Retry count for robustness

    async def get_car_details(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Set timeouts
            page.set_default_navigation_timeout(30000)
            page.set_default_timeout(30000)  # General timeout

            cars = []  # To store scraped cars

            for attempt in range(self.retries):
                try:
                    # Navigate to the page
                    await page.goto(self.url, wait_until="domcontentloaded")
                    await page.wait_for_selector('.StackedCard_card__Kvggc', timeout=30000)

                    # Extract car details
                    car_cards = await page.query_selector_all('.StackedCard_card__Kvggc')
                    for card in car_cards:
                        # Extract car information
                        link = await self.scrape_link(card)
                        car_type = await self.scrape_car_type(card)
                        title = await self.scrape_title(card)
                        pinned_today = await self.scrape_pinned_today(card)

                        # Scrape scrape_more_details from the car page
                        scrape_more_details = await self.scrape_more_details(link)

                        cars.append({
                            'id': scrape_more_details.get('id'),
                            'date_published': scrape_more_details.get('date_published'),
                            'relative_date': scrape_more_details.get('relative_date'),
                            'pin': pinned_today,
                            'type': car_type,
                            'title': title,
                            'description': scrape_more_details.get('description'),
                            'link': link,
                            'image': scrape_more_details.get('image'),
                            'price': scrape_more_details.get('price'),
                            'address': scrape_more_details.get('address'),
                            'additional_details': scrape_more_details.get('additional_details'),
                            'specifications': scrape_more_details.get('specifications'),
                            'views_no': scrape_more_details.get('views_no'),  # Added views number here
                            'submitter': scrape_more_details.get('submitter'),
                            'ads': scrape_more_details.get('ads'),
                            'membership': scrape_more_details.get('membership'),
                            'phone': scrape_more_details.get('phone'),
                        })
                    break  # Exit loop if successful

                except Exception as e:
                    print(f"Attempt {attempt + 1} failed for {self.url}: {e}")
                    if attempt + 1 == self.retries:
                        print(f"Max retries reached for {self.url}. Returning partial results.")
                        break
                finally:
                    # Close page between attempts to ensure proper cleanup
                    await page.close()
                    if attempt + 1 < self.retries:
                        page = await browser.new_page()

            await browser.close()
            return cars

    # Method to scrape the link
    async def scrape_link(self, card):
        rawlink = await card.get_attribute('href')
        base_url = 'https://www.q84sale.com'
        return f"{base_url}{rawlink}" if rawlink else None

    # Method to scrape the car type
    async def scrape_car_type(self, card):
        selector = '.text-6-med.text-neutral_600.styles_category__NQAci'
        element = await card.query_selector(selector)
        return await element.inner_text() if element else None

    # Method to scrape the car title
    async def scrape_title(self, card):
        selector = '.text-4-med.text-neutral_900.styles_title__l5TTA.undefined'
        element = await card.query_selector(selector)
        return await element.inner_text() if element else None

    # Method to scrape the car description
    async def scrape_description(self, page):
        # Selector to match the element containing the description
        selector = '.styles_description__DpRnU'
        element = await page.query_selector(selector)
        return await element.inner_text() if element else "No Description"

    # Method to scrape the pin status
    async def scrape_pinned_today(self, card):
        selector = '.StackedCard_tags__SsKrH'
        element = await card.query_selector(selector)

        if element:
            # Check if the div has any content or span inside it
            content = await element.inner_html()
            if content.strip() != "":
                return "Pinned today"

        return "Not Pinned"

    # New method to scrape the x value (second value)
    async def scrape_relative_date(self, page):
        try:
            # Define the parent container selector
            parent_selector = '.d-flex.styles_topData__Sx1GF'

            # Locate the parent container and get all the child divs with the desired class
            parent_locator = page.locator(parent_selector)

            # Wait for the parent container to be visible before proceeding
            await parent_locator.wait_for(state="visible", timeout=10000)

            # Get all child div elements with the class 'd-flex align-items-center styles_dataWithIcon__For9u'
            child_divs = parent_locator.locator('.d-flex.align-items-center.styles_dataWithIcon__For9u')

            # Wait until the elements are available and then fetch the second child div
            await child_divs.first.wait_for(state="visible", timeout=10000)  # Ensure first child is available
            await child_divs.nth(1).wait_for(state="visible", timeout=10000)  # Wait for second child to be visible

            # Extract the x value (content of the second div)
            relative_time_locator = child_divs.nth(1).locator('div.text-5-regular.m-text-6-med.text-neutral_600')
            relative_time_text = await relative_time_locator.inner_text()
            if relative_time_text:
                stripped_time = relative_time_text.replace(" ago", "").strip()
                return stripped_time  # Clean up whitespace
            else:
                print("relative_time value not found.")
                return None

        except Exception as e:
            print(f"Error while scraping relative_time value: {e}")
            return None

    # Method to scrape date_published
    async def scrape_publish_date(self, relative_time):
        # Regex to find relative time strings like "5 Hours ago" or "30 Minutes ago"
        relative_time_pattern = r'(\d+)\s+(Second|Minute|Hour|Day|Month|شهر|ثانية|دقيقة|ساعة|يوم)'

        # Search for relative time in the input string
        match = re.search(relative_time_pattern, relative_time, re.IGNORECASE)
        if not match:
            return "Invalid Relative Time"

        # Extract the number and unit (Seconds, Minutes, or Hours)
        number = int(match.group(1))
        unit = match.group(2).lower()

        # Get the current date and time
        current_time = datetime.now()

        # Calculate the publish date
        if unit in ["second", "ثانية"]:
            publish_time = current_time - timedelta(seconds=number)
        elif unit in ["minute", "دقيقة"]:
            publish_time = current_time - timedelta(minutes=number)
        elif unit in ["hour", "ساعة"]:
            publish_time = current_time - timedelta(hours=number)
        elif unit in ["day", "يوم"]:
            publish_time = current_time - timedelta(days=number)
        elif unit in ["month", "شهر"]:
            publish_time = current_time - relativedelta(months=number)
        else:
            return "Unsupported time unit found."

        return publish_time.strftime("%Y-%m-%d %H:%M:%S")

    # New method to scrape the number of views
    async def scrape_views_no(self, page):
        try:
            # Define the selector for the views number
            views_selector = '.d-flex.align-items-center.styles_dataWithIcon__For9u .text-5-regular.m-text-6-med.text-neutral_600'

            # Locate the element and extract its text
            views_element = await page.query_selector(views_selector)

            if views_element:
                views_no = await views_element.inner_text()  # Get the text value of x
                return views_no.strip()  # Remove any extra whitespace
            else:
                print(f"Views element not found using selector: {views_selector}")
                return None
        except Exception as e:
            print(f"Error while scraping views number: {e}")
            return None

    async def scrape_id(self, page):
        # Selector for the parent container
        parent_selector = '.el-lvl-1.d-flex.align-items-center.justify-content-between.styles_sectionWrapper__v97PG'

        # Find the parent element
        parent_element = await page.query_selector(parent_selector)
        if not parent_element:
            print("Parent element not found")
            return None

        # Nested element with the Ad ID
        ad_id_selector = '.text-4-regular.m-text-5-med.text-neutral_600'
        ad_id_element = await parent_element.query_selector(ad_id_selector)
        if not ad_id_element:
            print("Ad ID element not found within parent")
            return None

        # Extract inner text
        text = await ad_id_element.inner_text()
        # print(f"Extracted text: {text}")

        # Match the "Ad ID: <number>" pattern
        match = re.search(r'رقم الاعلان:\s*(\d+)', text)
        if match:
            # print(f"Matched Ad ID: {match.group(1)}")
            return match.group(1)
        else:
            print("Regex did not match")

        return None

    async def scrape_image(self, page):
        try:
            image_selector = '.styles_img__PC9G3'
            image = await page.query_selector(image_selector)
            return await image.get_attribute('src') if image else None
        except Exception as e:
            print(f"Error scraping image: {e}")
            return None

    # New method to scrape the price
    async def scrape_price(self, page):
        price_selector = '.h3.m-h5.text-prim_4sale_500'
        price = await page.query_selector(price_selector)
        return await price.inner_text() if price else "0 KWD"

    # New method to scrape the address
    async def scrape_address(self, page):
        address_selector = '.text-4-regular.m-text-5-med.text-neutral_600'
        address = await page.query_selector(address_selector)
        if address:
            text = await address.inner_text()
            # Check if the text matches the format "Ad ID: <any number>"
            if re.match(r'^رقم الاعلان: \d+$', text):
                return "Not Mentioned"
            return text
        return "Not Mentioned"

    async def scrape_additionalDetails_list(self, page):
        # Selector to match the elements containing 'x1'
        selector = '.styles_boolAttrs__Ce6YV .styles_boolAttr__Fkh_j div'
        elements = await page.query_selector_all(selector)

        values_list = []
        for element in elements:
            text = await element.inner_text()
            if text.strip():  # Check if the text is not empty
                values_list.append(text.strip())

        return values_list

    async def scrape_specifications(self, page):
        # Selector to match the structure containing all attributes
        selector = '.styles_attrs__PX5Fs .styles_attr__BN3w_'
        elements = await page.query_selector_all(selector)

        attributes = {}
        for element in elements:
            # Extract the alt attribute value from the <img> tag
            img_element = await element.query_selector('img')
            if img_element:
                alt_text = await img_element.get_attribute('alt')

                # Extract the text from the corresponding <div>
                text_element = await element.query_selector('.text-4-med.m-text-5-med.text-neutral_900')
                if text_element:
                    value = await text_element.inner_text()

                    # Add the extracted information to the dictionary
                    if alt_text and value:
                        # Clean up the value if needed
                        attributes[alt_text] = value.strip()

        return attributes

    # New method to scrape the phone number
    async def scrape_phone_number(self, page):
        """
        Extracts the phone number from a JSON object embedded in the page.
        """
        try:
            # Extract the content of the script tag with id="__NEXT_DATA__"
            script_content = await page.inner_html('script#__NEXT_DATA__')

            if script_content:
                # Parse the JSON data from the script content
                data = json.loads(script_content.strip())

                # Navigate through the structure to find the phone number
                phone_number = data.get("props", {}).get("pageProps", {}).get("listing", {}).get("phone", None)

                if phone_number:
                    return phone_number
                else:
                    print("Phone number not found in the JSON structure.")
                    return None
            else:
                print("Script tag with id '__NEXT_DATA__' not found.")
                return None

        except Exception as e:
            print(f"Error while scraping phone number: {e}")
            return None

    # Add new submitter scraping method
    async def scrape_submitter_details(self, page):
        info_wrapper_selector = '.styles_infoWrapper__v4P8_.undefined.align-items-center'
        info_wrappers = await page.query_selector_all(info_wrapper_selector)

        if len(info_wrappers) > 0:  # Ensure there is at least one div
            second_div = info_wrappers[0]  # Use the first div inside the info wrapper
            submitter_selector = '.text-4-med.m-h6.text-neutral_900'
            submitter_element = await second_div.query_selector(submitter_selector)
            submitter = await submitter_element.inner_text() if submitter_element else None

            details_selector = '.styles_memberDate__qdUsm span.text-neutral_600'
            detail_elements = await second_div.query_selector_all(details_selector)

            # Initialize ads and membership with default values
            ads = "0 ads"
            membership = "membership year not mentioned"

            for detail_element in detail_elements:
                detail_text = await detail_element.inner_text()

                # Check for ads pattern
                if re.match(r'^\d+\s+ads$', detail_text, re.IGNORECASE) or re.match(r'^\d+\s+اعلان$', detail_text, re.IGNORECASE) or re.match(r'^\d+\s+إعلان$', detail_text, re.IGNORECASE):
                    ads = detail_text

                # Check for membership pattern
                elif re.match(r'^عضو منذ \D+\s+\d+$', detail_text) or re.match(r'^member since \D+\s+\d+$', detail_text, re.IGNORECASE):
                    membership = detail_text

            return {
                'submitter': submitter,
                'ads': ads,
                'membership': membership
            }
        return {}

    # Method to scrape more_details
    async def scrape_more_details(self, url):
        retries = 3  # Number of retries for robustness
        for attempt in range(retries):
            try:
                # Create a new page for this car detail scraping
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    page = await browser.new_page()

                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)  # Increased timeout

                    # Extract details using helper methods
                    id = await self.scrape_id(page)
                    description = await self.scrape_description(page)
                    image = await self.scrape_image(page)
                    price = await self.scrape_price(page)
                    address = await self.scrape_address(page)
                    additional_details = await self.scrape_additionalDetails_list(page)
                    specifications = await self.scrape_specifications(page)
                    views_no = await self.scrape_views_no(page)
                    submitter_details = await self.scrape_submitter_details(page)
                    phone = await self.scrape_phone_number(page)
                    relative_date = await self.scrape_relative_date(page)
                    date_published = await self.scrape_publish_date(relative_date) if relative_date else None

                    # Consolidate details into a dictionary
                    details = {
                        'id': id,
                        'description': description,
                        'image': image,
                        'price': price,
                        'address': address,
                        'additional_details': additional_details,
                        'specifications': specifications,
                        'views_no': views_no,
                        'submitter': submitter_details.get('submitter'),
                        'ads': submitter_details.get('ads'),
                        'membership': submitter_details.get('membership'),
                        'phone': phone,
                        'relative_date': relative_date,
                        'date_published': date_published,
                    }

                    await browser.close()
                    return details

            except Exception as e:
                print(f"Error while scraping more details from {url}: {e}")
                if attempt + 1 == retries:
                    print(f"Max retries reached for {url}. Returning partial results.")
                    return {}

        return {}

# # Correctly run the async function with an instance of the class
# if __name__ == "__main__":
#     # Initialize the scraper with the main page URL
#     scraper = DetailsScraping("https://www.q84sale.com/ar/automotive/automotive-accessories/1")
#
#     # Use asyncio.run to execute the async function
#     cars = asyncio.run(scraper.get_car_details())
#
#     # Print the extracted details
#     for car in cars:
#         print(car)