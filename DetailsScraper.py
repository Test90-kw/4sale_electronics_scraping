import pandas as pd
import json
import asyncio
import nest_asyncio
import re
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# Apply nested event loops (mainly for interactive environments like Jupyter)
nest_asyncio.apply()

class DetailsScraping:
    def __init__(self, url, retries=3):
        self.url = url  # Target page to scrape
        self.retries = retries  # Retry attempts for scraping robustness

    # Main method to scrape all card details from a page
    async def get_card_details(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Headless browser
            page = await browser.new_page()

            # Set browser timeouts
            page.set_default_navigation_timeout(30000)
            page.set_default_timeout(30000)

            cards = []  # Result list

            # Retry loop
            for attempt in range(self.retries):
                try:
                    await page.goto(self.url, wait_until="domcontentloaded")  # Navigate
                    await page.wait_for_selector('.StackedCard_card__Kvggc')  # Ensure cards are loaded

                    card_cards = await page.query_selector_all('.StackedCard_card__Kvggc')  # List of cards
                    for card in card_cards:
                        link = await self.scrape_link(card)
                        card_type = await self.scrape_card_type(card)
                        title = await self.scrape_title(card)
                        pinned_today = await self.scrape_pinned_today(card)

                        # Get detailed info by opening card link
                        scrape_more_details = await self.scrape_more_details(link)

                        cards.append({
                            'id': scrape_more_details.get('id'),
                            'date_published': scrape_more_details.get('date_published'),
                            'relative_date': scrape_more_details.get('relative_date'),
                            'pin': pinned_today,
                            'type': card_type,
                            'title': title,
                            'description': scrape_more_details.get('description'),
                            'link': link,
                            'image': scrape_more_details.get('image'),
                            'price': scrape_more_details.get('price'),
                            'address': scrape_more_details.get('address'),
                            'additional_details': scrape_more_details.get('additional_details'),
                            'specifications': scrape_more_details.get('specifications'),
                            'views_no': scrape_more_details.get('views_no'),
                            'submitter': scrape_more_details.get('submitter'),
                            'ads': scrape_more_details.get('ads'),
                            'membership': scrape_more_details.get('membership'),
                            'phone': scrape_more_details.get('phone'),
                        })
                    break  # Exit on success
                except Exception as e:
                    print(f"Attempt {attempt + 1} failed for {self.url}: {e}")
                    if attempt + 1 == self.retries:
                        print(f"Max retries reached for {self.url}. Returning partial results.")
                        break
                finally:
                    await page.close()
                    if attempt + 1 < self.retries:
                        page = await browser.new_page()

            await browser.close()
            return cards

    # Extracts the link of a listing card
    async def scrape_link(self, card):
        rawlink = await card.get_attribute('href')
        base_url = 'https://www.q84sale.com'
        return f"{base_url}{rawlink}" if rawlink else None

    # Extracts the type/category of the listing (e.g., Prepaid Card)
    async def scrape_card_type(self, card):
        selector = '.text-6-med.text-neutral_600.styles_category__NQAci'
        element = await card.query_selector(selector)
        return await element.inner_text() if element else None

    # Extracts the title of the listing
    async def scrape_title(self, card):
        selector = '.text-4-med.text-neutral_900.styles_title__l5TTA.undefined'
        element = await card.query_selector(selector)
        return await element.inner_text() if element else None

    # Scrapes the label showing if the listing is "Pinned today"
    async def scrape_pinned_today(self, card):
        selector = '.StackedCard_tags__SsKrH'
        element = await card.query_selector(selector)
        if element:
            content = await element.inner_html()
            if content.strip() != "":
                return "Pinned today"
        return "Not Pinned"

    # Extracts relative publish date (e.g., منذ ساعة)
    async def scrape_relative_date(self, page):
        try:
            parent_locator = page.locator('.d-flex.styles_topData__Sx1GF')
            await parent_locator.wait_for(state="visible", timeout=10000)

            data_items = page.locator('.d-flex.align-items-center.styles_dataWithIcon__For9u')
            items = await data_items.all()

            for item in items:
                text = await item.inner_text()
                if any(word in text for word in ['منذ', 'ساعة', 'يوم', 'دقيقة', 'شهر']):
                    time_element = await item.locator('.text-5-regular.m-text-6-med.text-neutral_600').inner_text()
                    return time_element.strip()
            return None
        except Exception as e:
            print(f"Error while scraping relative_date: {e}")
            return None

    # Converts a relative Arabic time string into an absolute datetime
    async def scrape_publish_date(self, relative_time):
        relative_time_pattern = r'(\d+)\s+(Second|Minute|Hour|Day|Month|شهر|ثانية|دقيقة|ساعة|يوم)'
        match = re.search(relative_time_pattern, relative_time, re.IGNORECASE)
        if not match:
            return "Invalid Relative Time"

        number = int(match.group(1))
        unit = match.group(2).lower()
        current_time = datetime.now()

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

    # Scrapes number of views from the card details page
    async def scrape_views_no(self, page):
        try:
            views_selector = '.d-flex.align-items-center.styles_dataWithIcon__For9u .text-5-regular.m-text-6-med.text-neutral_600'
            views_element = await page.query_selector(views_selector)
            return (await views_element.inner_text()).strip() if views_element else None
        except Exception as e:
            print(f"Error while scraping views number: {e}")
            return None

    # Scrapes the listing (ad) ID
    async def scrape_id(self, page):
        parent_selector = '.el-lvl-1.d-flex.align-items-center.justify-content-between.styles_sectionWrapper__v97PG'
        parent_element = await page.query_selector(parent_selector)
        ad_id_selector = '.text-4-regular.m-text-5-med.text-neutral_600'
        ad_id_element = await parent_element.query_selector(ad_id_selector) if parent_element else None
        text = await ad_id_element.inner_text() if ad_id_element else ""
        match = re.search(r'رقم الاعلان:\s*(\d+)', text)
        return match.group(1) if match else None

    # Scrapes main image src of the listing
    async def scrape_image(self, page):
        try:
            image_selector = '.styles_img__PC9G3'
            image = await page.query_selector(image_selector)
            return await image.get_attribute('src') if image else None
        except Exception as e:
            print(f"Error scraping image: {e}")
            return None

    # Scrapes price (e.g., "20 KWD")
    async def scrape_price(self, page):
        price_selector = '.h3.m-h5.text-prim_4sale_500'
        price = await page.query_selector(price_selector)
        return await price.inner_text() if price else "0 KWD"

    # Scrapes address of the seller (e.g., "Salmiya")
    async def scrape_address(self, page):
        address_selector = '.text-4-regular.m-text-5-med.text-neutral_600'
        address = await page.query_selector(address_selector)
        if address:
            text = await address.inner_text()
            if re.match(r'^رقم الاعلان: \d+$', text):
                return "Not Mentioned"
            return text
        return "Not Mentioned"

    # Scrapes list of additional attributes (x1, imported, etc.)
    async def scrape_additionalDetails_list(self, page):
        selector = '.styles_boolAttrs__Ce6YV .styles_boolAttr__Fkh_j div'
        elements = await page.query_selector_all(selector)
        return [await e.inner_text() for e in elements if (await e.inner_text()).strip()]

    # Scrapes specification key-value pairs (e.g., RAM: 4GB)
    async def scrape_specifications(self, page):
        selector = '.styles_attrs__PX5Fs .styles_attr__BN3w_'
        elements = await page.query_selector_all(selector)
        attributes = {}
        for element in elements:
            img = await element.query_selector('img')
            text_el = await element.query_selector('.text-4-med.m-text-5-med.text-neutral_900')
            alt = await img.get_attribute('alt') if img else None
            val = await text_el.inner_text() if text_el else None
            if alt and val:
                attributes[alt] = val.strip()
        return attributes

    # Extract phone number from embedded JSON structure
    async def scrape_phone_number(self, page):
        try:
            script_content = await page.inner_html('script#__NEXT_DATA__')
            if script_content:
                data = json.loads(script_content.strip())
                return data.get("props", {}).get("pageProps", {}).get("listing", {}).get("phone", None)
        except Exception as e:
            print(f"Error while scraping phone number: {e}")
        return None

    # Scrapes submitter info: name, ads count, membership
    async def scrape_submitter_details(self, page):
        info_wrapper_selector = '.styles_infoWrapper__v4P8_.undefined.align-items-center'
        info_wrappers = await page.query_selector_all(info_wrapper_selector)

        if info_wrappers:
            submitter_element = await info_wrappers[0].query_selector('.text-4-med.m-h6.text-neutral_900')
            submitter = await submitter_element.inner_text() if submitter_element else None

            detail_elements = await info_wrappers[0].query_selector_all('.styles_memberDate__qdUsm span.text-neutral_600')
            ads, membership = "0 ads", "membership year not mentioned"

            for el in detail_elements:
                text = await el.inner_text()
                if re.match(r'^\d+\s+(ads|اعلان|إعلان)$', text):
                    ads = text
                elif re.match(r'^عضو منذ \D+\s+\d+$', text) or re.match(r'^member since \D+\s+\d+$', text, re.IGNORECASE):
                    membership = text

            return {
                'submitter': submitter,
                'ads': ads,
                'membership': membership
            }
        return {}

    # Consolidates all sub-scraping for a card detail page
    async def scrape_more_details(self, url):
        for attempt in range(3):
            try:
                async with async_playwright() as p:
                    browser = await p.chromium.launch(headless=True)
                    page = await browser.new_page()
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)

                    id = await self.scrape_id(page)
                    description = await self.scrape_description(page)
                    image = await self.scrape_image(page)
                    price = await self.scrape_price(page)
                    address = await self.scrape_address(page)
                    additional = await self.scrape_additionalDetails_list(page)
                    specs = await self.scrape_specifications(page)
                    views = await self.scrape_views_no(page)
                    submitter = await self.scrape_submitter_details(page)
                    phone = await self.scrape_phone_number(page)
                    relative = await self.scrape_relative_date(page)
                    published = await self.scrape_publish_date(relative) if relative else None

                    await browser.close()
                    return {
                        'id': id,
                        'description': description,
                        'image': image,
                        'price': price,
                        'address': address,
                        'additional_details': additional,
                        'specifications': specs,
                        'views_no': views,
                        'submitter': submitter.get('submitter'),
                        'ads': submitter.get('ads'),
                        'membership': submitter.get('membership'),
                        'phone': phone,
                        'relative_date': relative,
                        'date_published': published,
                    }

            except Exception as e:
                print(f"Error while scraping more details from {url}: {e}")
                if attempt + 1 == 3:
                    print(f"Max retries reached for {url}. Returning partial results.")
                    return {}

        return {}
