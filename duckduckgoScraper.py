import pandas as pd
import time
import os
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import re

def check_valid_brand_name(potential_brand):
    if not isinstance(potential_brand, str) or not potential_brand:
        return False
    word = potential_brand.strip()
    price_pattern = r'^[$\£€]?\s?\d{1,3}(?:[,.]\d{3})*(?:[.,]\d{1,2})?$'
    review_pattern = r'^\(?\d{1,3}(?:,\d{3})*\)?$'
    discount_pattern = r'^\d+(\.\d+)?%\s?(Off|OFF)?$'
    measurement_pattern = r'^\d+(\.\d+)?\s?(oz|lb|kg|g|ml|l|ft|in|cm|mm)$'
    if re.match(price_pattern, word) or \
       re.match(review_pattern, word) or \
       re.match(discount_pattern, word, re.IGNORECASE) or \
       re.match(measurement_pattern, word, re.IGNORECASE):
        return False
    lower_word = word.lower()
    invalid_substrings = [
        "shipping", "save", "stars", " out of ",
        "review", "rating", "customer", "sponsored", "advertisement",
        "delivery", "available", "sold by", "from ", "compare", "color",
        "size", "type", "with", "only", "days", "hours"
    ]
    exact_invalid_words = {
        "new", "best", "top", "free", "and", "or", "the", "for", "with", "view", "details"
    }
    if lower_word in exact_invalid_words:
        return False
    if any(term in lower_word for term in invalid_substrings):
        return False
    if not any(c.isalpha() for c in word):
        return False
    allowed_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ,.-&+'|@")
    if not all(c in allowed_chars for c in word):
        return False
    return True


def scrape_duckduckgo(playwright, query, NUM_ITEMS):
    user_data_path = os.path.join(os.getcwd(), "playwright_data")
    query = "+".join(sorted(query.split()))

    print(f"Launching Chromium browser for Phase 1. Data directory: {user_data_path}")

    browser = None
    page = None

    phase1_items = []

    try:
        browser = playwright.chromium.launch_persistent_context(
            user_data_dir=user_data_path,
            channel="chrome",
            headless=False,
            no_viewport=True,
        )
        page = browser.new_page()

        print("SCRAPING PRODUCT LISTINGS (using 'li' selector)")
        print(f'https://duckduckgo.com/?origin=funnel_home_google&t=h_&q={query}&ia=shopping&iax=shopping')
        page.goto(f'https://duckduckgo.com/?origin=funnel_home_google&t=h_&q={query}&ia=shopping&iax=shopping', timeout=60000)

        time.sleep(1)

        previous_count = 0
        scroll_attempts = 0
        MAX_SCROLL_ATTEMPTS = 20

        while scroll_attempts < MAX_SCROLL_ATTEMPTS:
            scroll_attempts += 1
            current_count = page.locator('li').count()
            print(f"Scroll Attempt {scroll_attempts}: Current li count: {current_count}")

            if (current_count >= NUM_ITEMS + 39 and previous_count != 0) or (current_count == previous_count and current_count > 0):
                print("Target count reached or no new elements found. Stopping scroll.")
                break

            previous_count = current_count

            print("Scrolling down...")
            page.mouse.wheel(0, 5000)

            time.sleep(1)
        
        if scroll_attempts >= MAX_SCROLL_ATTEMPTS:
             print(f"Reached maximum scroll attempts ({MAX_SCROLL_ATTEMPTS}).")

        vacancies = page.locator('li')
        li_elements = vacancies.element_handles()
        
        print(f"Found {len(li_elements)} total <li> elements after scrolling.")

        base_url = page.url
        
        for element_handle in li_elements:
            item_data = {
                'ProductName': 'N/A',
                'Price': 'N/A',
                'ReviewCount': 'N/A',
                'Brand': 'N/A',
                'URL': 'N/A'
            }
            h2_handle = element_handle.query_selector("h2")
            item_data['ProductName'] = h2_handle.inner_text().strip() if h2_handle else 'N/A'
            
            a_handle = element_handle.query_selector("a")
            if a_handle:
                href = a_handle.get_attribute("href")
                if href:
                    item_data['URL'] = urljoin(base_url, href)
            
            price_found = False
            review_found = False
            brand_found = False

            information_divs = element_handle.query_selector_all("div")

            for information_div in information_divs:
                span_elements = information_div.query_selector_all("span")

                if not span_elements:
                    continue

                for span in span_elements:
                    text = span.inner_text().strip()
                    if text.startswith('$') and not price_found:
                        item_data['Price'] = text.replace('$', '').strip()
                        price_found = True
                    elif text.startswith('(') and text.endswith(')') and not review_found:
                        review_text = text.replace('(', '').replace(')', '').strip()
                        if review_text.endswith('K'):
                            item_data['ReviewCount'] = int((float(review_text[:-1]) * 1000))
                        else:
                            item_data['ReviewCount'] = int(review_text)
                        review_found = True
                    elif not brand_found and check_valid_brand_name(text):
                        item_data['Brand'] = text.strip()
                        brand_found = True

            if item_data['ProductName'] != 'N/A' and item_data['Price'] != 'N/A':
                phase1_items.append(item_data)

        print(f"Finished Phase 1. {len(phase1_items)} unique potentially valid items extracted.")
        
    except PlaywrightTimeoutError as pte:
        print(f"Timeout during Phase 1: {pte}")
    except Exception as e:
        print(f"Error during Phase 1: {e}")
    finally:
        if browser:
            browser.close()
            print("Phase 1 browser closed.")

    return phase1_items

if __name__ == '__main__':
    with sync_playwright() as playwright:
        query = input("Enter your search query: ")
        NUM_ITEMS = 500
        phase1_results = scrape_duckduckgo(playwright, query, NUM_ITEMS)

        if phase1_results:
             print(f"\nPhase 1 completed. {len(phase1_results)} items found.")
             columns = ['ProductName', 'Price', 'ReviewCount', 'Brand', 'URL']
             df_phase1 = pd.DataFrame(phase1_results, columns=columns)

             output_file_phase1 = "ITEMS_phase1.csv"
             df_phase1.to_csv(output_file_phase1, index=False)
             print(f"Phase 1 results saved to {output_file_phase1}")
        else:
             print("\nNo initial items found in Phase 1. Exiting.")