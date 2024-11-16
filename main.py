from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import json
import time
from datetime import datetime
import logging
import re
from typing import Dict, List, Optional, Union

class BybitScraper:
    def __init__(self, headless: bool = True, timeout: int = 30):
        """Initialize the Bybit P2P scraper."""
        self.timeout = timeout
        self._setup_logging()
        self.driver = self._initialize_driver(headless)

    def _setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler('bybit_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _initialize_driver(self, headless: bool) -> webdriver.Chrome:
        """Initialize and configure the Chrome WebDriver."""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless=new')  # Updated for newer Chrome versions

        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/120.0.0.0 Safari/537.36")

        service = Service()  # Let Selenium handle driver path
        return webdriver.Chrome(service=service, options=chrome_options)

    def _clean_price(self, price_text: str) -> Optional[float]:
        """
        Clean and convert price text to float.
        Returns None if the price is invalid.
        """
        try:
            if not price_text or price_text.isspace():
                return None

            # Remove currency symbol and any non-numeric characters except decimal point
            price_str = re.sub(r'[^\d.]', '', price_text.split('\n')[0])
            return float(price_str) if price_str else None
        except Exception as e:
            self.logger.warning(f"Error cleaning price {price_text}: {str(e)}")
            return None

    def _extract_additional_info(self, row) -> Dict:
        """Extract additional information from the row."""
        try:
            available_amount = row.find_element(By.CSS_SELECTOR, "td:nth-child(3)").text.strip()
            payment_methods = row.find_element(By.CSS_SELECTOR, "td:nth-child(4)").text.strip()
            merchant_name = row.find_element(By.CSS_SELECTOR, "td:nth-child(5)").text.strip()

            return {
                "available_amount": available_amount,
                "payment_methods": payment_methods,
                "merchant_name": merchant_name
            }
        except NoSuchElementException as e:
            self.logger.warning(f"Could not extract additional info: {str(e)}")
            return {}

    def get_p2p_listings(
        self,
        token: str = "USDT",
        fiat: str = "NGN",
        action_type: str = "1",
        max_retries: int = 10
    ) -> Dict[str, Union[bool, List[Dict], str]]:
        """Scrape P2P listings from Bybit website."""
        url = f"https://www.bybit.com/fiat/trade/otc?actionType={action_type}&token={token}&fiat={fiat}"

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Attempt {attempt + 1}/{max_retries}: Loading {url}")
                self.driver.get(url)

                # Wait for the main content to load
                WebDriverWait(self.driver, self.timeout).until(
                    EC.presence_of_element_located((By.TAG_NAME, "tbody"))
                )

                # Additional wait for dynamic content
                time.sleep(5)

                # Take screenshot for debugging
                screenshot_path = f"bybit_page_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                self.driver.save_screenshot(screenshot_path)
                self.logger.info(f"Screenshot saved as '{screenshot_path}'")

                # Extract data from the table
                listings = []
                rows = self.driver.find_elements(By.CSS_SELECTOR, "tbody tr")

                for row in rows:
                    try:
                        # Get the price cell
                        price_element = row.find_element(By.CSS_SELECTOR, "td:nth-child(2)")
                        price_text = price_element.text.strip()

                        # Clean and validate the price
                        cleaned_price = self._clean_price(price_text)
                        if cleaned_price is not None:
                            listing_data = {
                                'price': cleaned_price,
                                'timestamp': datetime.now().isoformat(),
                                **self._extract_additional_info(row)
                            }
                            listings.append(listing_data)
                    except NoSuchElementException:
                        continue
                    except Exception as e:
                        self.logger.warning(f"Error parsing row: {str(e)}")
                        continue

                # Filter out empty prices and sort by price
                valid_listings = [l for l in listings if l['price'] is not None]
                valid_listings.sort(key=lambda x: x['price'])

                return {
                    "success": True,
                    "data": valid_listings,
                    "metadata": {
                        "token": token,
                        "fiat": fiat,
                        "action_type": "buy" if action_type == "1" else "sell",
                        "timestamp": datetime.now().isoformat(),
                        "total_rows_found": len(rows),
                        "valid_listings_found": len(valid_listings)
                    }
                }

            except TimeoutException:
                self.logger.error(f"Timeout waiting for content on attempt {attempt + 1}")
                if attempt == max_retries - 1:
                    return {
                        "success": False,
                        "data": None,
                        "message": "Timeout error: Page failed to load after multiple attempts"
                    }
                time.sleep(5)

            except Exception as e:
                self.logger.error(f"Unexpected error: {str(e)}")
                return {
                    "success": False,
                    "data": None,
                    "message": f"Error: {str(e)}"
                }

    def close(self):
        """Clean up resources."""
        if self.driver:
            self.driver.quit()
            self.logger.info("Browser session closed")

def save_to_excel(data: List[Dict], filename: str = f"bybit_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"):
    """Save the scraped data to an Excel file."""
    try:
        df = pd.DataFrame(data)
        df.to_excel(filename, index=False)
        print(f"Data successfully saved to {filename}")
    except Exception as e:
        print(f"Error saving to Excel: {str(e)}")

def main():
    scraper = BybitScraper(headless=True)

    try:
        result = scraper.get_p2p_listings(
            token="USDT",
            fiat="NGN",
            action_type="1"
        )

        if result["success"]:
            # Save raw data to JSON for backup
            with open("bybit_raw_data.json", "w") as f:
                json.dump(result, f, indent=2)

            # Save processed data to Excel
            save_to_excel(result["data"])

            # Print summary
            print(f"Time of scraping: {result['metadata']['timestamp']}")

            if result["data"]:
                print(f"\nPrice Range:")
                print(f"Lowest price: {result['data'][0]['price']} {result['metadata']['fiat']}")
                print(f"Highest price: {result['data'][-1]['price']} {result['metadata']['fiat']}")
        else:
            print("Error:", result["message"])

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()



import requests
import json
from datetime import datetime
import pandas as pd



class BinanceP2PAPI:
    BASE_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/json',
            'Origin': 'https://p2p.binance.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def search_advertisements(self,
                            asset: str = "USDT",
                            fiat: str = "XAF",
                            trade_type: str = "BUY",
                            payment_method: str = None,
                            page: int = 1,
                            rows: int = 10):
          """
          Search P2P advertisements on Binance and return raw JSON response
          """
          payload = {
              "asset": asset,
              "fiat": fiat,
              "merchantCheck": True,
              "page": page,
              "payTypes": [payment_method] if payment_method else [],
              "publisherType": None,
              "rows": rows,
              "tradeType": trade_type
          }

          try:
              response = self.session.post(self.BASE_URL, json=payload)
              response.raise_for_status()
              return response.json()

          except requests.exceptions.RequestException as e:
              return {
                  "success": False,
                  "code": "request_failed",
                  "message": str(e),
                  "data": None
              }

# Example usage
def main():
    api = BinanceP2PAPI()

    response = api.search_advertisements(
        asset="USDT",
        fiat="XAF",
        trade_type="BUY",
        rows=4
    )
    # ... (rest of your code)

    if response["success"]:

        data = []
        for ad in response["data"]:
            price = ad["adv"]["price"]
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data.append([current_time, price])

        df = pd.DataFrame(data, columns=["Timestamp", "Price"])
        df.to_excel(f"binance_p2p_data{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", index=False)
        print("Data saved to Excel file.")
    else:
        print("Error:", response["message"])
if __name__ == "__main__":
    main()
    
