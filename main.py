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
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

class BybitScraper:
    def __init__(self, headless: bool = True, timeout: int = 30):
        """Initialize the Bybit P2P scraper."""
        self.timeout = timeout
        self._setup_directories()
        self._setup_logging()
        self.driver = self._initialize_driver(headless)

    def _setup_directories(self):
        """Set up necessary directories for storing data and logs."""
        # Create base directory for all data
        self.data_dir = Path('pb2b')
        self.data_dir.mkdir(exist_ok=True)
        
        # Create subdirectories for different types of data
        self.logs_dir = self.data_dir / 'logs'
        self.screenshots_dir = self.data_dir / 'screenshots'
        self.excel_dir = self.data_dir / 'excel'
        self.json_dir = self.data_dir / 'json'
        
        for directory in [self.logs_dir, self.screenshots_dir, self.excel_dir, self.json_dir]:
            directory.mkdir(exist_ok=True)

    def _setup_logging(self):
        """Set up logging configuration."""
        log_file = self.logs_dir / f'bybit_scraper_{datetime.now().strftime("%Y%m%d")}.log'
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Logging initialized. Log file: {log_file}")

    def _initialize_driver(self, headless: bool) -> webdriver.Chrome:
        """Initialize and configure the Chrome WebDriver."""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument('--headless=new')

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

        service = Service()
        return webdriver.Chrome(service=service, options=chrome_options)

    def _clean_price(self, price_text: str) -> Optional[float]:
        """Clean and convert price text to float."""
        try:
            if not price_text or price_text.isspace():
                return None

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

                WebDriverWait(self.driver, self.timeout).until(
                    EC.presence_of_element_located((By.TAG_NAME, "tbody"))
                )

                time.sleep(5)

                # Take screenshot with organized path
                screenshot_path = self.screenshots_dir / f"bybit_page_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
                self.driver.save_screenshot(str(screenshot_path))
                self.logger.info(f"Screenshot saved as '{screenshot_path}'")

                listings = []
                rows = self.driver.find_elements(By.CSS_SELECTOR, "tbody tr")

                for row in rows:
                    try:
                        price_element = row.find_element(By.CSS_SELECTOR, "td:nth-child(2)")
                        price_text = price_element.text.strip()

                        cleaned_price = self._clean_price(price_text)
                        if cleaned_price is not None:
                            listing_data = {
                                'price': cleaned_price,
                                'timestamp': datetime.now().isoformat(),
                                **self._extract_additional_info(row)
                            }
                            listings.append(listing_data)
                    except (NoSuchElementException, Exception) as e:
                        self.logger.warning(f"Error parsing row: {str(e)}")
                        continue

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

    def save_to_excel(self, data: List[Dict]):
        """Save the scraped data to an Excel file in the excel directory."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = self.excel_dir / f"bybit_data_{timestamp}.xlsx"
        
        try:
            df = pd.DataFrame(data)
            df.to_excel(filename, index=False)
            self.logger.info(f"Data successfully saved to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving to Excel: {str(e)}")

    def save_to_json(self, result: Dict):
        """Save the raw data to a JSON file in the json directory."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = self.json_dir / f"bybit_raw_data_{timestamp}.json"
        
        try:
            with open(filename, "w") as f:
                json.dump(result, f, indent=2)
            self.logger.info(f"Raw data saved to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving to JSON: {str(e)}")

    def close(self):
        """Clean up resources."""
        if self.driver:
            self.driver.quit()
            self.logger.info("Browser session closed")

def main():
    scraper = BybitScraper(headless=True)

    try:
        result = scraper.get_p2p_listings(
            token="USDT",
            fiat="NGN",
            action_type="1"
        )

        if result["success"]:
            # Save data to both Excel and JSON
            scraper.save_to_json(result)
            scraper.save_to_excel(result["data"])

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
import pandas as pd
import json
import time
from datetime import datetime
import logging
import re
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Union, Tuple


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BinanceP2PAPI:
    """
    A client for interacting with Binance's P2P trading API.
    
    This class provides methods to search and retrieve P2P trading advertisements
    from Binance's platform, with support for various filtering options.
    """
    
    BASE_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"

    def __init__(self):
        """Initialize the API client with required headers."""
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/json',
            'Origin': 'https://p2p.binance.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def search_advertisements(
        self,
        asset: str = "USDT",
        fiat: str = "XAF",
        trade_type: str = "BUY",
        payment_method: Optional[str] = None,
        page: int = 1,
        rows: int = 10
    ) -> Dict[str, Any]:
        """
        Search P2P advertisements on Binance.
        
        Args:
            asset (str): Cryptocurrency asset to trade (default: "USDT")
            fiat (str): Fiat currency to trade against (default: "XAF")
            trade_type (str): Type of trade - "BUY" or "SELL" (default: "BUY")
            payment_method (Optional[str]): Specific payment method to filter by (default: None)
            page (int): Page number for pagination (default: 1)
            rows (int): Number of records per page (default: 10)
            
        Returns:
            Dict[str, Any]: API response containing advertisement data or error information
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
            logger.error(f"API request failed: {str(e)}")
            return {
                "success": False,
                "code": "request_failed",
                "message": str(e),
                "data": None
            }

def ensure_data_directory(base_dir: str = "p2b") -> str:
    """
    Ensure the data directory exists and return its path.
    
    Args:
        base_dir (str): Base directory name for storing data files
        
    Returns:
        str: Path to the data directory
    """
    # Get the current working directory
    current_dir = Path.cwd()
    
    # Create the data directory path
    data_dir = current_dir / base_dir
    
    # Create the directory if it doesn't exist
    data_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Using data directory: {data_dir}")
    return str(data_dir)

def save_to_excel(data: List[List[Any]], filename_prefix: str = "binance_p2p_data") -> str:
    """
    Save the provided data to an Excel file with timestamp in the p2b directory.
    
    Args:
        data (List[List[Any]]): List of rows containing timestamp and price data
        filename_prefix (str): Prefix for the output filename
        
    Returns:
        str: The name of the created file
    """
    try:
        # Ensure the data directory exists
        data_dir = ensure_data_directory()
        
        # Create DataFrame
        df = pd.DataFrame(data, columns=["Timestamp", "Price"])
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{filename_prefix}_{timestamp}.xlsx"
        
        # Create full file path
        file_path = os.path.join(data_dir, filename)
        
        # Save the file
        df.to_excel(file_path, index=False)
        logger.info(f"Data successfully saved to {file_path}")
        
        return file_path
    except Exception as e:
        logger.error(f"Failed to save data to Excel: {str(e)}")
        raise

def mains():
    """Main function to demonstrate API usage."""
    try:
        api = BinanceP2PAPI()
        logger.info("Fetching P2P advertisements...")
        
        response = api.search_advertisements(
            asset="USDT",
            fiat="XAF",
            trade_type="BUY",
            rows=4
        )

        if response.get("success"):
            data = [
                [datetime.now().strftime("%Y-%m-%d %H:%M:%S"), ad["adv"]["price"]]
                for ad in response["data"]
            ]
            
            filename = save_to_excel(data)
            logger.info(f"Data successfully saved to {filename}")
        else:
            logger.error(f"API request failed: {response.get('message', 'Unknown error')}")
            
    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")

if __name__ == "__main__":
    mains()
    
