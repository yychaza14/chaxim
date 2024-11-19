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

class DataSaver:
    """A class responsible for saving data in different formats."""
    
    def __init__(self, base_directory: Union[str, Path] = 'pb2b'):
        """
        Initialize the DataSaver with a base directory for storing files.
        
        Args:
            base_directory (Union[str, Path]): Base directory for storing all data files
        """
        self.base_dir = Path(base_directory)
        self._setup_directories()
        self._setup_logging()

    def _setup_directories(self) -> None:
        """Create necessary directories for storing different types of data."""
        # Create base directory if it doesn't exist
        self.base_dir.mkdir(exist_ok=True)
        
        # Create subdirectories for different data types
        self.logs_dir = self.base_dir / 'logs'
        self.excel_dir = self.base_dir / 'excel'
        self.json_dir = self.base_dir / 'json'
        
        for directory in [self.logs_dir, self.excel_dir, self.json_dir]:
            directory.mkdir(exist_ok=True)

    def _setup_logging(self) -> None:
        """Set up logging configuration."""
        log_file = self.logs_dir / f'data_saver_{datetime.now().strftime("%Y%m%d")}.log'
        
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
        self.logger.info(f"DataSaver logging initialized. Log file: {log_file}")

    def _generate_filename(self, prefix: str, extension: str) -> str:
        """
        Generate a filename with timestamp.
        
        Args:
            prefix (str): Prefix for the filename
            extension (str): File extension
            
        Returns:
            str: Generated filename
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        return f"{prefix}_{timestamp}.{extension}"

    def save_to_excel(
        self, 
        data: List[Dict],
        filename_prefix: str = "data",
        sheet_name: str = "Sheet1"
    ) -> Optional[Path]:
        """
        Save data to Excel file.
        
        Args:
            data (List[Dict]): Data to save
            filename_prefix (str): Prefix for the filename
            sheet_name (str): Name of the Excel sheet
            
        Returns:
            Optional[Path]: Path to saved file if successful, None otherwise
        """
        filename = self.excel_dir / self._generate_filename(filename_prefix, "xlsx")
        
        try:
            df = pd.DataFrame(data)
            df.to_excel(filename, sheet_name=sheet_name, index=False)
            self.logger.info(f"Data successfully saved to Excel: {filename}")
            return filename
        except Exception as e:
            self.logger.error(f"Error saving to Excel: {str(e)}")
            return None

    def save_to_json(
        self, 
        data: Dict,
        filename_prefix: str = "data",
        indent: int = 2
    ) -> Optional[Path]:
        """
        Save data to JSON file.
        
        Args:
            data (Dict): Data to save
            filename_prefix (str): Prefix for the filename
            indent (int): Number of spaces for JSON indentation
            
        Returns:
            Optional[Path]: Path to saved file if successful, None otherwise
        """
        filename = self.json_dir / self._generate_filename(filename_prefix, "json")
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=indent)
            self.logger.info(f"Data successfully saved to JSON: {filename}")
            return filename
        except Exception as e:
            self.logger.error(f"Error saving to JSON: {str(e)}")
            return None

    def save_data(
        self, 
        data: Dict[str, Union[bool, List[Dict], str]],
        excel_prefix: str = "bybit_data",
        json_prefix: str = "bybit_raw_data"
    ) -> Dict[str, Optional[Path]]:
        """
        Save data to both Excel and JSON formats.
        
        Args:
            data (Dict): Data to save
            excel_prefix (str): Prefix for Excel filename
            json_prefix (str): Prefix for JSON filename
            
        Returns:
            Dict[str, Optional[Path]]: Paths to saved files
        """
        results = {
            'excel_path': None,
            'json_path': None
        }
        
        if data.get("success") and data.get("data"):
            results['excel_path'] = self.save_to_excel(
                data["data"],
                filename_prefix=excel_prefix
            )
            results['json_path'] = self.save_to_json(
                data,
                filename_prefix=json_prefix
            )
        
        return results

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
        
        for directory in [self.logs_dir, self.screenshots_dir]:
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

    def close(self):
        """Clean up resources."""
        if self.driver:
            self.driver.quit()
            self.logger.info("Browser session closed")

def main():
    scraper = BybitScraper(headless=True)
    data_saver = DataSaver()

    try:
        result = scraper.get_p2p_listings(
            token="USDT",
            fiat="NGN",
            action_type="1"
        )

        if result["success"]:
            # Save data using the DataSaver class
            saved_files = data_saver.save_data(result)

            # Print summary
            print(f"Time of scraping: {result['metadata']['timestamp']}")

            if result["data"]:
                print(f"\nPrice Range:")
                print(f"Lowest price: {result['data'][0]['price']} {result['metadata']['fiat']}")
                print(f"Highest price: {result['data'][-1]['price']} {result['metadata']['fiat']}")
                
                if saved_files['excel_path']:
                    print(f"\nData saved to Excel: {saved_files['excel_path']}")
                if saved_files['json_path']:
                    print(f"Data saved to JSON: {saved_files['json_path']}")
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
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import os
import sys

class BinanceP2PAPI:
    """Binance P2P API client with enhanced features and error handling."""
    
    BASE_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    
    def __init__(self, data_dir: str = "binance_data"):
        """
        Initialize the Binance P2P API client.
        
        Args:
            data_dir: Base directory for storing data files
        """
        self.base_dir = data_dir
        self._setup_directories()
        self._setup_logging()
        self._setup_session()
        
    def _setup_directories(self) -> None:
        """Create necessary directory structure for data storage."""
        # Define directories relative to the base data directory
        self.data_dir = Path(self.base_dir)
        self.data_dir.mkdir(exist_ok=True)
        
        self.directories = {
            'logs': self.data_dir / 'logs',
            'excel': self.data_dir / 'excel',
            'json': self.data_dir / 'json'
        }
        
        # Create directories with proper permissions
        for directory in self.directories.values():
            directory.mkdir(parents=True, exist_ok=True)
            # Ensure directory has write permissions
            if os.name != 'nt':  # Skip on Windows
                try:
                    os.chmod(directory, 0o777)
                except Exception as e:
                    self.logger.warning(f"Could not set permissions for {directory}: {e}")
            
    def _setup_logging(self) -> None:
        """Configure logging with GitHub Actions-compatible setup."""
        log_file = self.directories['logs'] / f'binance_p2p_{datetime.now().strftime("%Y%m%d")}.log'
        
        # Ensure log directory exists and is writable
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Configure logging to both file and console
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, mode='a'),
                logging.StreamHandler(sys.stdout)  # Explicitly log to stdout for GitHub Actions
            ]
        )
        self.logger = logging.getLogger('BinanceP2PAPI')
        
    def _setup_session(self) -> None:
        """Configure requests session with retries and headers."""
        self.session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
        self.session.headers.update({
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/json',
            'Origin': 'https://p2p.binance.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def search_advertisements(
        self,
        asset: str = "USDT",
        fiat: str = "XAF",
        trade_type: str = "BUY",
        payment_method: Optional[str] = None,
        page: int = 1,
        rows: int = 10
    ) -> Dict:
        """Search P2P advertisements on Binance."""
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
        
        self.logger.info(f"Searching advertisements: {asset}/{fiat} - {trade_type}")
        
        try:
            response = self.session.post(self.BASE_URL, json=payload)
            response.raise_for_status()
            
            data = response.json()
            if not isinstance(data, dict) or "data" not in data:
                raise ValueError("Invalid response format from Binance API")
                
            return {
                "success": True,
                "data": data.get("data", []),
                "metadata": {
                    "asset": asset,
                    "fiat": fiat,
                    "trade_type": trade_type,
                    "timestamp": datetime.now().isoformat(),
                    "total_rows": len(data.get("data", []))
                }
            }
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {str(e)}"
            self.logger.error(error_msg)
            return {"success": False, "code": "request_failed", "message": error_msg}
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self.logger.error(error_msg)
            return {"success": False, "code": "unexpected_error", "message": error_msg}

    def save_data(self, response: Dict, prefix: str = "") -> Dict[str, Path]:
        """Save API response data to files."""
        if not response.get("success"):
            self.logger.error("Cannot save unsuccessful response")
            return {}
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_filename = f"{prefix}_{timestamp}" if prefix else timestamp
        saved_files = {}
        
        try:
            # Save JSON with explicit encoding
            json_path = self.directories['json'] / f"{base_filename}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(response, f, indent=2, ensure_ascii=False)
            saved_files['json'] = json_path
            
            # Save Excel if we have data
            if response.get("data"):
                excel_data = []
                for ad in response["data"]:
                    excel_data.append({
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "price": ad["adv"]["price"],
                        "amount": ad["adv"]["surplusAmount"],
                        "merchant": ad["advertiser"].get("nickName", "Unknown"),
                        "payment_methods": ", ".join(method["identifier"] for method in ad["adv"]["tradeMethods"])
                    })
                
                df = pd.DataFrame(excel_data)
                excel_path = self.directories['excel'] / f"{base_filename}.xlsx"
                df.to_excel(excel_path, index=False)
                saved_files['excel'] = excel_path
            
            # Ensure files are readable by GitHub Actions
            for file_path in saved_files.values():
                if os.name != 'nt':  # Skip on Windows
                    os.chmod(file_path, 0o666)
            
            self.logger.info(f"Data saved successfully: {', '.join(str(p) for p in saved_files.values())}")
            return saved_files
            
        except Exception as e:
            self.logger.error(f"Error saving data: {str(e)}")
            return {}

def setup_github_actions_env() -> Optional[Path]:
    """Configure environment for GitHub Actions with proper artifact handling."""
    if os.getenv('GITHUB_ACTIONS') == 'true':
        # Use GITHUB_WORKSPACE as base directory
        workspace = Path(os.getenv('GITHUB_WORKSPACE', '.'))
        
        # Create artifact directory within workspace
        artifact_dir = workspace / 'artifacts'
        artifact_dir.mkdir(parents=True, exist_ok=True)
        
        # Ensure directory is writable
        if os.name != 'nt':  # Skip on Windows
            os.chmod(artifact_dir, 0o777)
        
        return artifact_dir
    return None

def mains():
    try:
        artifact_dir = setup_github_actions_env()
        base_dir = str(artifact_dir) if artifact_dir else "binance_data"
        
        api = BinanceP2PAPI(data_dir=base_dir)
        
        pairs = [
            {"asset": "USDT", "fiat": "XAF"},
            {"asset": "USDT", "fiat": "EUR"}
        ]
        
        for pair in pairs:
            response = api.search_advertisements(
                asset=pair["asset"],
                fiat=pair["fiat"],
                trade_type="BUY",
                rows=20
            )
            
            if response["success"]:
                saved_files = api.save_data(
                    response,
                    prefix=f"binance_p2p_{pair['asset']}_{pair['fiat']}"
                )
                
                if saved_files:
                    print(f"\nData saved for {pair['asset']}/{pair['fiat']}:")
                    for file_type, path in saved_files.items():
                        print(f"{file_type.upper()}: {path}")
                    
                    if response["data"]:
                        prices = [float(ad["adv"]["price"]) for ad in response["data"]]
                        print(f"\nPrice Summary ({pair['fiat']}):")
                        print(f"Lowest: {min(prices)}")
                        print(f"Highest: {max(prices)}")
                        print(f"Average: {sum(prices)/len(prices):.2f}")
            else:
                print(f"Error fetching {pair['asset']}/{pair['fiat']}: {response.get('message')}")
                
    except Exception as e:
        print(f"Critical error in main execution: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    mains()


