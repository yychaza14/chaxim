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
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import requests
from bs4 import BeautifulSoup

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
                    "BYBIT": valid_listings,
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

#binance data 
class BinanceP2PAPI:
    """Simplified Binance P2P API client that matches BybitScraper's return format."""
    
    BASE_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
    
    def __init__(self):
        """Initialize the Binance P2P API client with minimal setup."""
        self._setup_logging()
        self._setup_session()
        
    def _setup_logging(self) -> None:
        """Configure basic logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
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

    def get_p2p_listings(
        self,
        token: str = "USDT",
        fiat: str = "XAF",
        action_type: str = "1",  # "1" for buy, "0" for sell
        max_retries: int = 3,
        rows: int = 6
    ) -> Dict:
        """
        Get P2P listings from Binance API.
        Matches BybitScraper's return format.
        """
        trade_type = "BUY" if action_type == "1" else "SELL"
        
        payload = {
            "asset": token,
            "fiat": fiat,
            "merchantCheck": True,
            "page": 1,
            "payTypes": [],
            "publisherType": None,
            "rows": rows,
            "tradeType": trade_type
        }
        
        self.logger.info(f"Fetching {trade_type} listings for {token}/{fiat}")
        
        try:
            response = self.session.post(self.BASE_URL, json=payload)
            response.raise_for_status()
            
            data = response.json()
            if not isinstance(data, dict) or "data" not in data:
                raise ValueError("Invalid response format from Binance API")
            
            listings = []
            for ad in data["data"]:
                listing_data = {
                    'price': float(ad["adv"]["price"]),
                    'timestamp': datetime.now().isoformat(),
                    'available_amount': ad["adv"]["surplusAmount"],
                    'payment_methods': ", ".join(method["identifier"] for method in ad["adv"]["tradeMethods"]),
                    'merchant_name': ad["advertiser"].get("nickName", "Unknown")
                }
                listings.append(listing_data)
            
            # Sort listings by price like BybitScraper does
            listings.sort(key=lambda x: x['price'])
            
            return {
                "success": True,
                "BINANCE": listings,
            }
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {str(e)}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "data": None,
                "message": error_msg
            }
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self.logger.error(error_msg)
            return {
                "success": False,
                "data": None,
                "message": error_msg
            }

'''
class DataSaver:
    """A class responsible for saving data in different formats with continuous JSON storage."""
    
    def __init__(self, base_directory: Union[str, Path] = 'pb2b', json_filename: str = 'continuous_data.json'):
        """
        Initialize the DataSaver with a base directory and a continuous JSON file.
        
        Args:
            base_directory (Union[str, Path]): Base directory for storing all data files
            json_filename (str): Name of the continuous JSON file
        """
        self.base_dir = Path(base_directory)
        self._setup_directories()
        self._setup_logging()
        
        # Set up the continuous JSON file path
        self.continuous_json_path = self.json_dir / json_filename

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
        data: Dict[str, List[Dict]],
        filename_prefix: str = "data",
        sheet_name: str = "Sheet1"
    ) -> Optional[Path]:
        """
        Save data to Excel file.
        
        Args:
            data (Dict[str, List[Dict]]): Data to save with 'bybit' and 'binance' keys
            filename_prefix (str): Prefix for the filename
            sheet_name (str): Name of the Excel sheet
            
        Returns:
            Optional[Path]: Path to saved file if successful, None otherwise
        """
        filename = self.excel_dir / self._generate_filename(filename_prefix, "xlsx")
        
        try:
            # Create separate DataFrames for Bybit and Binance data
            dfs = []
            
            if 'bybit' in data:
                bybit_df = pd.DataFrame(data['bybit'])
                bybit_df['source'] = 'Bybit'
                dfs.append(bybit_df)
            
            if 'binance' in data:
                binance_df = pd.DataFrame(data['binance'])
                binance_df['source'] = 'Binance'
                dfs.append(binance_df)
            
            # Combine the DataFrames
            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)
                combined_df.to_excel(filename, sheet_name=sheet_name, index=False)
                self.logger.info(f"Data successfully saved to Excel: {filename}")
                return filename
            else:
                self.logger.warning("No data to save to Excel")
                return None
                
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
        Save data to a new JSON file.
        
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
                json.dump(data, f, indent=indent, ensure_ascii=False)
            self.logger.info(f"Data successfully saved to JSON: {filename}")
            return filename
        except Exception as e:
            self.logger.error(f"Error saving to JSON: {str(e)}")
            return None

    def save_to_continuous_json(
        self, 
        data: Dict,
        indent: int = 2
    ) -> Optional[Path]:
        """
        Safely append data to a continuous JSON file, creating it if it doesn't exist.
        
        Args:
            data (Dict): Data to save
            indent (int): Number of spaces for JSON indentation
            
        Returns:
            Optional[Path]: Path to saved file if successful, None otherwise
        """
        try:
            # Ensure the directory exists
            self.continuous_json_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Read existing data or initialize an empty list
            if self.continuous_json_path.exists() and os.path.getsize(self.continuous_json_path) > 0:
                try:
                    with open(self.continuous_json_path, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                except json.JSONDecodeError:
                    # If file is corrupted or empty, start with an empty list
                    existing_data = []
            else:
                existing_data = []
            
            # Add new data to the list
            existing_data.append(data)
            
            # Write back to the file
            with open(self.continuous_json_path, 'w', encoding='utf-8') as f:
                json.dump(existing_data, f, indent=indent, ensure_ascii=False)
            
            self.logger.info(f"Data successfully {'appended to' if existing_data else 'created in'} continuous JSON: {self.continuous_json_path}")
            return self.continuous_json_path
        except Exception as e:
            self.logger.error(f"Error saving to continuous JSON: {str(e)}")
            return None

    def save_data(
        self, 
        bybit_data: Dict[str, Union[bool, List[Dict], str]] = None,
        binance_data: Dict[str, Union[bool, List[Dict], str]] = None,
        excel_prefix: str = "p2p_data",
        json_prefix: str = "p2p_data"
    ) -> Dict[str, Optional[Path]]:
        """
        Save data from Bybit and Binance to both Excel and continuous JSON formats.
        
        Args:
            bybit_data (Dict): Bybit scraper data
            binance_data (Dict): Binance API data
            excel_prefix (str): Prefix for Excel filename
            json_prefix (str): Prefix for JSON filename
            
        Returns:
            Dict[str, Optional[Path]]: Paths to saved files
        """
        results = {
            'excel_path': None,
            'continuous_json_path': None,
            'json_path': None
        }
        
        # Prepare combined data dictionary
        combined_data = {
            "success": False,
            "timestamp": datetime.now().isoformat(),
            "bybit": [],
            "binance": []
        }
        
        # Add Bybit data if available
        if bybit_data and bybit_data.get("success") and bybit_data.get("BYBIT"):
            combined_data["success"] = True
            combined_data["bybit"] = bybit_data["BYBIT"]
        
        # Add Binance data if available
        if binance_data and binance_data.get("success") and binance_data.get("BINANCE"):
            combined_data["success"] = True
            combined_data["binance"] = binance_data["BINANCE"]
        
        # Save data if any source is successful
        if combined_data["success"]:
            results['excel_path'] = self.save_to_excel(
                {"bybit": combined_data["bybit"], "binance": combined_data["binance"]},
                filename_prefix=excel_prefix
            )
            results['continuous_json_path'] = self.save_to_continuous_json(combined_data)
            
            # Backward compatibility: also save to a separate JSON file
            results['json_path'] = self.save_to_json(
                combined_data,
                filename_prefix=json_prefix
            )
        
        return results


def get_exchange_rate(from_currency='EUR', to_currency='XAF'):
    try:
        url = f'https://www.xe.com/currencyconverter/convert/?Amount=1&From={from_currency}&To={to_currency}'
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Target the specific paragraph with the rate
        rate_element = soup.select_one('p.sc-63d8b7e3-1.bMdPIi')
        
        if rate_element:
            # Extract the whole number part
            whole_number = rate_element.contents[0].strip()
            
            # Extract the decimal part from the span
            decimal_span = rate_element.find('span', class_='faded-digits')
            decimal_part = decimal_span.text.strip() if decimal_span else ''
            
            # Combine and return full rate
            full_rate = f"{whole_number}{decimal_part}"
            return full_rate
        
        print("No rate found")
        return None
    
    except Exception as e:
        print(f"Error: {e}")
        return None



def main():
    scraper = BybitScraper(headless=True)
    binance = BinanceP2PAPI()
    data_saver = DataSaver()
    rate = get_exchange_rate()
    rate = float(rate)

    try:
        
        resultbyb = scraper.get_p2p_listings(
            token="USDT",
            fiat="NGN",
            action_type="1"
        )
        
        resultbnb = binance.get_p2p_listings(
            token="USDT",
            fiat="EUR",
            action_type="1"
        )
        # CFA
        rateE  = rate/100
        rateE1 = ((rateE*3) + rate) * resultbnb['BINANCE'][-1]['price']
        rateE2 = ((rateE*3.5) + rate) * resultbnb['BINANCE'][-1]['price']
        rateE3 = ((rateE*3.7) + rate) * resultbnb['BINANCE'][-1]['price']
        rateE4 = ((rateE*3.8) + rate) * resultbnb['BINANCE'][-1]['price']
        rateE5 = ((rateE*4) + rate) * resultbnb['BINANCE'][-1]['price']
        rateE6 = ((rateE*4.5) + rate) * resultbnb['BINANCE'][-1]['price']
        rateE7 = ((rateE*4.6) + rate) * resultbnb['BINANCE'][-1]['price']
        rateE8 = ((rateE*4.7) + rate) * resultbnb['BINANCE'][-1]['price']
        rateE9 = ((rateE*4.8) + rate ) * resultbnb['BINANCE'][-1]['price']
        
        # NAIRA
        rateN1 = (1000/rateE1) * resultbyb['BYBIT'][0]['price']
        rateN2 = (1000/rateE2) * resultbyb['BYBIT'][0]['price']
        rateN3 = (1000/rateE3) * resultbyb['BYBIT'][0]['price']
        rateN4 = (1000/rateE4) * resultbyb['BYBIT'][0]['price']
        rateN5 = (1000/rateE5) * resultbyb['BYBIT'][0]['price']
        rateN6 = (1000/rateE6) * resultbyb['BYBIT'][0]['price']
        rateN7 = (1000/rateE7) * resultbyb['BYBIT'][0]['price']
        rateN8 = (1000/rateE8) * resultbyb['BYBIT'][0]['price']
        rateN9 = (1000/rateE9) * resultbyb['BYBIT'][0]['price']
        rate_ngn = [{"by3":[rateE1, rateN1]}, {"by3.5":[rateE2, rateN2]}, {"by3.7":[rateE3, rateN3]}, {"by3.8":[rateE4, rateN4]}, {"by4":[rateE5, rateN5]}, {"by4.5":[rateE6, rateN6]}, {"by4.6":[rateE7, rateN7]}, {"by4.7":[rateE8, rateN8]}, {"by4.8":[rateE9,rateN9]}]
        
        resultbyb["RATE_NGN"] = rate_ngn

        # Save both Bybit and Binance data
        saved_files = data_saver.save_data(
            bybit_data=resultbyb, 
            binance_data=resultbnb
        )

        # Print summary
        print("\nP2P Listing Scraping Results:")
        print(f"Time of scraping: {datetime.now().isoformat()}")
        
        # Bybit results
        if resultbyb["success"] and resultbyb.get("BYBIT"):
            print("\nBybit Results:")
            print(f"Number of listings: {len(resultbyb['BYBIT'])}")
            print(f"Lowest Bybit price: {resultbyb['BYBIT'][0]['price']} NGN")
            print(f"Highest Bybit price: {resultbyb['BYBIT'][-1]['price']} NGN")
            
              # Add this block to print RATE_NGN with each value on a new line
            print("\nRate NGN Breakdown:")
            for rate_dict in resultbyb['RATE_NGN']:
                for key, value in rate_dict.items():
                    print(f"{key}: EUR rate = {value[0]}, NGN rate = {value[1]}")

        else:
            print("\nBybit scraping failed or returned no data")
        
        # Binance results
        if resultbnb["success"] and resultbnb.get("BINANCE"):
            print("\nBinance Results:")
            print(f"Number of listings: {len(resultbnb['BINANCE'])}")
            print(f"Lowest Binance price: {resultbnb['BINANCE'][0]['price']} XAF")
            print(f"Highest Binance price: {resultbnb['BINANCE'][-1]['price']} XAF")
        else:
            print("\nBinance API call failed or returned no data")

        # Saved files
        if saved_files['excel_path']:
            print(f"\nData saved to Excel: {saved_files['excel_path']}")
        if saved_files['json_path']:
            print(f"Data saved to JSON: {saved_files['json_path']}")
        if rate:
            print(f"Exchange Rate: {rate}")
        else:
            print("no rate from XE")

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        logging.error(f"Error in main execution: {str(e)}", exc_info=True)
    finally:
        scraper.close()

if __name__ == "__main__":
    main()'''

def get_exchange_rate(from_currency='EUR', to_currency='XAF'):
    try:
        url = f'https://www.xe.com/currencyconverter/convert/?Amount=1&From={from_currency}&To={to_currency}'
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Target the specific paragraph with the rate
        rate_element = soup.select_one('p.sc-63d8b7e3-1.bMdPIi')
        
        if rate_element:
            # Extract the whole number part
            whole_number = rate_element.contents[0].strip()
            
            # Extract the decimal part from the span
            decimal_span = rate_element.find('span', class_='faded-digits')
            decimal_part = decimal_span.text.strip() if decimal_span else ''
            
            # Combine and return full rate
            full_rate = f"{whole_number}{decimal_part}"
            return full_rate
        
        print("No rate found")
        return None
    
    except Exception as e:
        print(f"Error: {e}")
        return None

import sqlite3
import logging
from datetime import datetime
from typing import Dict, List, Union, Optional
from pathlib import Path
import json

class DataSaver:
    """A class responsible for saving and retrieving data using SQLite3."""
    
    def __init__(self, base_directory: Union[str, Path] = 'pb2b', db_filename: str = 'p2p_listings.db'):
        """
        Initialize the DataSaver with a base directory and database filename.
        
        Args:
            base_directory (Union[str, Path]): Base directory for storing database
            db_filename (str): Name of the SQLite database file
        """
        self.base_dir = Path(base_directory)
        self._setup_directories()
        self._setup_logging()
        
        # Set up the database path
        self.db_path = self.base_dir / db_filename
        
        # Create database connection and tables
        self._create_connection()
        self._create_tables()

    def _setup_directories(self) -> None:
        """Create necessary directories for storing data."""
        # Create base directory if it doesn't exist
        self.base_dir.mkdir(exist_ok=True)
        
        # Create subdirectories for different data types
        self.logs_dir = self.base_dir / 'logs'
        self.logs_dir.mkdir(exist_ok=True)

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

    def _create_connection(self) -> None:
        """Create a connection to the SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            self.logger.info(f"Connected to SQLite database: {self.db_path}")
        except sqlite3.Error as e:
            self.logger.error(f"Error connecting to SQLite database: {e}")
            raise

    def _create_tables(self) -> None:
        """Create necessary tables if they don't exist."""
        try:
            # Create Bybit listings table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS bybit_listings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    price REAL,
                    timestamp TEXT,
                    available_amount TEXT,
                    payment_methods TEXT,
                    merchant_name TEXT
                )
            ''')

            # Create Binance listings table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS binance_listings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    price REAL,
                    timestamp TEXT,
                    available_amount TEXT,
                    payment_methods TEXT,
                    merchant_name TEXT
                )
            ''')

            # Create exchange rates table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS exchange_rates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    from_currency TEXT,
                    to_currency TEXT,
                    rate REAL,
                    timestamp TEXT
                )
            ''')

            # Create metadata table for storing combined data metadata
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token TEXT,
                    fiat TEXT,
                    action_type TEXT,
                    total_bybit_listings INTEGER,
                    total_binance_listings INTEGER,
                    timestamp TEXT
                )
            ''')

            self.conn.commit()
            self.logger.info("Database tables created successfully")
        except sqlite3.Error as e:
            self.logger.error(f"Error creating tables: {e}")
            raise

    def save_data(
        self, 
        bybit_data: Dict[str, Union[bool, List[Dict], str]] = None,
        binance_data: Dict[str, Union[bool, List[Dict], str]] = None,
        exchange_rate: Optional[float] = None,
        from_currency: str = 'EUR',
        to_currency: str = 'XAF'
    ) -> Dict[str, Union[bool, str]]:
        """
        Save data from Bybit and Binance to SQLite database.
        
        Args:
            bybit_data (Dict): Bybit scraper data
            binance_data (Dict): Binance API data
            exchange_rate (Optional[float]): Exchange rate to save
            from_currency (str): Source currency for exchange rate
            to_currency (str): Target currency for exchange rate
            
        Returns:
            Dict[str, Union[bool, str]]: Result of save operation
        """
        try:
            # Start a transaction
            self.conn.execute('BEGIN TRANSACTION')

            # Save Bybit listings
            if bybit_data and bybit_data.get("success") and bybit_data.get("BYBIT"):
                for listing in bybit_data["BYBIT"]:
                    self.cursor.execute('''
                        INSERT INTO bybit_listings 
                        (price, timestamp, available_amount, payment_methods, merchant_name) 
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        listing.get('price'),
                        listing.get('timestamp'),
                        listing.get('available_amount'),
                        listing.get('payment_methods'),
                        listing.get('merchant_name')
                    ))

            # Save Binance listings
            if binance_data and binance_data.get("success") and binance_data.get("BINANCE"):
                for listing in binance_data["BINANCE"]:
                    self.cursor.execute('''
                        INSERT INTO binance_listings 
                        (price, timestamp, available_amount, payment_methods, merchant_name) 
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        listing.get('price'),
                        listing.get('timestamp'),
                        listing.get('available_amount'),
                        listing.get('payment_methods'),
                        listing.get('merchant_name')
                    ))

            # Save exchange rate if provided
            if exchange_rate is not None:
                self.cursor.execute('''
                    INSERT INTO exchange_rates 
                    (from_currency, to_currency, rate, timestamp) 
                    VALUES (?, ?, ?, ?)
                ''', (
                    from_currency,
                    to_currency,
                    exchange_rate,
                    datetime.now().isoformat()
                ))

            # Save metadata
            self.cursor.execute('''
                INSERT INTO metadata 
                (token, fiat, action_type, total_bybit_listings, total_binance_listings, timestamp) 
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                bybit_data.get('metadata', {}).get('token', ''),
                bybit_data.get('metadata', {}).get('fiat', ''),
                bybit_data.get('metadata', {}).get('action_type', ''),
                len(bybit_data.get("BYBIT", [])),
                len(binance_data.get("BINANCE", [])),
                datetime.now().isoformat()
            ))

            # Commit the transaction
            self.conn.commit()
            self.logger.info("Data successfully saved to SQLite database")
            
            return {
                "success": True,
                "message": "Data saved to SQLite database",
                "database_path": str(self.db_path)
            }

        except sqlite3.Error as e:
            # Rollback in case of error
            self.conn.rollback()
            self.logger.error(f"Error saving data to SQLite database: {e}")
            return {
                "success": False,
                "message": f"Error saving data: {str(e)}",
                "database_path": str(self.db_path)
            }

    def retrieve_listings(
        self, 
        source: str = 'bybit', 
        limit: int = 100, 
        order_by: str = 'price', 
        ascending: bool = True
    ) -> List[Dict]:
        """
        Retrieve listings from the database.
        
        Args:
            source (str): Source of listings ('bybit' or 'binance')
            limit (int): Maximum number of listings to retrieve
            order_by (str): Column to order by
            ascending (bool): Sort in ascending or descending order
        
        Returns:
            List[Dict]: Retrieved listings
        """
        try:
            table = 'bybit_listings' if source.lower() == 'bybit' else 'binance_listings'
            order_direction = 'ASC' if ascending else 'DESC'
            
            query = f'''
                SELECT * FROM {table} 
                ORDER BY {order_by} {order_direction} 
                LIMIT ?
            '''
            
            self.cursor.execute(query, (limit,))
            columns = [column[0] for column in self.cursor.description]
            
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        
        except sqlite3.Error as e:
            self.logger.error(f"Error retrieving listings: {e}")
            return []

    def retrieve_exchange_rates(
        self, 
        from_currency: Optional[str] = None, 
        to_currency: Optional[str] = None, 
        limit: int = 10
    ) -> List[Dict]:
        """
        Retrieve exchange rates from the database.
        
        Args:
            from_currency (Optional[str]): Source currency
            to_currency (Optional[str]): Target currency
            limit (int): Maximum number of rates to retrieve
        
        Returns:
            List[Dict]: Retrieved exchange rates
        """
        try:
            query = 'SELECT * FROM exchange_rates'
            conditions = []
            params = []
            
            if from_currency:
                conditions.append('from_currency = ?')
                params.append(from_currency)
            
            if to_currency:
                conditions.append('to_currency = ?')
                params.append(to_currency)
            
            if conditions:
                query += ' WHERE ' + ' AND '.join(conditions)
            
            query += ' ORDER BY timestamp DESC LIMIT ?'
            params.append(limit)
            
            self.cursor.execute(query, params)
            columns = [column[0] for column in self.cursor.description]
            
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
        
        except sqlite3.Error as e:
            self.logger.error(f"Error retrieving exchange rates: {e}")
            return []

    def close(self) -> None:
        """Close database connection."""
        try:
            if hasattr(self, 'conn'):
                self.conn.close()
                self.logger.info("SQLite database connection closed")
        except Exception as e:
            self.logger.error(f"Error closing database connection: {e}")

def main():
    scraper = BybitScraper(headless=True)
    binance = BinanceP2PAPI()
    
    # Use DataSaver with SQLite
    data_saver = DataSaver(base_directory='pb2b', db_filename='p2p_listings.db')

    try:
        # Fetch P2P listings
        resultbyb = scraper.get_p2p_listings(
            token="USDT",
            fiat="NGN",
            action_type="1"
        )
        
        resultbnb = binance.get_p2p_listings(
            token="USDT",
            fiat="EUR",
            action_type="1"
        )
        
        # Get exchange rate
        rate = get_exchange_rate()
        rate = float(rate) if rate else None

        # Save data to SQLite
        saved_result = data_saver.save_data(
            bybit_data=resultbyb, 
            binance_data=resultbnb,
            exchange_rate=rate
        )

        # Print results
        print("\nP2P Listing Scraping Results:")
        print(f"Time of scraping: {datetime.now().isoformat()}")
        
        # Print saved result
        print(f"\nDatabase Save Result: {saved_result['success']}")
        print(f"Database Path: {saved_result['database_path']}")

        # Demonstrate data retrieval
        print("\nRetrieving Bybit Listings:")
        bybit_listings = data_saver.retrieve_listings(source='bybit', limit=15)
        for listing in bybit_listings:
            print(listing)

        print("\nRetrieving Exchange Rates:")
        exchange_rates = data_saver.retrieve_exchange_rates()
        for rate in exchange_rates:
            print(rate)

    except Exception as e:
        print(f"Error in main execution: {str(e)}")
    finally:
        scraper.close()
        data_saver.close()

if __name__ == "__main__":
    main()
