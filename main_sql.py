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
        bybit_listings = data_saver.retrieve_listings(source='bybit', limit=5)
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
