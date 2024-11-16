# Binance P2P API Client

A Python client for interacting with Binance's P2P trading API. This tool allows you to search and retrieve P2P trading advertisements from Binance's platform, with support for various filtering options and data export capabilities.

## Features

- Search P2P advertisements with customizable filters
- Support for different assets and fiat currencies
- Payment method filtering
- Automatic data export to Excel
- Comprehensive error handling and logging

## Requirements

- Python 3.7+
- Required packages:
  - requests
  - pandas
  - openpyxl

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yychaza14/binance-p2p-api.git
cd binance-p2p-api
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

## Usage

Basic usage example:

```python
from binance_p2p_api import BinanceP2PAPI

api = BinanceP2PAPI()
response = api.search_advertisements(
    asset="USDT",
    fiat="XAF",
    trade_type="BUY",
    rows=4
)

if response["success"]:
    print(f"Found {len(response['data'])} advertisements")
```

## Configuration

The following parameters can be customized when searching for advertisements:

- `asset`: Cryptocurrency asset (default: "USDT")
- `fiat`: Fiat currency (default: "XAF")
- `trade_type`: "BUY" or "SELL" (default: "BUY")
- `payment_method`: Specific payment method (optional)
- `page`: Page number for pagination (default: 1)
- `rows`: Number of records per page (default: 10)

## Output

The script saves data to an Excel file with the following format:
- Filename: `binance_p2p_data_YYYYMMDD_HHMMSS.xlsx`
- Columns: Timestamp, Price

## Error Handling

The client includes comprehensive error handling for:
- API request failures
- Invalid responses
- Data export issues

All errors are logged using Python's built-in logging module.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Disclaimer

This is an unofficial tool and is not affiliated with, endorsed by, or connected to Binance.
