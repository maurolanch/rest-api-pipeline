import requests
import logging
import time
import json
import os
import pandas as pd
from datetime import datetime
from requests.exceptions import Timeout, HTTPError, RequestException
from config import API_TOKEN, API_BASE_URL, EMAIL

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def fetch_data(dataset_type: str = 'ecommerce' , rows: int =1000) -> dict:
    """Fetch data from the API."""
    url = f"{API_BASE_URL}/datasets.php"
    params = {
        'email': EMAIL,
        'key': API_TOKEN,
        'type': dataset_type,
        'rows': rows
    }

    response = requests.get(url, params=params, timeout=30)

    response.raise_for_status()

    data = response.json()

    logger.info(f"Successfully fetched {len(data.get('tables', {}).get('orders', []))} orders")
    
    return data
    

def fetch_data_with_retry(dataset_type='ecommerce', rows=1000, retries=3):
    for attempt in range(retries):
        try:
            logger.info(f"Attempt {attempt + 1}")

            return fetch_data(dataset_type, rows)

        except Timeout:
            logger.warning(f"[Attempt {attempt + 1}/{retries}] Timeout occurred")

        except HTTPError as e:
            status = e.response.status_code if e.response else None

            if 400 <= status < 500:
                logger.error(f"Client error {status}")
                raise

            logger.warning(f"Server error {status}")

        except RequestException as e:
            logger.warning(f"Network error: {e}")

        # backoff (1,2,4...)
        if attempt < retries - 1:
            wait = 2 ** attempt
            logger.info(f"[Attempt {attempt + 1}/{retries}] Retrying in {wait}s...")
            time.sleep(wait)

    raise Exception("Max retries exceeded")


def validate_data(data: dict):
    if "tables" not in data:
        raise ValueError("Missing 'tables' key in response")

    for table, records in data["tables"].items():
        if not isinstance(records, list):
            logger.warning(f"Table {table} is not a list")

        if len(records) == 0:
            logger.warning(f"Table {table} is empty")


def save_raw_data(data: dict, folder: str = "data/raw"):
    os.makedirs(folder, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = f"{folder}/dataset_{timestamp}.json"

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    logger.info(f"Raw data saved to {filepath}")


def inspect_data(data: dict):
    print("Top-level keys:", data.keys())

    tables = data.get("tables", {})
    print("\nTables disponibles:", tables.keys())

    for table_name, records in tables.items():
        print(f"\nTabla: {table_name}")
        print(f"Cantidad de registros: {len(records)}")

        if records:
            print("Ejemplo registro:")
            print(records[0])

def transform_data(data: dict) -> dict:
    logger.info("Transforming data...")

    # Extracting orders table
    orders = data.get("tables", {}).get("orders", [])
    df = pd.DataFrame(orders)

    if df.empty:
        logger.warning("No orders data to transform")
        return {}
    
    # Transforming order fields
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')

    # Addind calculated fields
    df['order_month'] = df['order_date'].dt.to_period('M').astype(str)
    df['order_year'] = df['order_date'].dt.year
    df['is_high_value'] = df['total_amount'] > 100
    df['day_of_week'] = df['order_date'].dt.day_name()

    # Validating transformed data
    invalid_totals = df['total_amount'].isnull().any()
    if invalid_totals:
        logger.warning(f"{invalid_totals} orders have invalid total_amount")
    
    logger.info(f"Transformed {len(df)} orders with new fields")

    return data

data = fetch_data_with_retry()
transform_data(data)
