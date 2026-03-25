import requests
import logging
import time
import os
import pandas as pd
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


# Transforming the data to add calculated fields and ensure correct data types
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

    return df

# Function to save the transformed DataFrame in parquet format partitioned by year and month
def save_partitioned_parquet(
    df: pd.DataFrame,
    output_base: str = "output/orders",
    date_column: str = "order_date"
) -> None:
    """
    Stores a DataFrame in parquet format partitioned by year and month.

    Args:
        df (pd.DataFrame): Input DataFrame
        output_base (str): Base route for output (e.g. "output/orders")    
        date_column (str): Partition column (default: "order_date")
    """

    # Creating partitions
    for (year, month), group in df.groupby(["order_year", "order_month"]):

        partition_path = os.path.join(
            output_base,
            f"order_year={year}",
            f"order_month={month}"
        )

        os.makedirs(partition_path, exist_ok=True)

        file_path = os.path.join(partition_path, "data.parquet")

        group.to_parquet(file_path, index=False)

        print(f"Saved: {file_path}")

# Main pipeline function to orchestrate the ETL process
def main():
    """Main pipeline function."""
    logger.info("=" * 50)
    logger.info("API Pipeline - Starting...")
    logger.info("=" * 50)
    
    try:
        # Extract
        raw_data = fetch_data_with_retry(rows=5000)
        
        # Transform
        df = transform_data(raw_data)
        
        if df.empty:
            logger.error("There is no data to save after transformation. Exiting pipeline.")
            return
        
        # Load
        save_partitioned_parquet(df)
        
        logger.info("=" * 50)
        logger.info("Pipeline completed successfully!")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Pipeline has failed: {e}")
        raise

if __name__ == "__main__":
    main()