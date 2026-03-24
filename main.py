import requests
import logging
from config import API_TOKEN, API_BASE_URL

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def fetch_data(dataset_type: str = 'ecommerce' , rows: int =1000) -> dict:
    """Fetch data from the API."""
    url = f"{API_BASE_URL}/datasets.php"
    params = {
        'type': 'dataset_type',
        'rows': rows,
        'token': API_TOKEN
    }

    logger.info(f"Fetching rows: {rows} of type: {dataset_type} from API.")

    response = requests.get(url, params=params, timeout=30)

    response.raise_for_status()  # Raise an error for bad status codes

    data = response.json()

    logger.info(f"Successfully fetched {len(data.get('tables', {}).get('orders', []))} orders")
    
    return data
    