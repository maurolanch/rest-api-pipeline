import requests
import logging
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
    
response = fetch_data()
print(response)