import os
from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()

API_TOKEN = os.getenv('API_TOKEN')
API_BASE_URL = os.getenv('API_BASE_URL', 'https://iansaura.com/api')

if not API_TOKEN:
    raise ValueError("API_TOKEN not found. Please create a .env file with your API_TOKEN.")