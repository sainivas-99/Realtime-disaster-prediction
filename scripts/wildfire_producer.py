import time
import pandas as pd
import json
from kafka import KafkaProducer
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import yaml

load_dotenv()

NASA_API_KEY = os.getenv('wildfire_map_key')
if not NASA_API_KEY:
    raise ValueError("NASA_API_KEY not found in .env file")

try:
    with open('configs/kafka.yaml') as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    print("Error: Config file not found. Using fallback settings.")
    config = {
        'kafka': {
            'bootstrap_servers': 'localhost:9094',
            'topic': 'wildfire-topic'
        }
    }

KAFKA_BROKERS = config['kafka']['bootstrap_servers']
KAFKA_TOPIC = config['kafka']['topics']['wildfire']

# Constants
NASA_API_BASE = 'https://firms.modaps.eosdis.nasa.gov/api/area/csv'
SATELLITE = 'VIIRS_SNPP_NRT'
COVERAGE = 'world'
DAYS_BACK = 1

def get_nasa_url():
    date_str = (datetime.now() - timedelta(days=DAYS_BACK)).strftime('%Y-%m-%d')
    return f"{NASA_API_BASE}/{NASA_API_KEY}/{SATELLITE}/{COVERAGE}/{DAYS_BACK}/{date_str}"

def main():
    # Fetch data
    url = get_nasa_url()
    print(f"Fetching data from NASA FIRMS API...")
    fire_data = pd.read_csv(url)
    
    # Process data
    cols_to_drop = ['scan', 'track', 'version', 'instrument', 'satellite']
    fire_data = fire_data.drop(cols_to_drop, axis=1)
    fire_data['processed_at'] = datetime.now().isoformat()
    
    # Initialize producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )
    
    try:
        for i, fire in fire_data.head(10).iterrows():
            record = fire.to_dict()
            print(f"Sending fire at {record['latitude']},{record['longitude']}")
            producer.send(KAFKA_TOPIC, value=record)
            time.sleep(2)
            
        producer.flush()
        print("Successfully sent wildfire data")
    finally:
        producer.close()

if __name__ == "__main__":
    main()