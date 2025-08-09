import time
import json
import requests
from kafka import KafkaProducer
import yaml
from datetime import datetime

try:
    with open('configs/kafka.yaml') as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    print("Error: Config file not found. Using fallback settings.")
    config = {
        'kafka': {
            'bootstrap_servers': 'localhost:9094',
            'topic': 'earthquake-topic'
        }
    }

USGS_URL = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
RETRY_COUNT = 3
RETRY_DELAY = 5  

def fetch_earthquake_data():
    for attempt in range(RETRY_COUNT):
        try:
            response = requests.get(USGS_URL, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < RETRY_COUNT - 1:
                time.sleep(RETRY_DELAY)
    raise Exception("Failed to fetch earthquake data after retries")

def create_producer():
    """Create Kafka producer with enhanced configuration"""
    return KafkaProducer(
        bootstrap_servers=config['kafka']['bootstrap_servers'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',  
        retries=3,  
        linger_ms=100,
        compression_type='gzip'
    )

def process_earthquakes(raw_data):
    """Transform raw earthquake data into our format"""
    earthquakes = []
    for eq in raw_data['features']:
        try:
            earthquakes.append({
                "id": eq["id"],
                "magnitude": float(eq["properties"]["mag"]),
                "location": eq["properties"]["place"],
                "time": datetime.fromtimestamp(eq["properties"]["time"]/1000).isoformat(),
                "longitude": float(eq["geometry"]["coordinates"][0]),
                "latitude": float(eq["geometry"]["coordinates"][1]),
                "depth": float(eq["geometry"]["coordinates"][2]),
                "processed_at": datetime.utcnow().isoformat()
            })
        except (KeyError, TypeError) as e:
            print(f"Error processing earthquake data: {str(e)}")
    return earthquakes

def main():
    print(f"Starting earthquake producer at {datetime.utcnow().isoformat()}")
    
    # Fetch and process data
    raw_data = fetch_earthquake_data()
    earthquakes = process_earthquakes(raw_data)
    
    # Produce to Kafka
    producer = create_producer()
    try:
        for idx, earthquake in enumerate(earthquakes):
            print(f"[{idx+1}/{len(earthquakes)}] Sending: {earthquake['id']} ({earthquake['magnitude']}M)")
            producer.send(
                topic=config['kafka'].get('topic', 'earthquake-topic'),
                value=earthquake
            )
            time.sleep(3)
            
        producer.flush()
        print("All earthquakes sent successfully")
    except Exception as e:
        print(f"Error sending messages: {str(e)}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()