from kafka import KafkaConsumer
import json
import pprint

consumer = KafkaConsumer(
    'wildfire-topic',
    bootstrap_servers='localhost:9094',
    group_id='wildfire-consumer-group',
    auto_offset_reset='earliest',  
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

pp = pprint.PrettyPrinter(indent=2)

print("[Consumer] Waiting for wildfire messages...")

try:
    for fire in consumer:
        print("[Consumer] Received:")
        pp.pprint(fire.value)
except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    consumer.close()
