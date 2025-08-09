from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'earthquake-topic',
    bootstrap_servers = 'localhost:9094',
    enable_auto_commit = True,
    auto_offset_reset = 'earliest',
    value_deserializer = lambda v: json.loads(v.decode('utf-8'))
)

for msg in consumer:
    print(msg.value)