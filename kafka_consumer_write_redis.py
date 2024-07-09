import redis
from datetime import date
from kafka import KafkaConsumer, TopicPartition
from kafka.cluster import ClusterMetadata
import time
import json

"""
consume kafka then write to redis
"""

r = redis.Redis(host='localhost', port=6379, decode_responses=True)
key_prefix = "hot-url:"

def fetch_data():
    consumer = KafkaConsumer('url_processed',
                            group_id='group-test', value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                            bootstrap_servers=['localhost:9091', 'localhost:9092', 'localhost:9093'])
    
    for message in consumer:
        print(message.value)
        subscriber_id, label_count = message.value["subscriberid"], message.value["label_count"]
        r.set((key_prefix + subscriber_id), label_count)

fetch_data()
