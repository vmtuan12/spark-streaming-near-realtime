from kafka import KafkaProducer
import json
import datetime
import pandas as pd

producer = KafkaProducer(bootstrap_servers=['localhost:9091', 'localhost:9092', 'localhost:9093'], 
                        value_serializer=lambda x: json.dumps(x).encode('utf-8'))
topic = 'url'

df = pd.read_csv("./test.csv", index_col=False)

data = [item for item in df.to_dict(orient='index').values()]

for item in data:
    msg = item
    producer.send(topic, value=msg)
    producer.flush()
    print(msg)
