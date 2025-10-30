from asyncio import timeout

from confluent_kafka import Producer
import requests
from numpy.core.records import record
from scipy.constants import value

import json
import time

def dr(err, msg):
    if err:
        print("delivery failed:", err)

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'compression.type': 'gzip',
    'linger.ms': 50,
    'batch.num.messages': 1000,
    'debug': 'broker,protocol'
})

api_url = 'https://api.energidataservice.dk/dataset/PrivateConsumptionHeatingHour/download?format=json&limit=100'
topic = 'energy_data'

while True:
    response = requests.get(api_url, timeout=20)
    if response.ok:
        payload = response.json()
        records = (payload.get('records')
                   or payload.get('result', {}).get('records')
                   or (payload if isinstance(payload, list) else []))
        if not records:
            producer.produce(topic, key='batch', value=json.dumps(payload), callback=dr)
        else:
            for record in records:
                key = str(record.get('id') or record.get('_id') or '')
                producer.produce(topic, key=key, value=json.dumps(record), callback=dr)
        producer.poll(0)
    else:
        print(f"Fetch error {response.status_code} from {api_url}")

    producer.flush(5)
    time.sleep(20)