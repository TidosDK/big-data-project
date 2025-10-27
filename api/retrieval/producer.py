from confluent_kafka import Producer
import requests
import json
import time

producer = Producer({'bootstrap.servers': 'localhost:9092'})

api_url = 'https://api.energidataservice.dk/dataset/PrivateConsumptionHeatingHour/download?format=json&limit=100'

while True:
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        producer.produce('api', key='key', value=json.dumps(data))
    else:
        print(f'Failed to get data from {api_url}: Status code: {response.status_code}')

    producer.flush()
    time.sleep(20)