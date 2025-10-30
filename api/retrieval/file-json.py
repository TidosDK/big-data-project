from confluent_kafka import Producer
import requests
import json
import time
from datetime import datetime

producer = Producer({'bootstrap.servers': 'localhost:9092'})

api_url = 'https://api.energidataservice.dk/dataset/PrivateConsumptionHeatingHour/download?format=json&limit=100'

data = []

while True:
    response = requests.get(api_url)
    if response.status_code == 200:
        data.append(response.json())
    else:
        print(f"Failed to get data from {api_url} : Status code {response.status_code}")

    if len(data) > 0:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with open(f'api_data_{timestamp}.json', 'w') as file:
            json.dump(data, file)
        data = []
    time.sleep(20)