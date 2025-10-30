from confluent_kafka import Producer
import requests, json, time

producer = Producer({
    "bootstrap.servers": "127.0.0.1:9092",
    "compression.type": "gzip",
    "linger.ms": 50,
    "batch.num.messages": 1000,
})

API   = "https://api.energidataservice.dk/dataset/PrivateConsumptionHeatingHour/download?format=json&limit=100"
TOPIC = "energy_data"

def on_delivery(err, msg):
    if err:
        print("Delivery failed:", err)

while True:
    try:
        r = requests.get(API, timeout=20)
        r.raise_for_status()

        records = r.json()
        if not isinstance(records, list):
            raise ValueError(f"Expected list from /download endpoint, got {type(records)}")

        for rec in records:
            producer.produce(TOPIC, value=json.dumps(rec), callback=on_delivery)
            producer.poll(0)


        for _ in range(10):
            producer.poll(0.1)

    except Exception as e:
        print("Fetch/produce error:", e)


    time.sleep(60)
