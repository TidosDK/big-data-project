from confluent_kafka import Producer, KafkaException
import requests, json, time, signal, sys, os
from datetime import datetime
from dateutil.relativedelta import relativedelta

limit = 5000
offset = 0
STATE_FILE = "/data/last_date2.json"

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "energy_data")
API = os.getenv("API_URL", "https://api.energidataservice.dk/dataset/PrivateConsumptionHeatingHour")
POLL_SECS = int(os.getenv("POLL_SECONDS", "0")) # Changable


energynet_producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "compression.type": "gzip",
    "linger.ms": 50,
    "batch.num.messages": 1000,
})

def on_delivery(err, msg):
    if err:
        print(f"delivery failed: {err}", file=sys.stderr)
        
def handle_shutdown(*_):
    global run
    run = False

run = True
signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def read_state():
    if not os.path.exists(STATE_FILE):
        return datetime(2024, 1, 1)
    with open(STATE_FILE, "r") as f:
        state = json.load(f)
        return datetime.fromisoformat(state["last_date"])
    
def write_state(date):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_date": date.strftime("%Y-%m-%dT%H:%M")}, f)


all_records_of_month = []

start_date = read_state()
end_date = start_date + relativedelta(months=1) #Adds a month to the start date
start_date = start_date + relativedelta(minute=1)
start_str = start_date.strftime("%Y-%m-%dT%H:%M") 
end_str = end_date.strftime("%Y-%m-%dT%H:%M")

#Getting all the data for the entire month
while run:
    try:
        api_url = f"{API}?start={start_str}&end={end_str}&limit={limit}&offset={offset}"
        response = requests.get(api_url, timeout=20)
        response.raise_for_status()
        payload = response.json()

        if isinstance(payload, list):
            records = payload
        else:
            records = payload.get("records") or payload.get("result", {}).get("records")
            if not isinstance(records, list):
                records = []

            
        if not records:
            print("No more records, stopping pagination.")
            break

        
        all_records_of_month.extend(records)    
        
        if len(records) < limit:
            break
        
        offset += limit
        
    except Exception as e:
        print(f"HTTP/produce error: {e}", file=sys.stderr)

    time.sleep(POLL_SECS)

if all_records_of_month:
    write_state(end_date)
else:
    print("No data retrieved, will not update state file.")


all_records_of_month.reverse()

index = 0
try: 
    while index < len(all_records_of_month): #For uploading data to topic
        try:
            batch = all_records_of_month[index:index + limit]
            
            for record in batch:
                energynet_producer.produce(TOPIC, value=json.dumps(record), on_delivery=on_delivery)
                
            energynet_producer.poll(0.1)
            if index % (limit * 5) == 0:
                energynet_producer.flush()

            print(f"Produced {len(batch)} record(s) to {TOPIC}")

            index += limit
        except Exception as e:
            print(f"HTTP/produce error: {e}", file=sys.stderr)

        time.sleep(POLL_SECS)
finally:
    try:
        energynet_producer.flush(10)
    except KafkaException as e:
        print(f"flush error: {e}", file=sys.stderr)