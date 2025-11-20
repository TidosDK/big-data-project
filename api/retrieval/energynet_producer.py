from confluent_kafka import Producer, KafkaException
import requests, json, time, signal, sys, os
from datetime import datetime
from dateutil.relativedelta import relativedelta

limit = 100000
offset = 0
STATE_FILE = "/data/last_date.json"

failed_messages = []

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "energy_data")
API = os.getenv("API_URL", "https://api.energidataservice.dk/dataset/PrivateConsumptionHeatingHour")
POLL_SECS = int(os.getenv("POLL_SECONDS", "5")) # Changable

WAIT_FOR_NEXT_DATA_CHECK = 1800 #If no data between dates then wait 30 minutes
WAIT_AFTER_ERROR = 300 #If ERROR, wait 5 minutes


energynet_producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "compression.type": "gzip",
    "linger.ms": 50,
    "batch.num.messages": 1000,
})

def on_delivery(err, msg):
    if err:
        print(f"delivery failed: {err}", file=sys.stderr)
        failed_messages.append(msg.value())
        
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

    
    


def fetch_and_upload_data():
    offset = 0
    payload = None
    all_records_between_dates = []

    start_date = read_state()
    end_date = start_date + relativedelta(weeks=1) #Adds a week to the start date
    start_date = start_date + relativedelta(minutes=1) #Adds a minute
    start_str = start_date.strftime("%Y-%m-%dT%H:%M") 
    end_str = end_date.strftime("%Y-%m-%dT%H:%M")

    total_amount_of_records = None

    #Getting all the data between start- and end date
    while run:
        records = [] #Resetting records
        try:
            api_url = f"{API}?start={start_str}&end={end_str}&limit={limit}&offset={offset}"
            response = requests.get(api_url, timeout=20)
            response.raise_for_status()
            payload = response.json()
            
            if isinstance(payload, dict):
                if total_amount_of_records is None:
                    total_amount_of_records = (
                        payload.get("total") or 
                        payload.get("result", {}).get("total") or
                        0 #Ensures that if theres an error with getting the the total it will retry further down
                        )
                    
            if isinstance(payload, dict) and "error" in payload:
                raise RuntimeError(f"API error: {payload['error']}")
    

            if isinstance(payload, list):
                records = payload
            else:
                records = payload.get("records") or payload.get("result", {}).get("records")
                if not isinstance(records, list):
                    records = []
            
            if total_amount_of_records == 0 and not records:
                print(f"There's no data between the dates, waiting {WAIT_FOR_NEXT_DATA_CHECK} seconds before retry")
                time.sleep(WAIT_FOR_NEXT_DATA_CHECK)
                continue

                
            if not records and len(all_records_between_dates) == total_amount_of_records:
                print("No more records, stopping pagination.")
                break
            elif not records and len(all_records_between_dates) != total_amount_of_records:
                print("Didn't get all records, trying again...")
                offset = 0
                all_records_between_dates.clear()
                continue 
            
        except (requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            json.JSONDecodeError) as e:
            print(f"HTTP/fetching error: {e}", file=sys.stderr)
            time.sleep(WAIT_AFTER_ERROR)
            continue #Retries if error
        
        all_records_between_dates.extend(records)   
        
        offset += limit

        time.sleep(POLL_SECS)

    if all_records_between_dates:
        """
        If the last data point is before the current end date, 
        then we make sure it's starts from the date that this data set ended on
        """
        
        end_date = datetime.fromisoformat(all_records_between_dates[0]["TimeUTC"]) 
        write_state(end_date)
    else:
        print("No data retrieved, will not update state file.")
    
    total_amount_of_records = None
    all_records_between_dates.reverse()

    #For uploading data to the topic
    index = 0
    try: 
        while index < len(all_records_between_dates): #For uploading data to topic
            try:
                batch = all_records_between_dates[index:index + limit]
                
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
            
            
while run:
    fetch_and_upload_data()