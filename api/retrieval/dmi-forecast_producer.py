from confluent_kafka import Producer, KafkaException
import requests, json, time, signal, sys, os
from datetime import datetime
from dateutil.relativedelta import relativedelta

limit = 50000
offset = 0
STATE_FILE = "/data/dmi_last_date.json"

failed_messages = []

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "meterological_observations")
API_BASE = os.getenv("API_URL", "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items")
POLL_SECS = int(os.getenv("POLL_SECONDS", "5"))  # Changable

API_KEY = os.getenv("API_KEY")


WAIT_FOR_NEXT_DATA_CHECK = 1800
WAIT_AFTER_ERROR = 300

dmi_producer = Producer({
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
        return datetime(2024, 1, 1, 0, 0)
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
    end_date = start_date + relativedelta(days=1) # adds 1 day
    start_date = start_date + relativedelta(minutes=1)

    # DMI needs a Z at the end
    start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    while run:
        records = []
        try:
            api_url = f"{API_BASE}?api-key={API_KEY}&datetime={start_str}Z%2F{end_str}Z&limit={limit}&offset={offset}"
            # api_url += "&bbox=7,54,16,58&bbox-crs=https%3A%2F%2Fwww.opengis.net%2Fdef%2Fcrs%2FOGC%2F1.3%2FCRS84"

            response = requests.get(api_url, timeout=20)
            response.raise_for_status()
            payload = response.json()

            if isinstance(payload, dict):
                records = payload.get("features") or []
            elif isinstance(payload, list):
                records = payload
            else:
                records = []

            if not records:
                if not all_records_between_dates:
                    print(
                        f"No data between {start_str} and {end_str}, "
                        f"waiting {WAIT_FOR_NEXT_DATA_CHECK} seconds before retry"
                    )
                    time.sleep(WAIT_FOR_NEXT_DATA_CHECK)
                    continue
                else:
                    print("No more records, stopping pagination.")
                    break

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError,
                json.JSONDecodeError) as e:
            print(f"HTTP/fetching error: {e}", file=sys.stderr)
            time.sleep(WAIT_AFTER_ERROR)
            continue

        all_records_between_dates.extend(records)

        offset += limit

        time.sleep(POLL_SECS)

    if all_records_between_dates:
        write_state(end_date)
    else:
        print("No data retrieved, will not update state file.")

    index = 0
    all_records_between_dates.reverse()

    try:
        while index < len(all_records_between_dates):
            try:
                batch = all_records_between_dates[index:index + limit]

                for record in batch:
                    dmi_producer.produce(
                        TOPIC,
                        value=json.dumps(record),
                        on_delivery=on_delivery
                    )

                dmi_producer.poll(0.1)
                if index % (limit * 5) == 0:
                    dmi_producer.flush()

                print(f"Produced {len(batch)} record(s) to {TOPIC}")

                index += limit
            except Exception as e:
                print(f"HTTP/produce error: {e}", file=sys.stderr)

            time.sleep(POLL_SECS)
    finally:
        try:
            dmi_producer.flush(10)
        except KafkaException as e:
            print(f"flush error: {e}", file=sys.stderr)


while run:
    fetch_and_upload_data()
