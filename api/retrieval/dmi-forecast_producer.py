from confluent_kafka import Producer, KafkaException
import requests, json, time, signal, sys, os
from datetime import datetime
from dateutil.relativedelta import relativedelta

STATE_FILE = "/data/dmi_last_date.json"

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "meterological_observations")
API_BASE = os.getenv("API_URL", "https://dmigw.govcloud.dk/v2/metObs/collections/observation/items")
API_KEY = os.getenv("API_KEY")

PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "2000"))
POLL_SECS = float(os.getenv("POLL_SECONDS", "5"))
WAIT_FOR_NEXT_DATA_CHECK = int(os.getenv("WAIT_FOR_NEXT_DATA_CHECK", "1800"))
WAIT_AFTER_ERROR = int(os.getenv("WAIT_AFTER_ERROR", "300"))

dmi_producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "compression.type": "gzip",
    "linger.ms": 50,
    "batch.num.messages": 1000,
    "enable.idempotence": True,
    "acks": "all",
    "retries": 1000000,
    "delivery.timeout.ms": 120000,
})

delivered = 0
failed = 0

def on_delivery(err, msg):
    global delivered, failed
    if err:
        failed += 1
        print(f"delivery failed: {err}", file=sys.stderr)
    else:
        delivered += 1

run = True

def handle_shutdown(*_):
    global run
    run = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def read_state():
    if not os.path.exists(STATE_FILE):
        return datetime(2024, 1, 1, 0, 0)
    with open(STATE_FILE, "r") as f:
        state = json.load(f)
        return datetime.fromisoformat(state["last_date"])

def write_state(date):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"last_date": date.strftime("%Y-%m-%dT%H:%M")}, f)
    os.replace(tmp, STATE_FILE)

def safe_produce(record: dict):
    key = record.get("id")
    if not key:
        props = record.get("properties", {}) if isinstance(record, dict) else {}
        key = f"{props.get('stationId','unknown')}:{props.get('parameterId','unknown')}:{props.get('observed','unknown')}"
    value = json.dumps(record)
    while run:
        try:
            dmi_producer.produce(
                TOPIC,
                key=str(key),
                value=value,
                on_delivery=on_delivery
            )
            dmi_producer.poll(0)
            return
        except BufferError:
            dmi_producer.poll(1.0)

def fetch_and_upload_data():
    global delivered, failed
    delivered = 0
    failed = 0

    offset = 0
    saw_any = False

    start_date = read_state()
    end_date = start_date + relativedelta(days=1)
    start_date = start_date + relativedelta(minutes=1)

    start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")

    while run:
        try:
            api_url = (f"{API_BASE}?api-key={API_KEY}&datetime={start_str}Z%2F{end_str}Z&limit={PAGE_LIMIT}&offset={offset}")

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
                if not saw_any:
                    print(
                        f"No data between {start_str} and {end_str}, "
                        f"waiting {WAIT_FOR_NEXT_DATA_CHECK} seconds before retry"
                    )
                    time.sleep(WAIT_FOR_NEXT_DATA_CHECK)
                    return
                else:
                    print("No more records, stopping pagination.")
                    break

            saw_any = True

            for record in records:
                safe_produce(record)

            offset += PAGE_LIMIT
            time.sleep(POLL_SECS)

        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.HTTPError,
                json.JSONDecodeError) as e:
            print(f"HTTP/fetch error: {e}", file=sys.stderr)
            time.sleep(WAIT_AFTER_ERROR)
            continue

    try:
        dmi_producer.flush(30)
    except KafkaException as e:
        print(f"flush error: {e}", file=sys.stderr)
        failed += 1

    if saw_any and failed == 0:
        write_state(end_date)
        print(f"Checkpoint advanced to {end_date.isoformat()} (delivered={delivered})")
    else:
        print(
            f"Checkpoint NOT advanced (saw_any={saw_any}, failed={failed}, delivered={delivered}). "
            f"Will retry window after {WAIT_AFTER_ERROR}s.",
            file=sys.stderr
        )
        time.sleep(WAIT_AFTER_ERROR)

while run:
    fetch_and_upload_data()

try:
    dmi_producer.flush(10)
except Exception:
    pass
