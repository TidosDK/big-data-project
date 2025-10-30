from confluent_kafka import Consumer, KafkaError

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['energy_data'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print(f"Reached end of topic {msg.topic()} [{msg.partition()}]. Skipping.")
        else:
            print(f"Error on topic {msg.topic()} [{msg.partition()}]: {msg.error()}")
    else:
        print(f"received message: {msg.value().decode('utf-8')}")
consumer.close()