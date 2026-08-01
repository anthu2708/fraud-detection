import csv
import json
import os
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "transactions.raw"
DATA_PATH = os.getenv("DATA_PATH", "data/creditcard_test.csv")
TX_PER_SECOND = int(os.getenv("TX_PER_SECOND", "200"))


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    delay = 1.0 / TX_PER_SECOND
    count = 0
    loop = 0

    print(f"Replaying {DATA_PATH} → {TOPIC} at {TX_PER_SECOND} tx/s (looping)")

    while True:
        loop += 1
        with open(DATA_PATH) as f:
            for row in csv.DictReader(f):
                tx = {k: float(v) for k, v in row.items() if k != "Class"}
                tx["transaction_id"] = str(uuid.uuid4())
                tx["sent_at"] = datetime.now(timezone.utc).isoformat()

                producer.produce(TOPIC, key=tx["transaction_id"], value=json.dumps(tx))
                producer.poll(0)
                count += 1

                if count % 10_000 == 0:
                    print(f"[producer] sent {count} transactions (loop {loop})")

                time.sleep(delay)

        producer.flush()
        print(f"[producer] loop {loop} complete — restarting")


if __name__ == "__main__":
    main()
