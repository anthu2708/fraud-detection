import csv
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "transactions.raw"
DATA_PATH = os.getenv("DATA_PATH", "data/creditcard_test.csv")
TX_PER_SECOND = int(os.getenv("TX_PER_SECOND", "10"))
CONTROL_PORT = int(os.getenv("CONTROL_PORT", "8002"))

_paused = False


class _ControlHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        global _paused
        if self.path == "/pause":
            _paused = True
        elif self.path == "/resume":
            _paused = False
        else:
            self.send_response(404); self.end_headers(); return
        self.send_response(200); self.end_headers()
        self.wfile.write(b"paused" if _paused else b"running")

    def do_GET(self):
        if self.path == "/status":
            self.send_response(200); self.end_headers()
            self.wfile.write(b"paused" if _paused else b"running")
        else:
            self.send_response(404); self.end_headers()

    def log_message(self, *_):
        pass


def _start_control() -> None:
    HTTPServer(("0.0.0.0", CONTROL_PORT), _ControlHandler).serve_forever()


def main():
    threading.Thread(target=_start_control, daemon=True).start()
    print(f"[producer] control server on :{CONTROL_PORT}")

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    delay = 1.0 / TX_PER_SECOND
    count = 0
    loop = 0

    print(f"Replaying {DATA_PATH} → {TOPIC} at {TX_PER_SECOND} tx/s (looping)")

    while True:
        loop += 1
        with open(DATA_PATH) as f:
            for row in csv.DictReader(f):
                while _paused:
                    time.sleep(0.1)

                tx = {k: float(v) for k, v in row.items() if k != "Class"}
                tx["transaction_id"] = str(uuid.uuid4())
                tx["sent_at"] = datetime.now(timezone.utc).isoformat()
                tx["source"] = "auto"

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
