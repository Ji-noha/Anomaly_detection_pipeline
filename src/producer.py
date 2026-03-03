from confluent_kafka import Producer
import re
import json

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

log_pattern = re.compile(
    r'(?P<ip>\S+) .* '
    r'\[(?P<time>[^\]]+)\] '
    r'"(?P<method>\S+) (?P<endpoint>\S+) .*" '
    r'(?P<status>\d{3})'
)

with open("nginx/logs/access.log", "r") as f:
    for line in f:
        match = log_pattern.search(line)
        if not match:
            continue

        log_dict = match.groupdict()

        producer.produce(
            topic="nginx-logs",
            value=json.dumps(log_dict).encode("utf-8")
        )

producer.flush()
