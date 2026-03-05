#-- Active: 1770736190987@@127.0.0.1@5433@logdb
import json
from confluent_kafka import Consumer
import time
import psycopg2
from minio import Minio
import datetime
import uuid
from io import BytesIO
import time
import socket

def wait_for_service(host, port):
    while True:
        try:
            with socket.create_connection((host, port), timeout=5):
                print(f"{host}:{port} is ready!")
                break
        except OSError:
            print(f"Waiting for {host}:{port}...")
            time.sleep(2)

# Wait for dependencies
wait_for_service("kafka", 9092)
wait_for_service("minio", 9000)
wait_for_service("postgres", 5432)


consumer_config = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "logs-tracker-2",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

consumer.subscribe(["nginx-logs"])

print("Consumer is running and subscribed to nginx-logs topic")

# minio connection 
# insert into minio
minio_client= Minio(
    "minio:9000",
    access_key="minadmin",
    secret_key="admin2000",
    secure=False # because no HTTPS locally (its not puclic)
)
bucket_name="logs-version-2" # define bucket
        
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
        
# postgresql connection
DB_NAME = "logdb"
DB_USER = "logusr"
DB_PASS = "logpsd"
DB_HOST = "postgres"
DB_PORT = "5432"

conn = psycopg2.connect(database=DB_NAME, user= DB_USER, password= DB_PASS, host= DB_HOST, port=DB_PORT)
print("Database connected successfully")

cur = conn.cursor() # create a cursor
cur.execute("""
CREATE TABLE IF NOT EXISTS logs(
    ID SERIAL PRIMARY KEY,
    ip VARCHAR(50),
    time TIMESTAMP ,
    METHOD VARCHAR(50),
    ENDPOINT TEXT,
    STATUS INT  
            )
""")
conn.commit()
print("table was created succesfully")

# Consume kafka and write to postgresql
try:
    while True:
        msg = consumer.poll(2.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        value = msg.value().decode("utf-8")
        
        # Convert string to bytes and wrap in BytesIO
        data = BytesIO(value.encode("utf-8"))

        object_name = f"log-{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}-{uuid.uuid4()}.json"
        
        minio_client.put_object(
            bucket_name,
            object_name,
            data, 
            length=len(value.encode("utf-8")),
            content_type="data/json"
        )
        print("Raw logs are saved to MinIo ",object_name)


        logs = json.loads(value)
        # insert into postgresql
        cur.execute(
            "INSERT INTO logs (ip, time, method, endpoint, status) VALUES (%s, %s, %s, %s, %s)",
            (logs.get("ip"), logs.get("time"), logs.get("method"), logs.get("endpoint"), logs.get("status"))
        )
        conn.commit()
        print(f"Inserted log: {logs}")
        #print(f"Received log that has ip: {nginx_logs['ip']} : On {nginx_logs['time']} with the method {nginx_logs['method']}: and the endpoint {nginx_logs['endpoint']}: with status {nginx_logs['status']}")
except KeyboardInterrupt:
    print("\nStopping consumer")

finally:
    consumer.close()
    cur.close()
    conn.close()
    print("Consumer stopped and postgresql connection closed")

