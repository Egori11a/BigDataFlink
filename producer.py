import os
import json
import time
import pandas as pd
from kafka import KafkaProducer
import socket
import time

def wait_for_kafka(host='localhost', port=9092, timeout=60):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                print(f"[✓] Kafka доступен по адресу {host}:{port}")
                return
        except OSError:
            print(f"[...] Жду Kafka ({host}:{port})...")
            time.sleep(2)
    raise Exception("Kafka не отвечает")

wait_for_kafka()

DATA_DIR = 'исходные данные'
TOPIC_NAME = 'mock-topic'

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

csv_files = sorted([
    f for f in os.listdir(DATA_DIR)
    if f.lower().endswith('.csv') and 'mock_data' in f.lower()
])
#org.example.FlinkKafkaToPostgres
print(f"[INFO] Найдено файлов: {len(csv_files)}")
print(csv_files)

for filename in csv_files:
    full_path = os.path.join(DATA_DIR, filename)
    try:
        df = pd.read_csv(full_path)

        print(f"[+] Отправка строк из файла: {filename}")
        for index, row in df.iterrows():
            json_data = row.to_dict()
            producer.send(TOPIC_NAME, json_data)
            print(f"   → строка {index + 1} отправлена")
            time.sleep(0.01)

    except Exception as e:
        print(f"[ERROR] Ошибка при обработке {filename}: {e}")

producer.flush()
print("[✓] Все данные отправлены в Kafka.")
