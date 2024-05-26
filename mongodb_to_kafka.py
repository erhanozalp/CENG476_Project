from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient
import json
from urllib.parse import quote_plus
import pandas as pd
from bson import json_util

uri = "mongodb+srv://{username}:{encoded_password}@{cluster_url}/{dbname}?retryWrites=true&w=majority"

client = MongoClient(uri)
db = client['username']
collection = db['eksisozluk']

# Kafka Producer oluşturma
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v, default=json_util.default).encode('utf-8'))

# MongoDB'den verileri al ve Kafka'ya gönder (sadece 100 veri)
count = 0
max_count = 100

for document in collection.find().limit(max_count):
    producer.send('mongodb_topic', value=document)
    count += 1
    if count >= max_count:
        break

producer.flush()
print("All documents sent to Kafka")

# Kafka Consumer oluşturma
consumer = KafkaConsumer(
    'mongodb_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='csv_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'), object_hook=json_util.object_hook)
)

# Verileri depolamak için bir liste oluşturma
data = []

# Kafka'dan verileri çekme
for message in consumer:
    data.append(message.value)
    if len(data) >= max_count:
        break

# Verileri DataFrame'e dönüştürme ve CSV'ye kaydetme
df = pd.DataFrame(data)
df.to_csv('output.csv', index=False)
print("Final data saved to output.csv")
