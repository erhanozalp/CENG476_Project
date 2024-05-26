import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
import json
import os
from pymongo import MongoClient


# Kafka Producer oluşturma
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
i = 0

# CSV dosyalarının bulunduğu klasör yolu
csv_folder_path = 'eksisozlukDB'

# Tüm CSV dosyalarını okuma ve Kafka'ya gönderme
csv_files = [f for f in os.listdir(csv_folder_path) if f.endswith('.csv')]

for file in csv_files:
    file_path = os.path.join(csv_folder_path, file)
    data = pd.read_csv(file_path)
    for index, row in data.iterrows():
        producer.send('csv_topic', value=row.to_dict())
        print("producer for ",i)
        i = i+1

producer.flush()


# MongoDB Atlas bağlantısı
uri = "mongodb+srv://{username}:{encoded_password}@{cluster_url}/{dbname}?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri)
db = client['username']
collection = db['eksisozluk']



# Kafka Consumer oluşturma
consumer = KafkaConsumer(
    'csv_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='csv_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

i = 0
# Verileri MongoDB'ye yazma
for message in consumer:
    print("mesajları yazıyore ", i)
    i = i+1
    data = message.value
    collection.insert_one(data)
    print(f"Inserted: {data}")
