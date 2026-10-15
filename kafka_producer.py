import time
import json
import csv
import urllib.request
from kafka import KafkaProducer

def stream_csv_data():
    # Nos conectamos a la fuente original
    url = "https://www.datos.gov.co/api/v3/views/62tk-nxj5/query.csv"
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    print("Iniciando simulación de streaming desde la API original...")
    
    # Petición a la web
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req) as response:
        # Leemos el archivo en vivo línea por línea
        lines = (line.decode('utf-8') for line in response)
        reader = csv.DictReader(lines)
        
        for row in reader:
            # Enviamos el dato a Kafka
            producer.send('presion_data', value=row)
            print(f"Enviado a Kafka: {row}")
            time.sleep(1) # Simulación de llegada de datos en tiempo real

if __name__ == "__main__":
    stream_csv_data()
