import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# --- CONFIGURATION (Docker Network standard setup) ---
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
KAFKA_TOPIC = 'ecommerce-clicks'

# Kafka Producer Initialize karein
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"⚡ PulseStream Producer Started... Sending data to topic: {KAFKA_TOPIC}")

# Dummy Data Helpers
actions = ['view_item', 'add_to_cart', 'purchase']
products = ['PROD_001', 'PROD_002', 'PROD_003', 'PROD_004', 'PROD_005']
countries = ['PK', 'US', 'GB', 'AE', 'DE']

try:
    while True:
        # 1. Normal E-Commerce Click Data Generate karein
        data = {
            'timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'user_id': f"USR_{random.randint(1000, 9999)}",
            'ip_address': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
            'action': random.choices(actions, weights=[0.7, 0.2, 0.1])[0],
            'product_id': random.choice(products),
            'amount': round(random.uniform(10.0, 500.0), 2),
            'country': random.choice(countries)
        }
        
        # 2. ANOMALY INJECTION (Fake fraud behavior generate karne ke liye)
        if random.random() < 0.05:  # 5% chance of anomaly
            data['ip_address'] = '999.999.999.999'  # Intentional Fake Anomaly IP
            data['amount'] = 9999.99
            data['action'] = 'purchase'

        # 3. Kafka par data send karein
        producer.send(KAFKA_TOPIC, value=data)
        print(f"📤 Sent: {data['user_id']} | {data['action']} | {data['ip_address']}")
        
        # Har click ke darmiyan random break (0.5 se 2 seconds)
        time.sleep(random.uniform(0.5, 2.0))

except KeyboardInterrupt:
    print("\n🛑 Producer stopped manually.")
finally:
    producer.close()