import json
from kafka import KafkaConsumer

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'snmp'

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='snmp-worker-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"📥 [worker.py] Waiting for messages from Kafka topic '{TOPIC_NAME}'...")
    for message in consumer:
        data = message.value
        print(f"\n✅ [Consumer Received]")
        print(f"Device: {data.get('device')} ({data.get('ip')})")
        print(f"Metrics: {json.dumps(data.get('data'), indent=2)}")

if __name__ == '__main__':
    main()
