import time
import json
from kafka import KafkaProducer
from pysnmp.hlapi import (
    getCmd, SnmpEngine, CommunityData, UdpTransportTarget,
    ContextData, ObjectIdentity, ObjectType
)

# Configuration for Kafka and Cisco Router 7200
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'snmp'
ROUTER_IP = '192.168.1.1'  # Update to match your Cisco Router IP in GNS3
COMMUNITY_STRING = 'public'

# Standard OIDs for Cisco Router
OIDS = {
    'sysUpTime': '1.3.6.1.2.1.1.3.0',
    'sysDescr': '1.3.6.1.2.1.1.1.0',
    'cpu_usage_5m': '1.3.6.1.4.1.9.9.109.1.1.1.1.5.1'  # Cisco CPU Utilization
}

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def get_snmp_value(oid):
    errorIndication, errorStatus, errorIndex, varBinds = next(
        getCmd(
            SnmpEngine(),
            CommunityData(COMMUNITY_STRING, mpModel=0),
            UdpTransportTarget((ROUTER_IP, 161), timeout=2, retries=2),
            ContextData(),
            ObjectType(ObjectIdentity(oid))
        )
    )
    if not errorIndication and not errorStatus:
        for varBind in varBinds:
            return str(varBind[1])
    return None

def main():
    print(f"🚀 [collector.py] Starting SNMP polling from Cisco Router ({ROUTER_IP})...")
    while True:
        metrics = {}
        for name, oid in OIDS.items():
            metrics[name] = get_snmp_value(oid)

        payload = {
            'device': 'Cisco Router 7200',
            'ip': ROUTER_IP,
            'timestamp': time.time(),
            'data': metrics
        }

        producer.send(TOPIC_NAME, value=payload)
        producer.flush()
        print(f"📤 [Produce -> Topic: {TOPIC_NAME}] {payload}")
        time.sleep(5)

if __name__ == '__main__':
    main()
