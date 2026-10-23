# SNMP Kafka Data Pipeline Project

ระบบสะสมและรับส่งข้อมูล SNMP จาก Cisco Router 7200 ผ่าน Apache Kafka ตาม Architecture Diagram

## 🏗 Architecture
`Cisco Router 7200` --(SNMP)--> `collector.py (Producer)` --(Produce)--> `Kafka (Topic: snmp)` --(Consume)--> `worker.py (Consumer)`

## 🚀 Quick Start Guide

### 1. Start Infrastructure (Kafka & Zookeeper)
```bash
docker-compose up -d
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run Worker (Consumer)
```bash
python worker.py
```

### 4. Run Collector (Producer)
```bash
python collector.py
```

## 📤 Push to GitHub Commands
```bash
git init
git add .
git commit -m "Initial commit: SNMP Kafka Data Pipeline project"
git branch -M main
git remote add origin <YOUR_GITHUB_REPOSITORY_URL>
git push -u origin main
```
