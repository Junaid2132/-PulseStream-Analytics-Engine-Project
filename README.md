# PulseStream Analytics Engine ⚡

PulseStream is an end-to-end, real-time event-driven data engineering pipeline designed to ingest, process, and analyze high-velocity e-commerce clickstream data. The system dynamically identifies purchasing anomalies and system-wide patterns as events occur.

## 🏗️ System Architecture
The infrastructure is fully containerized using Docker and consists of the following components:
1. **Mock Producer:** A Python script simulating live user actions (clicks, views, purchases) and sending them to Kafka.
2. **Apache Kafka & Zookeeper:** Serves as the real-time event streaming backbone to ingest and queue messages.
3. **Apache Spark (Structured Streaming):** Consumes the raw JSON stream from Kafka, applies a data schema, and runs analytical logic to detect anomalies (e.g., high-amount transactions).
4. **dbt & Terraform:** Pre-configured directories for future analytics warehousing and cloud infrastructure provisioning.

---

## 🛠️ Tech Stack
* **Language:** Python 3.10+
* **Stream Ingestion:** Apache Kafka, Zookeeper
* **Stream Processing:** Apache Spark (Structured Streaming)
* **Containerization:** Docker & Docker Compose
* **Data Transformation & IaC:** dbt, Terraform

---

## 🚀 How to Run the Project

### 1. Start the Infrastructure
Bring up all the containerized services (Kafka, Spark Master/Worker, Zookeeper, Producer) in detached mode:
```bash
docker-compose up -d
2. Submit the Spark Streaming Job:

PowerShell
docker exec -it stream-spark-master /opt/spark/bin/spark-submit --packages org.ap
3. Trigger the Live Data Producer (Run in a New Terminal Window):
PowerShell
docker start stream-kafka-producer
