# PulseStream Analytics Engine ⚡

PulseStream is a production-ready, end-to-end real-time event analytics and anomaly detection engine. It is architected to ingest high-velocity, continuous clickstream and transactional events from an e-commerce platform, process the data inflight, and dynamically flag behavioral anomalies using a distributed stream-processing framework.

The primary objective of this project is to build a scalable, decoupled data infrastructure capable of handling millisecond-level event ingestions, restructuring semi-structured JSON payloads, and streaming immediate data insights to a console sink.

---

## 📌 Table of Contents
* [🏗️ System Architecture & Data Flow](#️-system-architecture--data-flow)
* [🛠️ Unified Technology Stack](#️-unified-technology-stack)
* [📊 Stream Data Contract & Schema Enforcement](#-stream-data-contract--schema-enforcement)
* [🚀 Deployment & Operational Guide](#-deployment--operational-guide)

---

## 🏗️ System Architecture & Data Flow

The entire data pipeline infrastructure is fully containerized, isolated, and orchestrated using **Docker Compose**. The flow of events across the system is designed as follows:

```text
[ Python Mock Producer ] 
         │
         ▼ (JSON Event Serialization via Port 9092)
[ Apache Kafka Broker (ecommerce-clicks Topic) ] <───> [ Apache Zookeeper ]
         │
         ▼ (Distributed Streaming Consumption)
[ Apache Spark Structured Streaming Engine ] 
         │
         ├─► [ Schema Enforcement & De-serialization ]
         ├─► [ Rule-Based Anomaly Detection Logic ]
         │
         ▼ (Append Output Mode)
[ Live Interactive Console Output Dashboard ]

Infrastructure Components Breakdown:
Event Ingestion Engine (Kafka & Zookeeper): Act as the decoupled messaging backbone. Zookeeper manages cluster state, leader election, and configurations, while a single-node Kafka broker maintains log persistence for the ecommerce-clicks stream.

Distributed Processing Cluster (Spark Master-Worker): A standalone Apache Spark cluster configured with a master node and a dedicated worker container to process streaming DataFrames concurrently using discrete micro-batches.

Data Transformation & Infrastructure Layout (dbt & Terraform): Includes pre-configured structural directories to scale this architectural design into a cloud data warehouse (e.g., Snowflake/BigQuery) managed by Infrastructure as Code (IaC).

🛠️ Unified Technology Stack
Core Language: Python 3.10+ (Robust scripting for generation and orchestration)

Message Broker Infrastructure: Apache Kafka 7.3.0 (Confluent Platform distribution)

Stream Processing Engine: Apache Spark 3.5.0 (Structured Streaming API utilizing PySpark)

Environment Isolation: Docker & Docker Compose (Container multi-networking configuration)

Modern Data Stack Preparedness: dbt (Data Build Tool for warehousing transformations) & Terraform (HashiCorp Infrastructure orchestration)

📊 Stream Data Contract & Schema EnforcementTo guarantee downstream data integrity, raw binary payloads consumed from the Kafka log are cast to string values, parsed, and validated against a strict data contract using Spark SQL types:Field NameData TypeDescriptiontimestampStringTypeISO-8601 formatted event record execution timeuser_idStringTypeUnique hash identifier assigned to the web session useractionStringTypeInteraction event type (click, view, add_to_cart, purchase)product_idStringTypeSystem SKU barcode identifying the specific e-commerce productamountDoubleTypeFinancial transaction monetary value (applicable on purchases)countryStringTypeAlpha-2 regional code of the originating client IP address

Bhai, ghalti ki maafi! Main aapka matlab ab bilkul sahi samajh gaya hoon. Aapko koi baahir ka link nahi chahiye, balki aap chahte hain ke poori detailed README (Table of Contents ke links ke sath) isi aik hi code box mein de doon taake aap yahin se aik hi baar copy-paste kar sakein.Yeh lijiye poora code aik hi box mein. Aap bas isko poora copy karke apni README.md file mein paste kar dein:Markdown# PulseStream Analytics Engine ⚡

PulseStream is a production-ready, end-to-end real-time event analytics and anomaly detection engine. It is architected to ingest high-velocity, continuous clickstream and transactional events from an e-commerce platform, process the data inflight, and dynamically flag behavioral anomalies using a distributed stream-processing framework.

The primary objective of this project is to build a scalable, decoupled data infrastructure capable of handling millisecond-level event ingestions, restructuring semi-structured JSON payloads, and streaming immediate data insights to a console sink.

---

## 📌 Table of Contents
* [🏗️ System Architecture & Data Flow](#️-system-architecture--data-flow)
* [🛠️ Unified Technology Stack](#️-unified-technology-stack)
* [📊 Stream Data Contract & Schema Enforcement](#-stream-data-contract--schema-enforcement)
* [🚀 Deployment & Operational Guide](#-deployment--operational-guide)

---

## 🏗️ System Architecture & Data Flow

The entire data pipeline infrastructure is fully containerized, isolated, and orchestrated using **Docker Compose**. The flow of events across the system is designed as follows:

```text
[ Python Mock Producer ] 
         │
         ▼ (JSON Event Serialization via Port 9092)
[ Apache Kafka Broker (ecommerce-clicks Topic) ] <───> [ Apache Zookeeper ]
         │
         ▼ (Distributed Streaming Consumption)
[ Apache Spark Structured Streaming Engine ] 
         │
         ├─► [ Schema Enforcement & De-serialization ]
         ├─► [ Rule-Based Anomaly Detection Logic ]
         │
         ▼ (Append Output Mode)
[ Live Interactive Console Output Dashboard ]
Infrastructure Components Breakdown:Event Ingestion Engine (Kafka & Zookeeper): Act as the decoupled messaging backbone. Zookeeper manages cluster state, leader election, and configurations, while a single-node Kafka broker maintains log persistence for the ecommerce-clicks stream.Distributed Processing Cluster (Spark Master-Worker): A standalone Apache Spark cluster configured with a master node and a dedicated worker container to process streaming DataFrames concurrently using discrete micro-batches.Data Transformation & Infrastructure Layout (dbt & Terraform): Includes pre-configured structural directories to scale this architectural design into a cloud data warehouse (e.g., Snowflake/BigQuery) managed by Infrastructure as Code (IaC).🛠️ Unified Technology StackCore Language: Python 3.10+ (Robust scripting for generation and orchestration)Message Broker Infrastructure: Apache Kafka 7.3.0 (Confluent Platform distribution)Stream Processing Engine: Apache Spark 3.5.0 (Structured Streaming API utilizing PySpark)Environment Isolation: Docker & Docker Compose (Container multi-networking configuration)Modern Data Stack Preparedness: dbt (Data Build Tool for warehousing transformations) & Terraform (HashiCorp Infrastructure orchestration)📊 Stream Data Contract & Schema EnforcementTo guarantee downstream data integrity, raw binary payloads consumed from the Kafka log are cast to string values, parsed, and validated against a strict data contract using Spark SQL types:Field NameData TypeDescriptiontimestampStringTypeISO-8601 formatted event record execution timeuser_idStringTypeUnique hash identifier assigned to the web session useractionStringTypeInteraction event type (click, view, add_to_cart, purchase)product_idStringTypeSystem SKU barcode identifying the specific e-commerce productamountDoubleTypeFinancial transaction monetary value (applicable on purchases)countryStringTypeAlpha-2 regional code of the originating client IP address🚨 Real-Time Analytical Transformation: Anomaly DetectionThe Spark core processor dynamically monitors the streaming DataFrame and computes an analytical flag field (is_anomaly) using conditional rule matching:If an incoming event record has an amount value exceeding $5000.0, the transaction is classified as an outlier or fraudulent behavior signature.A flag value of 1 is explicitly assigned, else it falls back to a safe standard code of 0.
Deployment & Operational Guide
Follow these automated steps sequentially within your terminal to initialize the distributed cluster and monitor the live data stream:

Step 1: Launch the Infrastructure Cluster
Spin up all containerized analytics components and background networks in a decoupled daemon state:
docker-compose up -d

Step 2: Submit and Execute the Spark Streaming Engine
Deploy the streaming analytics script directly onto the running Spark Master environment using the official Kafka native SQL integration package:
docker exec -it stream-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /tmp/spark_streaming.py

Step 3: Initialize the Synthetic Data Streaming Producer
Open an independent parallel terminal interface tab and awaken the dedicated Python event generator container to broadcast transactions:

docker start stream-kafka-producer

Once activated, your Spark runtime console will continuously intercept micro-batches from the broker and instantly print streaming state updates across the viewport.