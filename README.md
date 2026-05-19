# PulseStream: High-Velocity Real-Time E-Commerce Streaming Analytics Engine ⚡

An enterprise-grade, end-to-end real-time data engineering pipeline designed to ingest, process, and analyze high-velocity transactional events and clickstream data from an e-commerce platform with sub-second latency.

<br><br>

## 📌 Table of Contents
* [1. Project Overview](#📖-1-project-overview)
* [2. System Architecture and Data Flow](#🏗️-2-system-architecture-and-data-flow)
* [3. Tech Stack and Tools](#🛠️-3-tech-stack-and-tools)
* [4. Stream Data Contract and Schema Enforcement](#📋-4-stream-data-contract-and-schema-enforcement)
* [5. Real-Time Analytical Transformation and Anomaly Detection](#📊-5-real-time-analytical-transformation-and-anomaly-detection)
* [6. Deployment and Operational Guide](#🚀-6-deployment-and-operational-guide)

<br><br>

## 📖 1. Project Overview

In modern digital commerce, understanding user behavior and spotting fraudulent high-amount transactions requires immediate action. Standard batch processing creates hours of delay, making it impossible to intercept issues inflight.

PulseStream solves this challenge by implementing a decoupled, fault-tolerant infrastructure that captures micro-batches of continuous semi-structured JSON clickstream records, restructures them under tight schema design constraints, and dynamically runs rule-based analytical evaluation engines to instantly flag behavioral anomalies.

<br><br>

## 🏗️ 2. System Architecture and Data Flow
graph TD
    A[Python Mock Producer] -->|JSON Event Stream| B(Apache Kafka Broker)
    B <--> C[Apache Zookeeper]
    B -->|Distributed Consumption| D[Apache Spark Streaming Engine]
    D --> E{Schema Enforcement & Logic}
    E -->|Anomaly Flagged| F[Output Console Dashboard]
    
    style A fill:#f9f,stroke:#333,stroke-width:2px
    style B fill:#bbf,stroke:#333,stroke-width:2px
    style D fill:#dfd,stroke:#333,stroke-width:2px
    style F fill:#ffd,stroke:#333,stroke-width:2px

Infrastructure Components BreakdownEvent Ingestion Engine (Kafka & Zookeeper):

Serves as the distributed messaging backbone. Zookeeper handles health tracking and coordination, while the Kafka broker acts as the persistent storage log queue for the live ingestion stream.

Stream Processing Engine (Apache Spark Master-Worker):

A distributed standalone compute layer configured to continuously consume windowed event data micro-batches concurrently.

Modern Data Stack Layout (dbt & Terraform): 

Includes clean isolated directory roots ready to provision scalable cloud warehouses managed entirely by automation scripts.

<br><br>

## 🛠️ 3. Tech Stack and Tools

| Category | Technology | Purpose / Role in Pipeline |
| :--- | :--- | :--- |
| **Containerization** | Docker & Docker Compose | Multi-container environment isolation |
| **Stream Ingestion** | Apache Kafka & Zookeeper | Distributed decoupled message brokering |
| **Stream Processing** | Apache Spark (PySpark) | Structured Streaming micro-batch processing engine |
| **Core Language** | Python 3.10+ | Synthetic data generation and streaming scripts |
| **Transformation** | dbt Core | Prepared operational database modeling structure |
| **Infrastructure as Code** | Terraform | HashiCorp infrastructure provisioning layout |

<br><br>

## 📋 4. Stream Data Contract and Schema Enforcement

| Field Name | Data Type | Description |
| :--- | :--- | :--- |
| `timestamp` | `StringType` | ISO-8601 formatted event execution runtime log |
| `user_id` | `StringType` | Unique alphanumeric token tracking the user session |
| `action` | `StringType` | Event activity type (`click`, `view`, `add_to_cart`, `purchase`) |
| `product_id` | `StringType` | Standard stock keeping unit (SKU) identifying items |
| `amount` | `DoubleType` | Financial transaction transaction cost parameter |
| `country` | `StringType` | Alpha-2 geographical identification code |

<br><br>

## 📊 5. Real-Time Analytical Transformation and Anomaly Detection

As events move through the Spark core engine, the stream pipeline dynamically applies an integrated real-time processing rule block to isolate high-risk behavior profiles:

Rule Matrix:

The compute engine checks every event. If the interaction type is marked as a purchase and the financial monetary amount exceeds a strict threshold of $5000.0, it is isolated as an alert signature.

Flag Mapping:

Records satisfying this risk condition are given an explicit analytical flag index value of 1, while standard standard safe interactions evaluate to a default state indicator of 0.

<br><br>

## 🚀 6. Deployment and Operational Guide
Follow these automated steps sequentially within your terminal environment to initialize the cluster and review the live streaming analytics output:

Step 1: Launch the Infrastructure Cluster

docker-compose up -d

Step 2: Submit and Execute the Spark Streaming Engine

docker exec -it stream-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /tmp/spark_streaming.py

Step 3: Initialize the Synthetic Data Streaming Producer

docker start stream-kafka-producer
