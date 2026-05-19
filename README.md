# PulseStream: High-Velocity Real-Time E-Commerce Streaming Analytics Engine ⚡

An enterprise-grade, end-to-end real-time data engineering pipeline designed to ingest, process, and analyze high-velocity transactional events and clickstream data from an e-commerce platform with sub-second latency.

## 📌 Table of Contents
* [1. Project Overview](#1-project-overview)
* [2. System Architecture and Data Flow](#2-system-architecture-and-data-flow)
* [3. Tech Stack and Tools](#3-tech-stack-and-tools)
* [4. Stream Data Contract and Schema Enforcement](#4-stream-data-contract-and-schema-enforcement)
* [5. Real-Time Analytical Transformation and Anomaly Detection](#5-real-time-analytical-transformation-and-anomaly-detection)
* [6. Deployment and Operational Guide](#6-deployment-and-operational-guide)

## 1. Project Overview

In modern digital commerce, understanding user behavior and spotting fraudulent high-amount transactions requires immediate action. Standard batch processing creates hours of delay, making it impossible to intercept issues inflight.

PulseStream solves this challenge by implementing a decoupled, fault-tolerant infrastructure that captures micro-batches of continuous semi-structured JSON clickstream records, restructures them under tight schema design constraints, and dynamically runs rule-based analytical evaluation engines to instantly flag behavioral anomalies.

## 2. System Architecture and Data Flow

The entire data pipeline infrastructure is fully containerized, isolated, and orchestrated using **Docker Compose** across an enterprise-grade local network setup:

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

Infrastructure Components Breakdown
Event Ingestion Engine (Kafka & Zookeeper): Serves as the distributed messaging backbone. Zookeeper handles health tracking and coordination, while the Kafka broker acts as the persistent storage log queue for the live ingestion stream.

Stream Processing Engine (Apache Spark Master-Worker): A distributed standalone compute layer configured to continuously consume windowed event data micro-batches concurrently.

Modern Data Stack Layout (dbt & Terraform): Includes clean isolated directory roots ready to provision scalable cloud warehouses managed entirely by automation scripts.

## 3. Tech Stack and Tools

| Category | Technology | Purpose / Role in Pipeline |
| :--- | :--- | :--- |
| **Containerization** | Docker & Docker Compose | Multi-container environment isolation |
| **Stream Ingestion** | Apache Kafka & Zookeeper | Distributed decoupled message brokering |
| **Stream Processing** | Apache Spark (PySpark) | Structured Streaming micro-batch processing engine |
| **Core Language** | Python 3.10+ | Synthetic data generation and streaming scripts |
| **Transformation** | dbt Core | Prepared operational database modeling structure |
| **Infrastructure as Code** | Terraform | HashiCorp infrastructure provisioning layout |

