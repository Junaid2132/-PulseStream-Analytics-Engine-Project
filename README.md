# PulseStream: High-Velocity Real-Time E-Commerce Streaming Analytics Engine ⚡

An enterprise-grade, end-to-end real-time data engineering pipeline designed to ingest, process, and analyze high-velocity transactional events and clickstream data from an e-commerce platform with sub-second latency.

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

The entire data pipeline infrastructure is fully containerized, isolated, and orchestrated using **Docker Compose** across an enterprise-grade local network setup:

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

Infrastructure Components BreakdownEvent Ingestion Engine (Kafka & Zookeeper):
Serves as the distributed messaging backbone. Zookeeper handles health tracking and coordination, while the Kafka broker acts as the persistent storage log queue for the live ingestion stream.

Stream Processing Engine (Apache Spark Master-Worker):
A distributed standalone compute layer configured to continuously consume windowed event data micro-batches concurrently.

Modern Data Stack Layout (dbt & Terraform): 
Includes clean isolated directory roots ready to provision scalable cloud warehouses managed entirely by automation scripts.

<br><br>

## 🛠️ 3. Tech Stack and Tools

CategoryTechnologyPurpose / Role in PipelineContainerizationDocker & Docker ComposeMulti-container environment isolationStream IngestionApache Kafka & ZookeeperDistributed decoupled message brokeringStream ProcessingApache Spark (PySpark)Structured Streaming micro-batch processing engineCore LanguagePython 3.10+Synthetic data generation and streaming scriptsTransformationdbt CorePrepared operational database modeling structureInfrastructure as CodeTerraformHashiCorp infrastructure provisioning layout

<br><br>

## 📋 4. Stream Data Contract and Schema Enforcement

To guarantee downstream data integrity and stop malformed payloads from breaking the compute cluster, raw binary messages are cast into validated formats using strict Spark SQL types:
Field NameData TypeDescriptiontimestampStringTypeISO-8601 formatted event execution runtime loguser_idStringTypeUnique alphanumeric token tracking the user sessionactionStringTypeEvent activity type (click, view, add_to_cart, purchase)product_idStringTypeStandard stock keeping unit (SKU) identifying itemsamountDoubleTypeFinancial transaction transaction cost parametercountryStringTypeAlpha-2 geographical identification code

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
PowerShell docker-compose up -d

Step 2: Submit and Execute the Spark Streaming Engine
PowerShell docker exec -it stream-spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /tmp/spark_streaming.py

Step 3: Initialize the Synthetic Data Streaming Producer
PowerShell docker start stream-kafka-producer
