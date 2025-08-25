# Real-Time Disaster Prediction Platform

This project is a **real-time data pipeline** for monitoring and predicting disasters (e.g., earthquakes, floods, wildfires) using **Kafka, Spark Structured Streaming, Delta Lake, and AWS**. It is designed to demonstrate **distributed systems, real-time data processing, machine learning in production, and cloud integration** — skills expected in large-scale production systems (MAANG-level).

---

## Project Overview

The aim of this module is to:
1. Collect earthquake event data in real-time from **USGS GeoJSON API**.
2. Publish the data into a **Kafka topic**.
3. Consume the stream using **Apache Spark Structured Streaming**.
4. Persist the stream as a **Delta Lake Table** (transactional storage format).
5. Enable downstream analytics and dashboards (local or cloud-based).

This is the first step towards building a **scalable, cloud-ready, real-time disaster monitoring system**.

![Initial Project Plan](Assets/Initial_plan.drawio.svg)
---

## Features

- **Real-time ingestion** of disaster-related data (e.g., NOAA/USGS/NASA feeds).
- **Apache Kafka** as the event streaming backbone.
- **Spark Structured Streaming** for data consumption, transformation, and ML inference.
- **Delta Lake** as the storage layer for structured and incremental updates.
- **AWS S3** integration for data lake storage and checkpoints.
- **Modular architecture** to extend pipelines for new disaster types.
- **End-to-end scalability** suitable for cloud deployment.

---

## Architecture

        +----------------------+
        |   Data Sources        |
        | (NOAA, USGS, NASA)    |
        +----------+-----------+
                   |
                   v
          +------------------+
          |  Kafka Producers |
          +------------------+
                   |
                   v
          +------------------+
          |  Kafka Topics    |
          +------------------+
                   |
                   v
        +----------------------+
        | Spark Structured     |
        | Streaming Consumers  |
        |  - Earthquake data   |
        |  - Flood data        |
        |  - Wildfire data     |
        +----------------------+
                   |
                   v
          +------------------+
          |   Delta Lake     |
          | (AWS S3 backend) |
          +------------------+
                   |
                   v
          +------------------+
          | ML Models & APIs |
          |   Prediction +   |
          |   Alert System   |
          +------------------+

---

## Components

1. **Kafka Producer (`earthquake_producer.py`)**
   - Fetches data from [USGS Earthquake GeoJSON API](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php).
   - Publishes earthquake events into Kafka topic (`earthquake_topic`).

2. **Kafka Consumer with Spark (`earthquake_consumer.py`)**
   - Reads events from Kafka topic.
   - Parses JSON messages into structured schema.
   - Writes data as **Delta Table** with checkpointing for fault tolerance.

3. **Delta Lake Table**
   - Stores earthquake data in `delta/earthquake_delta_table/` (or on **AWS S3**).
   - Supports incremental updates and time travel for analytics.

---

## Next Steps

- Query Layer: Use AWS Athena or Databricks for SQL analytics.

- Dashboards:
    - Local: Streamlit.
    - Cloud: AWS QuickSight.

- Other Feeds: Extend ingestion to tsunami warnings, wildfires, floods.

- ML Models: Add predictive models for early disaster alerts.