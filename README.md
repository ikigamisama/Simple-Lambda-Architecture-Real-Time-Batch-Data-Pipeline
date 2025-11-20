# 📡 Lambda Architecture Data Pipeline

**Real-Time + Batch Processing using Apache Airflow and AWS S3**

This project implements a modern **Lambda Architecture** that combines **low-latency real-time ingestion** with **daily batch ETL processing**, all orchestrated by **Apache Airflow** and stored in **AWS S3**.

The system delivers:

- ⚡ **Speed Layer** — real-time ingestion every **5 minutes**
- 🧱 **Batch Layer** — daily ETL at **2:00 AM**
- 📊 **Serving Layer** — unified analytics refreshed every **10 minutes**

Ideal for demonstrating how real-time and historical data can power dashboards, alerts, and analytical workloads together.

---

## 🏗 1. Architecture Overview

```
                   ┌──────────────────────────────┐
                   │          Speed Layer          │
                   │       (Real-Time Ingest)      │
                   │  Interval: Every 5 minutes    │
                   └───────────────┬───────────────┘
                                   │
                                   ▼
                   ┌──────────────────────────────┐
                   │          Batch Layer          │
                   │      Daily ETL at 2:00 AM     │
                   └───────────────┬───────────────┘
                                   │
                                   ▼
                   ┌──────────────────────────────┐
                   │          Serving Layer        │
                   │     Merge Batch + Real-Time   │
                   │   Interval: Every 10 minutes  │
                   └──────────────────────────────┘
```

The platform simulates five independent event streams:

- IoT sensor telemetry
- API access logs
- E-commerce clickstream
- Social media interactions
- Financial transactions

All pipelines land data in **S3**, undergo incremental or daily transformations, and are merged into consumable datasets for reporting and dashboards.

---

## ⚙️ 2. Airflow DAGs

| DAG ID                                     | Schedule       | Layer          |
| ------------------------------------------ | -------------- | -------------- |
| `1-infra_s3_bootstrap`                     | Manual         | Infrastructure |
| `2-api_logs_realtime_ingest`               | `*/5 * * * *`  | Speed          |
| `2-ecommerce_clickstream_realtime_ingest`  | `*/5 * * * *`  | Speed          |
| `2-financial_transactions_realtime_ingest` | `*/5 * * * *`  | Speed          |
| `2-iot_sensors_realtime_ingest`            | `*/5 * * * *`  | Speed          |
| `2-social_media_realtime_ingest`           | `*/5 * * * *`  | Speed          |
| `3-batch_etl_all_streams`                  | `0 2 * * *`    | Batch          |
| `4-serving_layer_merge`                    | `*/10 * * * *` | Serving        |

---

## 📁 3. Project Structure

```
airflow/
├── dags/
│   ├── create_s3_buckets.py
│   ├── realtime/
│   │   ├── api_logs_realtime_ingest.py
│   │   ├── ecommerce_clickstream_realtime_ingest.py
│   │   ├── financial_transactions_realtime_ingest.py
│   │   ├── iot_sensors_realtime_ingest.py
│   │   └── social_media_realtime_ingest.py
│   ├── batch_etl.py
│   └── serving_layer.py
│
├── plugins/
│   └── utils/
│       └── s3_utils.py
└── README.md
```

---

## 🔄 4. Data Flow Summary

### ⚡ Speed Layer — Real-Time Ingestion

- Trigger: **every 5 minutes**
- Raw data stored to:

  ```
  s3://<bucket>/realtime/<stream>/
  ```

### 🧱 Batch Layer — Daily ETL (Medallion)

- Trigger: **2:00 AM**
- Generates:

  - **Bronze** — raw ingested dataset
  - **Silver** — cleaned and validated data
  - **Gold** — business-ready aggregated features

- Stored in:

  ```
  s3://<bucket>/batch/
  ```

### 📊 Serving Layer — Unified Analytics

- Trigger: **every 10 minutes**
- Merges latest real-time and batch outputs
- Produces:

  - unified datasets
  - reporting tables
  - dashboard-ready outputs
  - optional alerting outputs

---

## 🪣 5. S3 Layout

```
s3://your-bucket/
├── realtime/
│   ├── api_logs/
│   ├── ecommerce_clickstream/
│   ├── financial_transactions/
│   ├── iot_sensors/
│   └── social_media/
│
├── batch/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
└── serving/
    ├── unified/
    ├── dashboards/
    ├── analytics/
    └── alerts/
```

---

## 🚀 What This Project Demonstrates

- End-to-end orchestrated data pipelines with realistic schedules
- Real-time + batch processing working together
- Medallion architecture within a Lambda framework
- Modular, easily extendable Airflow DAG design
- A strong foundation for BI dashboards, data science workloads, or alerting systems
