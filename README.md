# Simple Lambda Architecture Real-Time + Batch Data Pipeline (Airflow + S3)

This project implements a minimal, production-ready Lambda Architecture using **Apache Airflow** and **AWS S3**.
It combines **real-time ingestion (Speed Layer)**, **daily batch ETL (Batch Layer)**, and a **Serving Layer** that merges both views.

---

## 1. Architecture Overview

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
                   │   Interval: Every 10 minutes   │
                   └──────────────────────────────┘
```

The pipeline processes five real-time event streams:

- IoT sensors
- API logs
- Social media
- E-commerce clickstream
- Financial transactions

All data lands in S3, flows through batch ETL, and is merged into unified serving datasets.

---

## 2. Airflow DAGs

| DAG ID                                     | Schedule       | Layer            |
| ------------------------------------------ | -------------- | ---------------- |
| `1-infra_s3_bootstrap`                     | Manual         | Infra setup      |
| `2-api_logs_realtime_ingest`               | `*/5 * * * *`  | Speed (Realtime) |
| `2-ecommerce_clickstream_realtime_ingest`  | `*/5 * * * *`  | Speed (Realtime) |
| `2-financial_transactions_realtime_ingest` | `*/5 * * * *`  | Speed (Realtime) |
| `2-iot_sensors_realtime_ingest`            | `*/5 * * * *`  | Speed (Realtime) |
| `2-social_media_realtime_ingest`           | `*/5 * * * *`  | Speed (Realtime) |
| `3-batch_etl_all_streams`                  | `0 2 * * *`    | Batch            |
| `4-serving_layer_merge`                    | `*/10 * * * *` | Serving          |

---

## 3. Project Structure

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

## 4. Data Flow Summary

### Speed Layer (Real-Time Ingestion)

- Trigger: Every 5 minutes
- Writes raw CSV events to `s3://<bucket>/realtime/<stream>/`

### Batch Layer (Daily ETL)

- Trigger: 2:00 AM
- Produces:

  - Bronze (raw)
  - Silver (cleaned)
  - Gold (aggregated)

- Stored under `s3://<bucket>/batch/`

### Serving Layer (Unified Views)

- Trigger: Every 10 minutes
- Merges Speed + Batch results
- Outputs unified datasets, analytics, dashboards, alerts

---

## 5. S3 Layout

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
