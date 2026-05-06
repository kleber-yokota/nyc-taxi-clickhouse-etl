# NYC Taxi Trip ETL

ETL pipeline for ingesting NYC Trip Data, storing in object storage (Garage), and loading into ClickHouse for OLAP analysis.

## Overview

```
NYC Trip Data → Extract → Transform → Load → Garage (Object Storage) → ClickHouse (OLAP) → Analytics
```

## Flow

1. **Extract** — Download NYC taxi trip datasets (yellow, green, fhv, etc.), skipping files already in S3
2. **Transform** — Clean, normalize, and transform raw data
3. **Load (Object Storage)** — Persist transformed data into Garage (S3-compatible object storage)
4. **Load (OLAP)** — Load data into ClickHouse for analytical queries and dashboards

## Architecture

- **Garage** — S3-compatible object storage for durable, low-cost storage of raw and transformed data
- **ClickHouse** — OLAP database for trip data analysis, aggregations, and dashboards

## Data

Source dataset: [NYC Taxi & Limousine Commission (TLC) Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Running

```bash
uv sync
uv run python main.py
```

## Project Structure

```
├── main.py              # Entry point
├── config.py            # Configuration (Garage, ClickHouse)
├── extract/             # Download and ingestion of NYC Trip data
├── transform/           # Data cleaning and transformation
├── load/                # Loading into Garage and ClickHouse
└── tests/               # Tests
```
