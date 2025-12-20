# Automated Satellite Telemetry Data Warehouse

## Project Overview
This project is a fully automated, headless ETL (Extract, Transform, Load) pipeline designed to ingest high-volume orbital telemetry data. It integrates real-time satellite tracking data from CelesTrak with space weather indices from NOAA, normalizing and storing them in a PostgreSQL data warehouse for long-term historical analysis.

The system is designed with a "Quality-First" architecture, implementing strict data validation gates and logic to ensure warehouse integrity without manual intervention.

## System Architecture

### Data Flow
1. **Extraction:** Python scripts fetch raw TLE (Two-Line Element) data and Solar Flux JSON data from external APIs.
2. **Transformation:**
   - Raw TLE strings are parsed to extract orbital elements (Inclination, Eccentricity, B* Drag).
   - Timestamp alignment and normalization.
3. **Loading:** Data is upserted into PostgreSQL. Custom logic checks for existing Primary Keys (Satellite ID + Epoch Time) to prevent duplication.
4. **Orchestration:** GitHub Actions triggers the pipeline on a fixed 8-hour schedule (CRON).

### Database Schema (Star Schema)
The data is modeled in PostgreSQL (Supabase) using a normalized approach to optimize storage and query performance.

* **fact_telemetry:** High-frequency orbital state vectors (Mean Motion, Drag Term, Inclination).
  - Primary Key: Composite (norad_id, epoch_utc)
* **fact_space_weather:** Daily solar flux (F10.7) and geomagnetic indices.
  - Primary Key: date_utc
* **dim_satellites:** Static metadata for objects (International Designator, Launch Year).
  - Primary Key: norad_id

## Technical Features

### Idempotency & Deduplication
The pipeline utilizes a "Check-then-Write" strategy. Before ingestion, the script queries the database for existing records matching the current batch's unique identifiers. Only new, unique records are committed. This allows the pipeline to be re-run multiple times without creating duplicate entries or corrupting the dataset.

### Automated Data Quality Gates
To protect the warehouse from low-quality data ingestion (Data Drift), the pipeline enforces a minimum row count threshold. If an API returns a statistically insignificant sample size (indicating an upstream outage or network error), the write operation is aborted, and the event is logged.

## Tech Stack
* **Language:** Python 3.12 (pandas, sqlalchemy, requests)
* **Database:** PostgreSQL (hosted on Supabase)
* **Orchestration:** GitHub Actions (CI/CD)

## Setup & Usage

1. **Clone the Repository**
   git clone https://github.com/Souma20/celestrak-tle-data-pipeline.git

2. **Install Dependencies**
   pip install -r requirements.txt

3. **Environment Configuration**
   Create a .env file with your database credentials:
   DATABASE_URL="postgresql://user:password@host:port/database"

4. **Manual Execution**
   python script.py

5. **Automated Execution**
   The pipeline is configured via `.github/workflows/main.yml` to run automatically every 8 hours. Check the "Actions" tab for execution logs.

# Automated Satellite Telemetry Data Warehouse

## Project Overview
This project is a fully automated, headless ETL (Extract, Transform, Load) pipeline designed to ingest high-volume orbital telemetry data. It integrates real-time satellite tracking data from CelesTrak with space weather indices from NOAA, normalizing and storing them in a PostgreSQL data warehouse to enable historical analysis of orbital decay mechanics.

## System Architecture

```mermaid
graph LR
    A[NOAA API] -->|JSON Data| C(Python ETL Worker)
    B[CelesTrak API] -->|TLE Data| C
    C -->|1. Parsing & Transformation| D[2. Deduplication Logic]
    D -->|3. Upsert Strategy| E[(PostgreSQL Warehouse)]
    E -->|Fact & Dimension Tables| F[Analytics Ready Data]
