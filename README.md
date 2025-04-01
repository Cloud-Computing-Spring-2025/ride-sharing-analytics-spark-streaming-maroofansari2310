# Real-Time Ride-Sharing Analytics with Apache Spark

## Overview
This project processes real-time ride-sharing data using Apache Spark Structured Streaming. It consists of three tasks that ingest, aggregate, and analyze streaming data, writing results to CSV files.

---

## Setup

### Prerequisites
- Apache Spark 3.5+
- Python 3.8+ with dependencies:
  ```bash
  pip install pyspark faker
  ```

### Start Data Generator
Run the Python script to simulate ride-sharing events:
```bash
python data_generator.py
```
**Output** (example):
```
Streaming data to localhost:9999...
New client connected: ('127.0.0.1', 45872)
Sent: {'trip_id': '64f666bb-ddc4...', 'driver_id': 1, ...}
```

---

## Tasks

### Task 1: Raw Data Ingestion
**Purpose**: Parse and store raw ride events from the socket stream.  
**Command**:
```bash
spark-submit task1.py
```
**Output Directory**: `output/task1/data`  
**Sample Output** (`part-00000-*.csv`):
```
trip_id,driver_id,distance_km,fare_amount,timestamp
c1d76131-2edc-40b8-a402-90d04d554b83,100,33.79,41.08,2025-04-01 23:09:20
c01be251-f544-437b-9b76-2586db7faede,69,46.82,72.22,2025-04-01 23:09:22
830940b1-51d9-4cdf-b917-4e0b2e721b7d,49,44.59,32.89,2025-04-01 23:09:24
```

---

### Task 2: Driver-Level Aggregations
**Purpose**: Calculate total fare and average distance per driver.  
**Command**:
```bash
spark-submit task2.py
```
**Output Directory**: `output/task2`  
**Sample Output** (`part-00000-*.csv`):
```
driver_id,total_fare,avg_distance
11,141.99,5.59
69,82.21000000000001,45.995000000000005
64,116.4,3.45
3,86.32,18.34
59,50.47,32.54
16,137.67,47.55
```

---

### Task 3: Windowed Fare Analytics
**Purpose**: Compute total fares in 5-minute sliding windows (1-minute interval).  
**Command**:
```bash
spark-submit task3.py
```
**Output Directory**: `output/task3`  
**Sample Output** (`part-00000-*.csv`):
```
window_start,window_end,total_fare
2025-04-01T23:13:00.000Z,2025-04-01T23:18:00.000Z,2108.6400000000003
2025-04-01T23:12:00.000Z,2025-04-01T23:17:00.000Z,2108.6400000000003
2025-04-01T23:10:00.000Z,2025-04-01T23:15:00.000Z,2108.6400000000003
2025-04-01T23:11:00.000Z,2025-04-01T23:16:00.000Z,2108.6400000000003

```

---

## Folder Structure
```
ride-sharing-analytics-spark-streaming/
├── data_generator.py       # Simulates ride events
├── task1.py                # Ingests raw data
├── task2.py                # Driver-level aggregations
├── task3.py                # Windowed analytics
├── output/
│   ├── task1/data/         # Raw ride data (CSV)
│   ├── task2/              # Driver aggregates (CSV)
│   └── task3/              # Windowed totals (CSV)
└── checkpoints/            # Spark metadata (not for processing)
    ├── task1/
    ├── task2/
    └── task3/
```

---

## Notes
1. **Timestamp Format**: Outputs use ISO-8601 format (`2025-04-01T22:49:00.000Z`) for window boundaries.  
2. **Future Dates**: Timestamps appear in 2025 due to the synthetic data generator.  
3. **Checkpoints**: Metadata is stored in `checkpoints/` – do not modify/delete while jobs are running.  
4. **Output Files**: CSV files are partitioned and may contain multiple `part-*.csv` files.  
