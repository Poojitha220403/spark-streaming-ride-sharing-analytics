````markdown
# Ride Sharing Analytics Using Spark Streaming and Spark SQL

A real-time analytics pipeline using **Apache Spark Structured Streaming** with a synthetic ride-sharing feed generated over a socket (localhost:9999).

---

## Prerequisites

- **Python 3.x**
  ```bash
  python3 --version
````

* **PySpark**

  ```bash
  pip install pyspark==3.5.1
  ```
* **Faker** (for the data generator)

  ```bash
  pip install faker
  ```
* **Java 8+** (required by Spark)

  ```bash
  java -version
  ```

---

## Project Structure

> This reflects the structure and outputs currently in the Codespace.

```
HandsOn9/
├── data_generator.py
├── task1.py
├── task2.py
├── task3.py
├── outputs/
│   ├── task1/
│   │   ├── row_0_<uuid>/
│   │   │   └── part-00000-....csv
│   │   ├── row_1_<uuid>/
│   │   │   └── part-00000-....csv
│   │   ├── row_2_<uuid>/
│   │   │   └── part-00000-....csv
│   │   └── ... (one folder per row, one CSV per folder)
│   ├── task2/
│   │   ├── batch_0/part-00000-....csv
│   │   ├── batch_1/part-00000-....csv
│   │   ├── batch_2/part-00000-....csv
│   │   ├── batch_3/part-00000-....csv
│   │   └── batch_4/part-00000-....csv
│   └── task3/
│       └── batch_85/part-00000-....csv
└── README.md
```

> **Note:** `checkpoints/` exists locally for Spark state and should **not** be committed.

---

## How to Run

Open **two terminals** (keep the generator running while you run each task).

### 1) Start the data generator (Terminal 1)

```bash
python data_generator.py
# Streams one JSON event per line to localhost:9999
```

### 2) Run a task (Terminal 2)

Run **one task at a time**:

```bash
# Task 1: Ingestion & parsing → CSV (one file per row)
python task1.py

# Task 2: Aggregations by driver → CSV (one folder per micro-batch)
python task2.py

# Task 3: Windowed sum(fare_amount) over 5-min window (slide=1 min, watermark=1 min) → CSV
python task3.py
```

> **Tip:** Task 3 needs a few minutes of runtime for windows to materialize.

---

## Overview & Objectives

We build a real-time analytics pipeline for a ride-sharing platform using Spark Structured Streaming:

* **Task 1:** Ingest and parse streaming ride data (JSON → columns).
* **Task 2:** Real-time aggregations on driver earnings and trip distances.
* **Task 3:** Time-based trend analysis using a 5-minute window sliding by 1 minute with a 1-minute watermark.

---

## 🧩 Task 1 — Basic Streaming Ingestion and Parsing

**Goal:** Ingest from `localhost:9999` and parse JSON into columns
`(trip_id, driver_id, distance_km, fare_amount, timestamp)`.

**Implementation notes:**

* `spark.readStream.format("socket")` to read the stream.
* Parse JSON → columns using a predefined schema.
* **No console printing**; each micro-batch is handled so **each row is written to its own CSV file** under `outputs/task1/row_*`.

**Current output example (header order from your files):**

```
outputs/task1/row_0_<uuid>/part-00000-....csv
distance_km,driver_id,fare_amount,timestamp,trip_id
...
```

---

## 📊 Task 2 — Real-Time Aggregations (Driver-Level)

**Aggregations:**

* `SUM(fare_amount)` as **total_fare**
* `AVG(distance_km)` as **avg_distance**
* Grouped by `driver_id`

**Implementation notes:**

* Convert timestamp → `TimestampType` (used for consistency).
* Use `outputMode("complete")` for grouped aggregations.
* **Write to CSV** with **one folder per micro-batch** under `outputs/task2/batch_*`.

**Current output examples present:**

```
outputs/task2/batch_0/part-00000-....csv
outputs/task2/batch_1/part-00000-....csv
outputs/task2/batch_2/part-00000-....csv
outputs/task2/batch_3/part-00000-....csv
outputs/task2/batch_4/part-00000-....csv
```

**CSV header (from your files):**

```
driver_id,total_fare,avg_distance
```

---

## ⏱️ Task 3 — Windowed Time-Based Analytics

**Windowing:**

* Convert `timestamp` → `event_time` (`TimestampType`)
* **Watermark:** 1 minute
* **Window:** 5 minutes
* **Slide:** 1 minute
* Aggregation: `SUM(fare_amount)` → `sum_fare`

**Implementation notes:**

* Use `groupBy(window(event_time, "5 minutes", "1 minute"))`.
* Select `window.start` and `window.end` into `window_start` and `window_end`.
* **Write to CSV** with **one folder per micro-batch** under `outputs/task3/batch_*`.

**Current output example present:**

```
outputs/task3/batch_85/part-00000-....csv
```

**CSV header (from your file):**

```
window_start,window_end,sum_fare
```

---

## 🔎 Verifying Outputs/ 

List generated files:

```bash
ls -R outputs
```

Preview a few files:

```bash
head outputs/task1/*/part-*.csv 2>/dev/null | sed -n '1,5p'
head outputs/task2/batch_*/part-*.csv 2>/dev/null | sed -n '1,10p'
head outputs/task3/batch_*/part-*.csv 2>/dev/null | sed -n '1,5p'
```

---

## Sample Outputs

## Task 1 — Parsed Events (example row)

From outputs/task1/row_1_0_baabb114-9e9e-40dd-b9af-eec6696f1955/part-00000-2677106c-f8b4-4118-8994-eff81c307221-c000.csv:

```
distance_km,driver_id,fare_amount,timestamp,trip_id
47.74,55,13.27,2025-10-14 17:19:44,0a68b425-466c-46d8-8c92-48467d125e42
```

## Task 2 — Driver Aggregations (sample from batch_1)

From outputs/task2/batch_1/part-00000-3a6cb8fa-2259-4103-85f7-3268a9810f5d-c000.csv:

```
driver_id,total_fare,avg_distance
31,126.78,31.74
65,139.84,28.42
53,38.82,16.62
28,86.96,8.46
93,81.01,26.12
20,24.16,27.23
40,71.8,16.15
19,51.39,15.75
```

(Your file contains more rows; this snippet shows the top portion.)

## Task 3 — Windowed Aggregation (sample from batch_85)

From outputs/task3/batch_85/part-00000-c6fec9d1-95fb-41d2-a059-06d5564804d3-c000.csv:

```
window_start,window_end,sum_fare
2025-10-14T17:37:00.000Z,2025-10-14T17:42:00.000Z,23685.089999999997
```

## 🧹 Cleaning / Reruns

If you change output mode or schema and see a checkpoint error, stop the job and clear state for that task:

```bash
rm -rf checkpoints/task1 checkpoints/task2 checkpoints/task3
```

Optionally prune outputs if you want to regenerate:

```bash
# CAUTION: this removes files
rm -rf outputs/task1 outputs/task2 outputs/task3
```
