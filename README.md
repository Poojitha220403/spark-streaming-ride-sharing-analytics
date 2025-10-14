Here’s a single, ready-to-paste **README.md** that matches your code and the outputs you generated (row-per-file for Task 1, batch folders for Tasks 2 & 3, including your `batch_0…batch_4` and `batch_85`):

````markdown
# Ride Sharing Analytics Using Spark Streaming and Spark SQL

A real-time analytics pipeline using **Apache Spark Structured Streaming** with a synthetic ride-sharing feed generated over a socket (localhost:9999).

---

## ✅ Prerequisites

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

## 📁 Project Structure

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

## ▶️ How to Run

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

## 🧠 Overview & Objectives

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

## 🔎 Verifying Outputs

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