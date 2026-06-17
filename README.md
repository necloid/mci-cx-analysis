# 🛍️ E-Commerce Customer Experience Analytics Platform
> Fayza Lathifah Humam (5025241094)

### End-to-End ELT Pipeline & Business Intelligence Dashboard for *DustiniaDelixia Groceria*

This project builds an automated data pipeline to clean, process, and model large-scale marketplace data. By moving data from raw files into specialized analytical data marts, it helps identify exactly why customer reviews are stagnating. To turn this data into actionable business strategy, the platform connects directly to an interactive Metabase BI dashboard thus allowing team leaders to instantly see multi-dimensional insights.

<break>
<div align="center">

![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?logo=python&logoColor=white)
![Engine](https://img.shields.io/badge/OLAP%20Warehouse-ClickHouse-FFCC00?logo=clickhouse&logoColor=black)
![Orchestration](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CE2?logo=apache-airflow&logoColor=white)
![Visualization](https://img.shields.io/badge/BI%20Layer-Metabase-509EE3?logo=metabase&logoColor=white)
![Architecture](https://img.shields.io/badge/Architecture-ELT%20%7C%20Star%20Schema-orange)
![Environment](https://img.shields.io/badge/Environment-Docker%20Compose-2496ED?logo=docker&logoColor=white)

</div>

## 🏗️ System Architecture
```
Remote Cloud Storage Server
        ↓  (Dynamic Target Discovery)
[Ingestion — Python gdown]
        ↓  fetch .zip & extract automatically
[Data Lake — .../data_lake]
        ↓  read & clean data (.csv)
[Processing — ClickHouse Engine]
        ↓  insert-select & denormalization
[Data Warehouse — ClickHouse Marts]
        ↓  direct connection
[Dashboard — Metabase]
```
The project utilizes a containerized modern ELT data engineering blueprint, moving raw datasets out of local staging into highly optimized, denormalized Analytical Data Marts ready for BI visualization.

## 📂 Project Structure

```text
MCI-CX-ANALYSIS/
├── dags/
│   ├── scripts/
│   │   ├── 01_ingest_download_archive.py
│   │   ├── 02_ingest_extract_lake.py
│   │   ├── 03_load_to_staging.py
│   │   ├── 04_transform_to_core.py
│   │   └── 05_aggregate_to_analytics.py
│   └── customer_experience_pipeline.py
├── data_lake/
├── metabase_data/
├── .env
├── docker-compose.yml
├── dockerfile
└── requirements.txt
```
## 🛠️ Execution Guide
### Prerequisites
- Docker & Docker Compose
- Minimum 4GB RAM allocated to Docker Daemon (8GB recommended for Airflow/ClickHouse concurrent execution)

### Step 1 — Project Initialization
Run the commands:
```
git clone https://github.com/necloid/mci-cx-analysis.git
cd mci-cx-analysis
cp .env.example .env
```
### Step 2 — Launching the Infrastructure Stack
Run the commands:
```
docker-compose build
docker-compose up airflow-init
docker-compose up -d
```
### Step 3 — Accessing the Ecosystem Portals
- Apache Airflow Webserver: `http://localhost:8080` (Credentials: admin / admin or whatever is in your .env file)
- Metabase BI Client: `http://localhost:3000`
- ClickHouse Server Port: 8123 (HTTP) / 9000 (Native Client)

### Step 4 — Triggering and Testing the Workflow Pipeline
Log in to the Apache Airflow web portal (`http://localhost:8080`), click on the 'play' icon next to the delete icon to unpause the customer_experience_pipeline DAG and trigger the workflow manually to start processing.

<img width="959" height="412" alt="image" src="https://github.com/user-attachments/assets/8f9ec3fa-70b0-479d-a455-7f9f3bb3bc5f" />

To verify that the OLAP database engine is running correctly and accessible, execute the following command in your terminal to connect via the native command-line client:
```bash
docker compose exec clickhouse-server clickhouse-client --user admin --password rahasia
```
Then run this sample aggregation query inside the client prompt to verify that transactional data has been processed correctly:
```sql
SELECT 
    toYear(order_purchase_timestamp) AS purchase_year, 
    count(order_id) AS total_orders,
    round(avg(review_score), 2) AS avg_overall_rating 
FROM core.orders 
GROUP BY purchase_year 
ORDER BY purchase_year ASC;
```

To accesss the Customer Experience Analysis Dashboard, navigate to `http://localhost:3000` and click on the Sign In button. Enter the configured credentials (default setup uses the admin email and password specified during the initial setup container variables). Once logged in, select the Customer Analytics dashboard from the homepage to view the live analytics metrics.

<img width="1918" height="877" alt="image" src="https://github.com/user-attachments/assets/c5d2d599-65c6-47c5-9c60-d37fd3951cd0" />


## 🔁 Pipeline Phase 1: Ingestion & Extraction Layer
The first phase of the ELT architecture simulates an automated batch data ingestion and landing zone process, replicating how a modern enterprise data pipeline handles periodic data drops from external third-party platforms or upstream applications. Although actually consisting of two steps, both focuses on automating the retrieval of compressed archive datasets from Google Drive and extracting them directly into the local persistent Data Lake area.

### 1. Archive Ingestion (`01_ingest_download_archive.py`)
This script dynamically scans a remote Google Drive folder to locate and download the newest compressed transaction data.

* **Target Discovery & Regex Filtering:** It sends an HTTP GET request to a shared Google Drive folder link, scanning the page with Regular Expressions (`re`) to filter for files matching the pattern `Funnel_*.zip`.
* **Deterministic Sorting:** If multiple matching archive files exist in the cloud folder, the script sorts them alphabetically in descending order to automatically target and isolate the most recent batch.
* **Stream Downloading:** It utilizes the `gdown` engine to stream the selected file directly into the local data lake directory (`/opt/airflow/data_lake`), validating that the file is not empty.

### 2. Archive Extraction & Staging (`02_ingest_extract_lake.py`)
Once the compressed bundle is safely saved, this script handles unpacking the raw dataset into the local staging environment.

* **Directory Inspection & Target Isolation:** It verifies that the local data lake folder exists and scans for files beginning with `funnel_` and ending with `.zip`, prioritizing the newest file.
* **High-Throughput Extraction:** It uses Python's native `zipfile` utility to unpack all internal `.csv` data files directly into the host-mounted `data_lake/` volume.
* **Storage Conservation:** Immediately after a successful extraction, the original `.zip` archive wrapper is unlinked from the system disk via `os.remove()` to save space and prevent container storage bloat.

<img width="1312" height="502" alt="image" src="https://github.com/user-attachments/assets/d18f5da8-de03-4fbe-8281-ce33c0df163b" />

[Image Source](https://www.dataforgelabs.com/data-transformation-tools/medallion-architecture)

## 🔁 Pipeline Phase 2: Loading to ClickHouse (Staging Layer)

This phase is the first implementation of the medallion archicture which is the raw ingestion pipeline or the "bronze layer" (`03_load_to_staging.py`) by establishing a connection link to the OLAP cluster, creating an isolated `staging` database namespace, and landing all 11 core data tables.

* **Universal String Schema Landing:** The script builds tables using ClickHouse's high-performance `MergeTree()` engine, casting every raw column as a flexible `String` or `Nullable(String)`. This design completely eliminates ingestion crashes caused by dirty data formats or sudden structural adjustments in external file dumps.
* **Idempotent Storage Strategy:** Every upload sequence triggers a strict `TRUNCATE TABLE` command immediately before streaming records down from the local data lake. This guarantees total pipeline safety, allowing you to safely re-run the script multiple times without risking duplicated data records or messy, partial writes.
* **Memory-Protected Stream Chunking:** Data is processed and pushed to ClickHouse in sequential blocks restricted to a `chunksize` of 50,000 records via Pandas. This setup establishes a fixed, highly predictable memory footprint that insulates the Docker container from **Out-Of-Memory (OOM)** failures when handling massive transaction assets.
* **Zero Sorting Write Optimization:** Tables are purposefully initialized with an empty sorting key (`ORDER BY tuple()`), which allows ClickHouse to bypass heavy file-sorting overhead and append millions of incoming rows directly to disk instantly. An automated `SELECT count()` check runs at the end of each block to guarantee zero data loss.

**Why choose ELT over ETL for this project?** Well, architectural efficiency and computing power. In a traditional ETL model, transformations rely entirely on Python memory and the container's CPU to process data before loading it. While a framework like PySpark could handle this, it is massive overkill for this architecture; PySpark requires a distributed cluster infrastructure, heavy JVM (Java Virtual Machine) overhead, and complex worker node coordination just to run basic transformations. By switching to an ELT pattern, we can avoid that. We load the raw data into ClickHouse instantly, then offload the heavy transformations onto ClickHouse’s native columnar engine. Because ClickHouse processes data in massive column arrays directly inside the CPU cache using vectorized execution, it completes these analytical transformations exponentially faster, while keeping our Docker container stack lightweight and resource-efficient.

## 🔁 Pipeline Phase 3: Core Transformation Layer (Core Layer)

This phase which is the "silver layer" handles the data cleaning, type-casting, and core optimization process (`04_transform_to_core.py`). It moves data out of the unformatted `staging` namespace and structures it into a production-ready `core` database schema.

### Data Cleaning Steps
* **Strict Type Casting:** Converts raw text strings into specific numerical and decimal formats (e.g., `toUInt8` for review scores, `toFloat64` for order prices) to enable high-speed analytical calculations.
* **Null Handling & Empty String Parsing:** Scans fields for empty text markers (`''`) and converts them into explicit database `NULL` values. This ensures accurate missing-value calculations across critical operational indicators like delivery dates or client revenue.
* **Timestamp Normalization:** Leverages ClickHouse’s native `parseDateTimeBestEffort` engine function to parse variable date formats into proper, uniform `DateTime` metrics.

### Optimizations for Efficiency
* **Low-Cardinality Storage Mapping:** Frequently repeated dimension values such as country states (`customer_state`) or order updates (`order_status`) are structured using the `LowCardinality(String)` constraint. This acts as an internal dictionary lookup, saving massive disk blocks and accelerating downstream dashboard filters.
* **Primary Key Index Clustering:** Every target table is built with optimized indexing arrays (`ORDER BY order_purchase_timestamp`, `ORDER BY customer_id`). This sorts the data physically on disk to ensure instantaneous time-series aggregations and lightning-fast key lookups.
* **Idempotent Insertion Safety:** The script triggers a explicit `TRUNCATE TABLE` command directly before running the core SQL transformation queries. This guarantees a safe workflow loop, meaning re-running the phase reconstructs a pristine core warehouse layer without causing duplicated records.

## 🔁 Pipeline Phase 4: Aggregation Layer (Analytics Layer)

This phase or the "Gold Layer" compiles the production-ready analytical data marts (`05_materialize_analytics.py`). It reads clean data from the `core` namespace and uses ClickHouse's high-performance columnar engine to materialize 4 distinct data marts designed for direct BI dashboard consumption.

### Analytics Tables & Core Metrics

#### 1. Customer Experience Sentiment Mart (`mart_customer_experience_sentiment`)

This mart isolates transaction reviews and uses text-pattern scanning alongside numerical ratings to derive qualitative customer feedback insights.

* **Sentiment Category (`sentiment_category`):** Runs a native text matching function (`multiSearchAny`) over Portuguese feedback strings to label text as *Positive*, *Negative*, or *Neutral*. If no written feedback exists, it defaults to evaluating the numerical `review_score`.
* **Root-Cause Extraction (`customer_feedback_topic`):** For negative sentiment, it groups the underlying complaint into concrete operational friction buckets such as *Logistics (Delay)*, *Warehouse (Missing Items)*, or *Product Quality (Defect)* based on targeted phrase maps.

#### 2. Delivery & SLA Fulfillment Lags Impact Mart (`mart_cx_delivery_impact`)

This table maps shipping fulfillment timelines against client ratings to quantify exactly how fulfillment efficiency influences brand loyalty.

* **SLA Breach Flag (`is_late`):** A boolean metric that flags orders where the actual customer delivery date exceeded the maximum promised delivery timeline.
* **Transit Segmentation (`seller_handling_days` & `carrier_transit_days`):** Uses precise timestamp gaps (`dateDiff`) to isolate exactly how long a package sat with the seller versus how long it spent in carrier transit.
* **Freight Value Percentage (`freight_value_pct`):** Evaluates price structures by calculating the exact ratio of shipping costs relative to the raw item value.

#### 3. Product Attributes & Seller Quality Insights Mart (`mart_cx_product_and_seller_quality`)

This model tracks product catalog specifications against aggregated merchant performance scores to identify bad actors or high-performing vendors.

* **Seller Bayesian Rating (`seller_bayesian_rating`):** Rather than using a naive mathematical average, this applies a Bayesian smoothing formula with a threshold constraint ($m=15$). This prevents low-volume sellers with a single five-star review from artificially outranking high-volume veteran merchants.
* **Late Shipping Rate (`seller_late_shipping_rate`):** Calculates the precise percentage of line items where a seller missed their strict fulfillment shipping deadline.

#### 4. Regional Logistics & Freight Cost Gaps Mart (`mart_cx_regional_logistics_gaps`)

A multi-dimensional geographical aggregation table that exposes operational shipping and cost imbalances across different states and municipalities.

* **Late Order Ratio (`pct_orders_late`):** The percentage of localized orders failing to meet estimated delivery targets within a specific city/state perimeter.
* **Interstate Shipment Ratio (`pct_interstate_shipments`):** Tracks supply chain distribution by calculating the percentage of orders fulfilled by out-of-state vendors versus local merchants.

## 📊 Pipeline Phase 5: BI Dashboard (Metabase Integration)

The final layer of the architecture connects the materialized `analytics` database marts directly to Metabase for executive visualization. Because Metabase is connected directly to the ClickHouse OLAP cluster, it bypasses complex, slow BI processing loops and leverages the pre-aggregated Gold tables for real-time dashboard performance. The attached images below is the real-time BI dashboard result:
<img width="1572" height="1998" alt="img23" src="https://github.com/user-attachments/assets/cc5bb393-d936-4481-aff5-0838c693d874" />
<img width="1572" height="2001" alt="img24" src="https://github.com/user-attachments/assets/f3f9375f-d420-425d-900d-487159df9495" />
<img width="1572" height="423" alt="img25" src="https://github.com/user-attachments/assets/57fca0ae-70fd-47f7-a33f-c6f51858e10e" />

## 🎯 Strategic Next Actions
Based on the dashboard results, these are strategic next steps DustiniaDelixia Groceria can take to improve thier business:
* **Maintain Continuous Data Audits:** Establish a routine schedule to regularly review pipeline data health, ensuring that your tracking metrics naturally adapt whenever customer behavior or backend systems change.
* **Spread Out Warehouses:** Build smaller storage centers closer to where most customers live so packages travel shorter distances and arrive faster as the business grows.
* **Double-Check Inventory Early:** Link stock tracking to the checkout system so errors, like missing items or wrong packages, are caught before they ever leave the building.
* **Keep Sellers Accountable:** Use a smart rating system that balances out low sales volumes to automatically reward great sellers and flag consistently slow ones.

## 🏁 Conclusion
Doing this final project was pretty tough, especially when trying to write complex queries to sort through thousands of messy customer reviews and translating advanced tracking math required a lot of trial and error especially since this is all very new to me. There are also external hurdles such as me unexpectedly getting very sick for 5 days which really hindered my progress, with finals going on as well I almost wanted to give up honestly. But I realized all the hours and days I spent learning and building this project, and the ambition I had to be part of MCI Lab. So I pushed forward and finished it, even if the results weren't up to my initial expectations :^) (not to mention I forgot to push this readme, haha)

Despite these hardships, solving these problems taught me how real data flows from a raw, unorganized state into clean data centers and useful dashboard metrics. I gained invaluable hands-on experience with modern, high-performance columnar databases like ClickHouse, seeing firsthand how they can process massive amounts of data exponentially faster than traditional systems. Ultimately, this project showed me that data engineering is could help build the infrastructure that helps a company make smart, real-world business choices. I realized that I liked experimenting and looking up articles in this field a lot, and I might try pursue it in the future.

Nonetheless, thank you MCI Admins for the opportunity!

![cat](https://i.pinimg.com/originals/c3/35/53/c33553a6442d05f4eeea7b000e9d4245.gif)





