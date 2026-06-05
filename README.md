#  🛍️ E-Commerce Customer Experience Analysis
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
