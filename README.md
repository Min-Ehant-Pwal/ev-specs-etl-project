# ev-specs-etl-project

# EV Data Engineering Pipeline  
### Automated ETL: Web Scraping → Bronze → Silver → Gold → Analytics

This project implements a fully automated **data engineering pipeline** that extracts Electric Vehicle (EV) specifications from a public website, processes the data through a multi-layered warehouse architecture, and produces an analytics-ready dataset suitable for business intelligence tools.

The system is designed using industry-standard data engineering patterns and emphasizes reproducibility, automation, and clear separation of concerns across pipeline layers.

---

# 📚 Project Overview

The pipeline performs the following operations:

1. **Web Scraping** – Extract vehicle data directly from EV-Database.org  
2. **Bronze Layer (Raw Zone)** – Store raw scraped data with minimal processing  
3. **Silver Layer (Clean Zone)** – Standardize formats, convert data types, resolve inconsistencies, and extract clean numerical values  
4. **Gold Layer (Analytics Zone)** – Generate validated fact tables, compute performance/value metrics, and create summary aggregates  
5. **Analytical Views** – Prepare a set of SQL views for Power BI or SQL-based analytics  
6. **Logging** – Capture detailed execution logs for each layer  

The entire pipeline can be executed end-to-end using:
python run_pipeline.py

All databases, tables, and schemas are created automatically on first run.

---

# 📁 Project Structure

<pre>
EV_Data_Engineering_Pipeline/
|
|-- scraping/
|     |-- scrapeAllTest.py
|
|-- bronze/
|     |-- bronzeDDL.py
|     |-- proc_load_bronze.py
|
|-- silver/
|     |-- silver_load.py
|
|-- gold/
|     |-- goldDDL.py
|     |-- gold_load.py
|     |-- gold_views.sql
|
|-- run_pipeline.py
|
|-- logs/
      |-- (auto-generated execution logs)
</pre>



---

# 🛠️ System Requirements

### Software
- Python 3.10 or newer  
- MySQL Server 8+  
- MySQL Workbench (optional, for inspection)

### Python Packages
Install dependencies:
pip install mysql-connector-python pandas requests beautifulsoup4 (if failed)
python pip -m install mysql-connector-python pandas requests beautifulsoup4

---

# 🗄️ Database Setup

No manual database creation is required.  
The ETL scripts will automatically create:

- `DataWarehouse_bronze`
- `DataWarehouse_silver`
- `DataWarehouse_gold`

Ensure the MySQL root credentials inside the scripts match your environment:
USER = "root"
PASSWORD = "your_mysql_password"(change as needed)

---

# 🚀 Running the Full Pipeline

Execute:
python run_pipeline.py

This orchestrates:

1. **Scraping** – Generates `scrapedData.csv`  
2. **Bronze Load** – Loads raw scraped data  
3. **Silver Load** – Cleans and transforms raw attributes  
4. **Gold Load** – Generates metric tables + brand aggregates  
5. **View Creation** – Creates analytical SQL views  
6. **Logging** – Writes full logs to `/logs/` folder  

If any layer fails, the pipeline halts and logs the error.

---

# 🔍 Data Warehouse Layers

### 🥉 **Bronze Layer (Raw Zone)**
- Direct load of scraped CSV
- Minimal validation
- Preserves source structure
- Useful for debugging and data lineage

### 🥈 **Silver Layer (Clean Zone)**
- Type conversions (e.g., text → INT/DECIMAL)
- Cleaning of:
  - battery sizing
  - weight
  - acceleration times
  - charging rates
  - prices (extract numeric from £xx,xxx)
- Normalization of categorical fields:
  - drivetrain: AWD, FWD, RWD
  - class: mini/compact/medium/etc.

### 🥇 **Gold Layer (Analytics Zone)**
- Business-ready fact table with metrics:
  - `price_per_kwh`
  - `price_per_mile`
  - `value_score` (range/price)
  - `performance_score` (1/0-62)
  - `efficiency_score` (1/Wh per mile)
  - `charging_score` (kW/kWh)
  - `price_per_weight`
- Brand-level summary table
- Used for dashboards and advanced analytics

---

# 📊 Analytical Views (Power BI Ready)

The file `gold/gold_views.sql` includes:

- `vw_ev_analytics`  
- `vw_top_value_evs`  
- `vw_best_price_per_kwh`  
- `vw_top_performance`  
- `vw_best_efficiency`  
- `vw_range_vs_price`  
- `vw_brand_averages`  
- `vw_best_price_per_weight`  
- and more  

Run the views by executing:
USE DataWarehouse_gold;
SOURCE gold_views.sql;

---

# 🧪 Validation & Testing

After running the pipeline, validate the output using:

```sql
SELECT * FROM DataWarehouse_bronze.ev_specs_bronze LIMIT 5;
SELECT * FROM DataWarehouse_silver.silver_specs LIMIT 5;
SELECT * FROM DataWarehouse_gold.gold_ev_summary LIMIT 5;
SELECT * FROM DataWarehouse_gold.vw_ev_analytics LIMIT 5;
```
You should see:
Clean numeric fields
Correct price extraction
No duplicates
Properly populated derived metrics

---

🧾 Logging
Each ETL script produces a timestamped log file under /logs/.
Logs include:
start/end times
execution details
SQL errors
Python errors
row counts per layer
This ensures full traceability and reproducibility.

---

🔧 Troubleshooting

MySQL: Authentication Error
Ensure the MySQL service is running and the password in scripts is correct.

Scraping returns 0 rows
The website structure may have changed; update selectors in scrapeAllTest.py.

price_gbp is NULL
Ensure you are using the latest silver_load.py with the correct price extraction logic.

Unicode/Encoding Errors in Logs
Windows users should keep all log files in UTF-8 encoding.

---

👤 Author

Min Ehant Pwal
Final-Year Data Engineering Project
Teesside University @ MDIS Singapore
