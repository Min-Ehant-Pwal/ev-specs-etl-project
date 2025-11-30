import mysql.connector
import os
from datetime import datetime

# ============================
# Logging setup
# ============================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = f"goldDDL_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(LOG_DIR, log_filename)

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ============================
# MYSQL CONNECTION
# ============================

HOST = "localhost"
USER = "root"
PASSWORD = "88888888"

conn = mysql.connector.connect(
    host=HOST,
    user=USER,
    password=PASSWORD
)
cursor = conn.cursor()

log("Starting Gold DDL...")

# ============================
# CREATE DATABASE
# ============================

cursor.execute("CREATE DATABASE IF NOT EXISTS DataWarehouse_gold;")
cursor.execute("USE DataWarehouse_gold;")
log("Database created/selected: DataWarehouse_gold")

# Drop tables if exist
cursor.execute("DROP TABLE IF EXISTS gold_brand_summary;")
cursor.execute("DROP TABLE IF EXISTS gold_ev_summary;")

# ============================
# CREATE gold_ev_summary TABLE
# ============================

ev_summary_sql = """
CREATE TABLE gold_ev_summary (
    ev_id INT AUTO_INCREMENT PRIMARY KEY,
    manufacturer_name VARCHAR(100),
    model_name VARCHAR(200),
    drivetrain VARCHAR(50),
    class VARCHAR(50),
    seat INT,
    range_miles INT,
    battery_kwh DECIMAL(5,2),
    efficiency_whpm INT,
    zero_to_sixty_sec DECIMAL(4,2),
    weight_kg INT,
    rapidcharge_kw INT,
    towing_kg INT,
    boot_space_liters INT,
    price_gbp INT,

    -- Core metrics
    price_per_kwh DECIMAL(10,2),
    price_per_mile DECIMAL(10,2),

    -- Analytic metrics
    value_score DECIMAL(10,4),
    performance_score DECIMAL(10,4),
    efficiency_score DECIMAL(10,4),
    charging_score DECIMAL(10,4),
    price_per_weight DECIMAL(10,4)
);
"""
cursor.execute(ev_summary_sql)
log("Created table: gold_ev_summary")

# ============================
# CREATE gold_brand_summary TABLE
# ============================

brand_summary_sql = """
CREATE TABLE gold_brand_summary (
    brand_id INT AUTO_INCREMENT PRIMARY KEY,
    manufacturer_name VARCHAR(100),
    model_count INT,
    avg_price_gbp DECIMAL(10,2),
    avg_range_miles DECIMAL(10,2),
    avg_battery_kwh DECIMAL(10,2),
    avg_efficiency_whpm DECIMAL(10,2),
    avg_zero_to_sixty_sec DECIMAL(10,2),
    min_price_gbp INT,
    max_price_gbp INT
);
"""
cursor.execute(brand_summary_sql)
log("Created table: gold_brand_summary")

conn.commit()
cursor.close()
conn.close()

log("Gold DDL completed successfully.")
