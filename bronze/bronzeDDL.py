import mysql.connector
import os
from datetime import datetime

# ========= LOGGING SETUP =========
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"bronzeDDL_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(log_file, "a") as f:
        f.write(line + "\n")

# ========= MYSQL CONNECTION =========
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="88888888"
)
cursor = conn.cursor()

log("Starting Bronze DDL...")

# ========= CREATE DB =========
cursor.execute("CREATE DATABASE IF NOT EXISTS DataWarehouse_bronze;")
log("Database DataWarehouse_bronze ensured.")

cursor.execute("USE DataWarehouse_bronze;")

# ========= DROP OLD TABLE =========
cursor.execute("DROP TABLE IF EXISTS ev_specs_bronze;")
log("Dropped old ev_specs_bronze table.")

# ========= CREATE NEW TABLE (UPDATED WITH price_raw) =========
create_sql = """
CREATE TABLE ev_specs_bronze (
    id INT AUTO_INCREMENT PRIMARY KEY,
    company VARCHAR(100),
    model VARCHAR(200),
    drivetrain VARCHAR(100),
    class VARCHAR(100),
    seat VARCHAR(20),
    price_raw VARCHAR(50),
    range_raw VARCHAR(50),
    efficiency VARCHAR(50),
    weight VARCHAR(50),
    zero_to_sixty VARCHAR(50),
    one_stop_range VARCHAR(50),
    battery VARCHAR(50),
    rapidcharge VARCHAR(50),
    towing VARCHAR(50),
    boot_space VARCHAR(50),
    price_range VARCHAR(50)
);
"""

cursor.execute(create_sql)
conn.commit()

log("Bronze table created successfully")
cursor.close()
conn.close()