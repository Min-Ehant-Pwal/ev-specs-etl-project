import mysql.connector
import traceback
from datetime import datetime
import os

# =====================================================
# DYNAMIC LOGGING SETUP
# =====================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# timestamped log file
log_filename = f"silver_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(LOG_DIR, log_filename)

def write_log(msg):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    write_log(line)

# =====================================================
# SAFE SQL EXECUTION
# =====================================================

def safe_execute(cursor, sql, params=None, step_name="UNKNOWN"):
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
    except mysql.connector.Error as e:
        log("==============================================")
        log(f"SQL ERROR during {step_name}")
        log(f"Message: {e.msg}")
        log(f"Error Code: {e.errno}")
        log("FAILED SQL:")
        log(sql)
        traceback.print_exc()
        log("==============================================")
        raise e
    except Exception as ex:
        log("==============================================")
        log(f"PYTHON ERROR during {step_name}")
        log(str(ex))
        traceback.print_exc()
        log("==============================================")
        raise ex

# =====================================================
# MYSQL CONNECTION
# =====================================================

log("Starting Silver Layer ETL...")

try:
    conn = mysql.connector.connect(
    host="localhost",
    user="EV_specs",
    password="MDIS@2025"
    )
    cursor = conn.cursor()
except Exception as conn_err:
    log("FAILED TO CONNECT TO MYSQL")
    log(str(conn_err))
    raise conn_err

# =====================================================
# PREPARE DATABASE
# =====================================================

safe_execute(cursor, "CREATE DATABASE IF NOT EXISTS DataWarehouse_silver;", step_name="CREATE DATABASE")
safe_execute(cursor, "USE DataWarehouse_silver;", step_name="USE DATABASE")

log("Dropping old Silver tables...")
safe_execute(cursor, "SET FOREIGN_KEY_CHECKS = 0;", step_name="DISABLE FK")
safe_execute(cursor, "DROP TABLE IF EXISTS silver_specs;", step_name="DROP silver_specs")
safe_execute(cursor, "DROP TABLE IF EXISTS silver_vehicle;", step_name="DROP silver_vehicle")
safe_execute(cursor, "DROP TABLE IF EXISTS silver_manufacturer;", step_name="DROP silver_manufacturer")
safe_execute(cursor, "SET FOREIGN_KEY_CHECKS = 1;", step_name="ENABLE FK")

conn.commit()

# =====================================================
# CREATE TABLES
# =====================================================

log("Creating Silver tables...")

safe_execute(cursor, """
CREATE TABLE silver_manufacturer (
    manufacturer_id INT AUTO_INCREMENT PRIMARY KEY,
    manufacturer_name VARCHAR(100) UNIQUE
);
""", step_name="CREATE silver_manufacturer")

safe_execute(cursor, """
CREATE TABLE silver_vehicle (
    vehicle_id INT AUTO_INCREMENT PRIMARY KEY,
    manufacturer_id INT,
    model_name VARCHAR(200),
    drivetrain VARCHAR(50),
    class VARCHAR(50),
    seat INT,
    FOREIGN KEY (manufacturer_id)
        REFERENCES silver_manufacturer(manufacturer_id)
);
""", step_name="CREATE silver_vehicle")

safe_execute(cursor, """
CREATE TABLE silver_specs (
    spec_id INT AUTO_INCREMENT PRIMARY KEY,
    vehicle_id INT,
    range_miles INT,
    efficiency_whpm INT,
    weight_kg INT,
    zero_to_sixty_sec DECIMAL(4,2),
    one_stop_range_miles INT,
    battery_kwh DECIMAL(5,2),
    rapidcharge_kw INT,
    towing_kg INT,
    boot_space_liters INT,
    price_per_mile INT,
    price_gbp INT,
    FOREIGN KEY (vehicle_id)
        REFERENCES silver_vehicle(vehicle_id)
);
""", step_name="CREATE silver_specs")

conn.commit()

log("Silver tables created.")

# =====================================================
# LOAD MANUFACTURERS
# =====================================================

log("Loading: silver_manufacturer")

safe_execute(cursor, """
INSERT INTO silver_manufacturer (manufacturer_name)
SELECT DISTINCT TRIM(company)
FROM DataWarehouse_bronze.ev_specs_bronze
WHERE company IS NOT NULL AND TRIM(company) <> '';
""", step_name="INSERT manufacturers")

conn.commit()

cursor.execute("SELECT COUNT(*) FROM silver_manufacturer")
log(f"Manufacturers inserted: {cursor.fetchone()[0]}")

# =====================================================
# LOAD VEHICLES
# =====================================================

log("Loading: silver_vehicle")

safe_execute(cursor, """
INSERT INTO silver_vehicle (
    manufacturer_id, model_name, drivetrain, class, seat
)
SELECT
    m.manufacturer_id,
    b.model,
    CASE 
        WHEN TRIM(b.drivetrain) = 'All Wheel Drive' THEN 'AWD'
        WHEN TRIM(b.drivetrain) = 'Rear Wheel Drive' THEN 'RWD'
        WHEN TRIM(b.drivetrain) = 'Rare Wheel Drive' THEN 'RWD'
        WHEN TRIM(b.drivetrain) = 'Front Wheel Drive' THEN 'FWD'
        ELSE TRIM(b.drivetrain)
    END,
    CASE
        WHEN UPPER(TRIM(b.class)) = 'A' THEN 'mini'
        WHEN UPPER(TRIM(b.class)) = 'B' THEN 'compact'
        WHEN UPPER(TRIM(b.class)) = 'C' THEN 'medium'
        WHEN UPPER(TRIM(b.class)) = 'D' THEN 'large'
        WHEN UPPER(TRIM(b.class)) = 'E' THEN 'executive'
        WHEN UPPER(TRIM(b.class)) = 'F' THEN 'luxury'
        WHEN UPPER(TRIM(b.class)) = 'N' THEN 'passenger van'
        WHEN UPPER(TRIM(b.class)) = 'S' THEN 'sports'
        ELSE TRIM(b.class)
    END,
    b.seat
FROM DataWarehouse_bronze.ev_specs_bronze b
JOIN silver_manufacturer m ON TRIM(m.manufacturer_name) = TRIM(b.company);
""", step_name="INSERT vehicles")

conn.commit()

cursor.execute("SELECT COUNT(*) FROM silver_vehicle")
log(f"Vehicles inserted: {cursor.fetchone()[0]}")

# =====================================================
# LOAD SPECS (NO DEDUPE)
# =====================================================

log("Loading: silver_specs")

silver_specs_sql = """
INSERT INTO silver_specs (
    vehicle_id, 
    range_miles, 
    efficiency_whpm, 
    weight_kg,
    zero_to_sixty_sec, 
    one_stop_range_miles, 
    battery_kwh,
    rapidcharge_kw, 
    towing_kg, 
    boot_space_liters, 
    price_per_mile,
    price_gbp
)
SELECT
    v.vehicle_id,
    CAST(NULLIF(REGEXP_REPLACE(b.range_raw, '[^0-9.]', ''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.efficiency, '[^0-9.]', ''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.weight, '[^0-9.]', ''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.zero_to_sixty, '[^0-9.]',''), '') AS DECIMAL(4,2)),
    CAST(NULLIF(REGEXP_REPLACE(b.one_stop_range, '[^0-9.]',''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.battery, '[^0-9.]', ''), '') AS DECIMAL(5,2)),
    CAST(NULLIF(REGEXP_REPLACE(b.rapidcharge,'[^0-9.]',''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.towing,'[^0-9.]',''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.boot_space,'[^0-9.]',''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.price_range,'[^0-9.]',''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.price_raw,'[^0-9]',''), '') AS UNSIGNED)
FROM DataWarehouse_bronze.ev_specs_bronze b
JOIN silver_vehicle v
    ON TRIM(v.model_name) = TRIM(b.model);
"""

safe_execute(cursor, silver_specs_sql, step_name="INSERT specs")
conn.commit()

cursor.execute("SELECT COUNT(*) FROM silver_specs")
log(f"Specs inserted: {cursor.fetchone()[0]}")

# =====================================================
# DONE
# =====================================================

cursor.close()
conn.close()

log("Silver layer ETL completed successfully.")

