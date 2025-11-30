import mysql.connector
import traceback
from datetime import datetime
import os

# =====================================================
# LOGGING
# =====================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_filename = f"silver_layer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(LOG_DIR, log_filename)

def write_log(msg):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    write_log(line)

def now():
    return datetime.now()

# =====================================================
# SAFE SQL
# =====================================================
def safe_execute(cursor, sql, params=None, step_name="UNKNOWN"):
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
    except mysql.connector.Error as e:
        log("==============================================")
        log(f" SQL ERROR DURING STEP: {step_name}")
        log("----------------------------------------------")
        log(f"Message     : {e.msg}")
        log(f"Error Code  : {e.errno}")
        log("----------------------------------------------")
        log("FAILED SQL:")
        log(sql)
        traceback.print_exc()
        log("==============================================")
        raise e

# =====================================================
# MYSQL
# =====================================================

HOST = "localhost"
USER = "root"
PASSWORD = "88888888"

log("Starting Silver Layer ETL...")

conn = mysql.connector.connect(
    host=HOST,
    user=USER,
    password=PASSWORD
)
cursor = conn.cursor()

# =====================================================
# PREP SCHEMA
# =====================================================
log("Preparing schema: DataWarehouse_silver")

safe_execute(cursor, "CREATE DATABASE IF NOT EXISTS DataWarehouse_silver;")
safe_execute(cursor, "DROP TABLE IF EXISTS DataWarehouse_silver.silver_specs;")
safe_execute(cursor, "DROP TABLE IF EXISTS DataWarehouse_silver.silver_vehicle;")
safe_execute(cursor, "DROP TABLE IF EXISTS DataWarehouse_silver.silver_manufacturer;")
conn.commit()

log("Creating Silver tables...")

safe_execute(cursor, """
CREATE TABLE DataWarehouse_silver.silver_manufacturer (
    manufacturer_id INT AUTO_INCREMENT PRIMARY KEY,
    manufacturer_name VARCHAR(100) UNIQUE
);
""")

safe_execute(cursor, """
CREATE TABLE DataWarehouse_silver.silver_vehicle (
    vehicle_id INT AUTO_INCREMENT PRIMARY KEY,
    manufacturer_id INT,
    model_name VARCHAR(200),
    drivetrain VARCHAR(50),
    class VARCHAR(50),
    seat INT,
    FOREIGN KEY (manufacturer_id)
        REFERENCES DataWarehouse_silver.silver_manufacturer(manufacturer_id)
);
""")

safe_execute(cursor, """
CREATE TABLE DataWarehouse_silver.silver_specs (
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
        REFERENCES DataWarehouse_silver.silver_vehicle(vehicle_id)
);
""")

conn.commit()

log("Tables created.\n")

# =====================================================
# LOAD MANUFACTURER
# =====================================================

log("Loading: silver_manufacturer")

safe_execute(cursor, """
INSERT INTO DataWarehouse_silver.silver_manufacturer (manufacturer_name)
SELECT DISTINCT TRIM(company)
FROM DataWarehouse_bronze.ev_specs_bronze
WHERE company IS NOT NULL AND TRIM(company) <> '';
""")

conn.commit()

cursor.execute("SELECT COUNT(*) FROM DataWarehouse_silver.silver_manufacturer")
log(f"Inserted {cursor.fetchone()[0]} manufacturers.\n")

# =====================================================
# LOAD VEHICLE
# =====================================================

log("Loading: silver_vehicle")

safe_execute(cursor, """
INSERT INTO DataWarehouse_silver.silver_vehicle (
    manufacturer_id, model_name, drivetrain, class, seat
)
SELECT
    m.manufacturer_id,
    b.model,
    CASE 
        WHEN TRIM(b.drivetrain) = 'All Wheel Drive'  THEN 'AWD'
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
JOIN DataWarehouse_silver.silver_manufacturer m
    ON TRIM(m.manufacturer_name) = TRIM(b.company);
""")

conn.commit()

cursor.execute("SELECT COUNT(*) FROM DataWarehouse_silver.silver_vehicle")
log(f"Inserted {cursor.fetchone()[0]} vehicles.\n")

# =====================================================
# LOAD SILVER SPECS (FIXED PRICE)
# =====================================================

log("Loading: silver_specs")

safe_execute(cursor, "TRUNCATE TABLE DataWarehouse_silver.silver_specs;")

silver_specs_sql = """
INSERT INTO DataWarehouse_silver.silver_specs (
    vehicle_id, range_miles, efficiency_whpm, weight_kg,
    zero_to_sixty_sec, one_stop_range_miles, battery_kwh,
    rapidcharge_kw, towing_kg, boot_space_liters,
    price_per_mile, price_gbp
)
SELECT
    v.vehicle_id,

    CAST(NULLIF(REGEXP_REPLACE(b.range_raw, '[^0-9.]', ''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.efficiency, '[^0-9.]', ''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.weight, '[^0-9.]', ''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.zero_to_sixty, '[^0-9.]', ''), '') AS DECIMAL(4,2)),
    CAST(NULLIF(REGEXP_REPLACE(b.one_stop_range, '[^0-9.]', ''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.battery, '[^0-9.]', ''), '') AS DECIMAL(5,2)),
    CAST(NULLIF(REGEXP_REPLACE(b.rapidcharge,'[^0-9.]', ''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.towing,   '[^0-9.]', ''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.boot_space,'[^0-9.]', ''), '') AS UNSIGNED),
    CAST(NULLIF(REGEXP_REPLACE(b.price_range,'[^0-9.]', ''), '') AS UNSIGNED),
    CAST(
        NULLIF(
            REPLACE(REPLACE(REPLACE(REPLACE(b.price_raw, '£',''), ',', ''), ' ', ''), '*', ''),
        '') AS UNSIGNED
    ) AS price_gbp

FROM DataWarehouse_bronze.ev_specs_bronze b
JOIN DataWarehouse_silver.silver_vehicle v
    ON TRIM(v.model_name) = TRIM(b.model);
"""

safe_execute(cursor, silver_specs_sql)
conn.commit()

cursor.execute("SELECT COUNT(*) FROM DataWarehouse_silver.silver_specs")
log(f"Inserted {cursor.fetchone()[0]} spec rows.\n")

# =====================================================
# DONE
# =====================================================

cursor.close()
conn.close()

log("Silver ETL Finished.")
