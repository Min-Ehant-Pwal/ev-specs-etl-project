import mysql.connector
import traceback
import os
from datetime import datetime

# ============================
# Logging setup
# ============================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = f"gold_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(LOG_DIR, log_filename)

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def safe_execute(cursor, sql, step="UNKNOWN"):
    try:
        cursor.execute(sql)
    except Exception as e:
        log(f" ERROR during {step}: {e}")
        traceback.print_exc()
        raise

# ============================
# MYSQL CONNECTION
# ============================

log("Starting Gold Load...")

conn = mysql.connector.connect(
    host="localhost",
    user="EV_specs",
    password="MDIS@2025"
)
cursor = conn.cursor()

safe_execute(cursor, "CREATE DATABASE IF NOT EXISTS DataWarehouse_gold;", "CREATE DB")
safe_execute(cursor, "USE DataWarehouse_gold;", "USE DataWarehouse_gold")

# Clear tables
safe_execute(cursor, "TRUNCATE TABLE gold_ev_summary;", "TRUNCATE gold_ev_summary")
safe_execute(cursor, "TRUNCATE TABLE gold_brand_summary;", "TRUNCATE gold_brand_summary")

# ============================
# LOAD gold_ev_summary
# ============================

log("Loading gold_ev_summary...")

ev_sql = """
INSERT INTO gold_ev_summary (
    manufacturer_name,
    model_name,
    drivetrain,
    class,
    seat,
    range_miles,
    battery_kwh,
    efficiency_whpm,
    zero_to_sixty_sec,
    weight_kg,
    rapidcharge_kw,
    towing_kg,
    boot_space_liters,
    price_gbp,
    price_per_kwh,
    price_per_mile,
    value_score,
    performance_score,
    efficiency_score,
    charging_score,
    price_per_weight
)
SELECT
    m.manufacturer_name,
    v.model_name,
    v.drivetrain,
    v.class,
    v.seat,
    s.range_miles,
    s.battery_kwh,
    s.efficiency_whpm,
    s.zero_to_sixty_sec,
    s.weight_kg,
    s.rapidcharge_kw,
    s.towing_kg,
    s.boot_space_liters,
    s.price_gbp,

    -- price per kWh
    CASE WHEN s.price_gbp > 0 AND s.battery_kwh > 0
        THEN ROUND(s.price_gbp / s.battery_kwh, 2)
        ELSE NULL END,

    -- price per mile
    CASE WHEN s.price_gbp > 0 AND s.range_miles > 0
        THEN ROUND(s.price_gbp / s.range_miles, 2)
        ELSE NULL END,

    -- value score
    CASE WHEN s.price_gbp > 0 AND s.range_miles > 0
        THEN ROUND(s.range_miles / s.price_gbp, 4)
        ELSE NULL END,

    -- performance score
    CASE WHEN s.zero_to_sixty_sec > 0
        THEN ROUND(1 / s.zero_to_sixty_sec, 4)
        ELSE NULL END,

    -- efficiency score
    CASE WHEN s.efficiency_whpm > 0
        THEN ROUND(1 / s.efficiency_whpm, 4)
        ELSE NULL END,

    -- charging score
    CASE WHEN s.battery_kwh > 0 AND s.rapidcharge_kw > 0
        THEN ROUND(s.rapidcharge_kw / s.battery_kwh, 4)
        ELSE NULL END,

    -- price per kg
    CASE WHEN s.weight_kg > 0 AND s.price_gbp > 0
        THEN ROUND(s.price_gbp / s.weight_kg, 4)
        ELSE NULL END

FROM DataWarehouse_silver.silver_vehicle v
JOIN DataWarehouse_silver.silver_manufacturer m
    ON v.manufacturer_id = m.manufacturer_id
JOIN DataWarehouse_silver.silver_specs s
    ON s.vehicle_id = v.vehicle_id;
"""

safe_execute(cursor, ev_sql, "INSERT gold_ev_summary")
conn.commit()

cursor.execute("SELECT COUNT(*) FROM gold_ev_summary;")
log(f"gold_ev_summary rows inserted: {cursor.fetchone()[0]}")

# ============================
# LOAD gold_brand_summary
# ============================

log("Loading gold_brand_summary...")

brand_sql = """
INSERT INTO gold_brand_summary (
    manufacturer_name,
    model_count,
    avg_price_gbp,
    avg_range_miles,
    avg_battery_kwh,
    avg_efficiency_whpm,
    avg_zero_to_sixty_sec,
    min_price_gbp,
    max_price_gbp
)
SELECT
    manufacturer_name,
    COUNT(*),
    ROUND(AVG(price_gbp), 2),
    ROUND(AVG(range_miles), 2),
    ROUND(AVG(battery_kwh), 2),
    ROUND(AVG(efficiency_whpm), 2),
    ROUND(AVG(zero_to_sixty_sec), 2),
    MIN(price_gbp),
    MAX(price_gbp)
FROM gold_ev_summary
WHERE price_gbp IS NOT NULL
GROUP BY manufacturer_name;
"""

safe_execute(cursor, brand_sql, "INSERT gold_brand_summary")
conn.commit()

cursor.execute("SELECT COUNT(*) FROM gold_brand_summary;")
log(f"gold_brand_summary rows inserted: {cursor.fetchone()[0]}")

cursor.close()
conn.close()

log("Gold Load completed successfully.")

