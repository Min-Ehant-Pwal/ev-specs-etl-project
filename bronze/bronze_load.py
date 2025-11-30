import mysql.connector
import pandas as pd
import os
from datetime import datetime
import traceback

# ========= LOGGING =========
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_path = os.path.join(LOG_DIR, f"bronze_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(log_path, "a") as f:
        f.write(line + "\n")

# ========= SAFE EXECUTION HELPERS =========
def safe_execute(cursor, sql, params=None, step="UNKNOWN"):
    try:
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
    except Exception as e:
        log(f" ERROR during {step}: {e}")
        traceback.print_exc()
        raise

def safe_executemany(cursor, sql, rows, step="UNKNOWN"):
    try:
        cursor.executemany(sql, rows)
    except Exception as e:
        log(f" ERROR during {step}: {e}")
        traceback.print_exc()
        raise

# ========= CSV PATH =========
CSV_PATH = os.path.join(PROJECT_ROOT, "bronze", "scrapedData.csv")

log("=========== BRONZE LOAD START ===========")
log(f"CSV path: {CSV_PATH}")

# ========= LOAD CSV =========
try:
    df = pd.read_csv(CSV_PATH)
    log(f"Loaded CSV with {len(df)} rows.")
except Exception as e:
    log(" FAILED TO LOAD CSV")
    log(str(e))
    traceback.print_exc()
    raise SystemExit("Failed to load scrapedData.csv")

# ========= EXPECTED COLUMNS =========
required_cols = [
    "company", "model", "drivetrain", "class", "seat",
    "price_raw",          # NEW
    "range_raw", "efficiency", "weight",
    "zero_to_sixty", "one_stop_range",
    "battery", "rapidcharge", "towing",
    "boot_space", "price_range"
]

missing = [c for c in required_cols if c not in df.columns]
if missing:
    log(" MISSING REQUIRED COLUMNS:")
    for col in missing:
        log(f" - {col}")
    raise SystemExit("CSV does not match expected schema.")

# ========= CLEAN DATA =========
df = df[required_cols]

def clean_val(x):
    if pd.isna(x): return None
    x = str(x).strip()
    return None if x == "" else x

df = df.applymap(clean_val)

# ========= MYSQL LOAD =========
conn = mysql.connector.connect(
    host="localhost",
    user="EV_specs",
    password="MDIS@2025"
)
cursor = conn.cursor()

safe_execute(cursor, "USE DataWarehouse_bronze;", step="USE Bronze DB")
safe_execute(cursor, "TRUNCATE TABLE ev_specs_bronze;", step="TRUNCATE Bronze table")

insert_sql = """
INSERT INTO ev_specs_bronze (
    company, model, drivetrain, class, seat,
    price_raw, range_raw, efficiency, weight,
    zero_to_sixty, one_stop_range, battery,
    rapidcharge, towing, boot_space, price_range
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s
);
"""

safe_executemany(cursor, insert_sql, df.values.tolist(), step="INSERT rows")

conn.commit()

log(f" Bronze load complete. {cursor.rowcount} rows inserted.")

cursor.close()
conn.close()

log("=========== BRONZE LOAD FINISHED ===========")

