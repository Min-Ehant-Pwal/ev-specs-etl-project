import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import sys
import os
from datetime import datetime
import traceback

# =====================================================
# LOGGING SETUP (DYNAMIC)
# =====================================================

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")

os.makedirs(LOG_DIR, exist_ok=True)

log_filename = f"scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(LOG_DIR, log_filename)

def write_log(msg):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    write_log(line)

# =====================================================
# NORMALIZATION MAP FOR SPEC LABELS
# =====================================================

NORMALIZE = {
    # Range
    "Range": "range_raw",
    "Range*": "range_raw",

    # Efficiency
    "Efficiency": "efficiency",
    "Efficiency*": "efficiency",

    # Weight
    "Weight": "weight",

    # Acceleration
    "0-62": "zero_to_sixty",
    "0-62*": "zero_to_sixty",

    # 1-stop range
    "1-Stop Range": "one_stop_range",

    # Battery
    "Battery": "battery",
    "Battery*": "battery",

    # Rapidcharge
    "Rapidcharge": "rapidcharge",
    "Rapidcharge*": "rapidcharge",

    # Towing
    "Towing": "towing",

    # Boot space
    "Boot Space": "boot_space",

    # Price/range mixed label
    "Price/range": "price_range",
    "Price/range*": "price_range"
}

# =====================================================
# SCRAPING URL + HEADERS
# =====================================================

URL = "https://ev-database.org/uk/#group=vehicle-group&rs-pr=10000_100000&rs-er=0_500&rs-ld=0_500&rs-ac=2_23&rs-dcfc=0_400&rs-ub=10_200&rs-tw=0_2500&rs-ef=150_600&rs-sa=-1_5&rs-w=1000_3500&rs-c=0_5000&rs-y=2010_2030&s=1&p=82-10"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

log("========== STARTING SCRAPING ==========")
log(f"Requesting URL: {URL}")

# =====================================================
# REQUEST PAGE
# =====================================================

try:
    resp = requests.get(URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    html = resp.text
    log(" Page fetched successfully.")
except Exception as e:
    log(" ERROR FETCHING URL")
    log(str(e))
    traceback.print_exc()
    sys.exit(1)

soup = BeautifulSoup(html, "html.parser")

# =====================================================
# SCRAPING LOOP
# =====================================================

rows = []
items = soup.select("div.list-item")

log(f"Found {len(items)} vehicle items.")

for idx, item in enumerate(items, start=1):
    try:
        a = item.select_one("a.title") or item.find("a")
        if not a:
            continue

        # company
        company = ""
        for sp in a.find_all("span"):
            if "model" in (sp.get("class") or []) or "hidden" in (sp.get("class") or []):
                continue
            txt = sp.get_text(strip=True)
            if txt:
                company = txt
                break

        # model
        model_tag = a.select_one("span.model")
        model = model_tag.get_text(strip=True) if model_tag else ""

        # drivetrain
        drivetrain = ""
        icons_row = item.select_one(".icons .icons-row-1") or item.select_one(".icons-row-1")
        if icons_row:
            tooltip = icons_row.find(attrs={"data-tooltip": True})
            if tooltip:
                drivetrain = tooltip.get("data-tooltip", "").strip()

        # market class
        market_class = ""
        ms_tag = item.find(attrs={"data-tooltip": lambda v: v and "Market Segment" in v})
        if ms_tag:
            inner = ms_tag.get_text(strip=True)
            market_class = inner or ms_tag.get("data-tooltip", "").split(":")[-1].strip()

        # seat
        seat = ""
        tooltip_wr = item.select_one(".tooltip-wrapper")
        if tooltip_wr:
            seat = tooltip_wr.get_text(strip=True)

        # price_raw: extract from <div class="price_buy">
        price_raw = None
        price_div = item.select_one("div.price_buy")
        if price_div:
            txt = price_div.get_text(strip=True)
            if txt.startswith("£"):
                price_raw = txt

        row = {
            "company": company,
            "model": model,
            "drivetrain": drivetrain,
            "class": market_class,
            "seat": seat,
            "price_raw": price_raw
        }

        # SPEC DETAILS
        specs_div = item.select_one("div.specs")
        if specs_div:
            for spec_block in specs_div.find_all(recursive=False):
                label_tag = spec_block.select_one("span.label")
                if not label_tag:
                    continue

                raw_label = label_tag.get_text(strip=True)
                norm_label = NORMALIZE.get(raw_label)

                if not norm_label:
                    continue  # skip unknown labels

                value = ""
                for sp in spec_block.find_all("span"):
                    if sp is label_tag:
                        continue
                    if "hidden" in (sp.get("class") or []):
                        continue
                    txt = sp.get_text(strip=True)
                    if txt:
                        value = txt
                        break

                row[norm_label] = value

        rows.append(row)

        if idx % 10 == 0:
            log(f"Processed {idx} vehicles...")

    except Exception as scrape_err:
        log(f"❌ Error parsing item #{idx}: {scrape_err}")
        traceback.print_exc()

# =====================================================
# CREATE DATAFRAME
# =====================================================

df = pd.DataFrame(rows)
log(f"Total scraped rows: {len(df)}")

if df.empty:
    log("❌ SCRAPING FAILED — NO DATA FOUND.")
    sys.exit(1)

# =====================================================
# ENSURE STANDARDIZED COLUMNS
# =====================================================

expected_cols = [
    "company", "model", "drivetrain", "class", "seat",
    "price_raw",
    "range_raw", "efficiency", "weight",
    "zero_to_sixty", "one_stop_range",
    "battery", "rapidcharge", "towing",
    "boot_space", "price_range"
]

for col in expected_cols:
    if col not in df.columns:
        df[col] = None

df = df[expected_cols]

# =====================================================
# SAVE CLEAN CSV TO BRONZE
# =====================================================

BRONZE_DIR = os.path.join(PROJECT_ROOT, "bronze")
os.makedirs(BRONZE_DIR, exist_ok=True)

csv_path = os.path.join(BRONZE_DIR, "scrapedData.csv")
df.to_csv(csv_path, index=False, encoding="utf-8")

log(f" Saved scraped data to: {csv_path}")
log("========== SCRAPING COMPLETE ==========")

time.sleep(1)
