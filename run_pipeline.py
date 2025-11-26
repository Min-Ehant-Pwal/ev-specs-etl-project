import subprocess
import sys
import os
from datetime import datetime

# ============================================
# Logging Setup
# ============================================

LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ============================================
# Run script helper
# ============================================

def run_step(script_path, step_name):
    log(f"----- Starting: {step_name} -----")
    try:
        completed = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True
        )

        if completed.returncode != 0:
            log(f"ERROR in {step_name}")
            log("----- STDOUT -----")
            log(completed.stdout)
            log("----- STDERR -----")
            log(completed.stderr)
            raise Exception(f"{step_name} failed. See logs.")
        else:
            log(f"✔ Completed: {step_name}")
            log("----- STDOUT -----")
            log(completed.stdout)
    except Exception as e:
        log(f"Pipeline stopped due to error in step: {step_name}")
        log(str(e))
        raise


# ============================================
# Pipeline Execution
# ============================================

if __name__ == "__main__":
    log("========== PIPELINE START ==========")

    try:
        ROOT = os.path.dirname(os.path.abspath(__file__))

        # 1. SCRAPE DATA
        run_step(
            os.path.join(ROOT, "scraping", "web_scrape.py"),
            "Data Scraping"
        )

        # 2. LOAD BRONZE
        run_step(
            os.path.join(ROOT, "bronze", "bronze_load.py"),
            "Bronze Load"
        )

        # 3. LOAD SILVER
        run_step(
            os.path.join(ROOT, "silver", "silver_load.py"),
            "Silver Load"
        )

        # 4. LOAD GOLD
        run_step(
            os.path.join(ROOT, "gold", "gold_load.py"),
            "Gold Load"
        )

        log("========== PIPELINE COMPLETED SUCCESSFULLY ==========")

    except Exception as pipeline_error:
        log("========== PIPELINE FAILED ==========")
        log(str(pipeline_error))
