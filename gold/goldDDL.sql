-- ============================================================
-- GOLD LAYER – ANALYTICS TABLES
-- ============================================================

CREATE DATABASE IF NOT EXISTS DataWarehouse_gold;
USE DataWarehouse_gold;

DROP TABLE IF EXISTS gold_ev_summary;
DROP TABLE IF EXISTS gold_brand_summary;

-- -------------------------------
-- Gold EV Summary (Main Fact Table)
-- -------------------------------
CREATE TABLE gold_ev_summary (
    summary_id INT AUTO_INCREMENT PRIMARY KEY,
    vehicle_id INT,
    manufacturer_name VARCHAR(100),
    model_name VARCHAR(200),

    range_miles INT,
    battery_kwh DECIMAL(5,2),
    weight_kg INT,
    zero_to_sixty_sec DECIMAL(4,2),
    efficiency_whpm INT,
    rapidcharge_kw INT,
    towing_kg INT,
    boot_space_liters INT,

    price_gbp INT,
    price_per_kwh DECIMAL(10,2),
    price_per_mile DECIMAL(10,2),
    price_per_weight DECIMAL(10,4),

    value_score DECIMAL(10,4),
    performance_score DECIMAL(10,4),
    efficiency_score DECIMAL(10,6),
    charging_score DECIMAL(10,4)
);

-- -------------------------------
-- Brand Summary Table
-- -------------------------------
CREATE TABLE gold_brand_summary (
    brand_id INT AUTO_INCREMENT PRIMARY KEY,
    manufacturer_name VARCHAR(100),

    avg_price_gbp DECIMAL(10,2),
    avg_range_miles DECIMAL(10,2),
    avg_battery_kwh DECIMAL(10,2),
    avg_efficiency_whpm DECIMAL(10,2),
    avg_zero_to_sixty DECIMAL(10,3),

    vehicle_count INT
);
