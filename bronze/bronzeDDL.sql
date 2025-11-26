-- ============================================================
-- BRONZE LAYER – RAW SCRAPED DATA
-- ============================================================

CREATE DATABASE IF NOT EXISTS DataWarehouse_bronze;
USE DataWarehouse_bronze;

DROP TABLE IF EXISTS ev_specs_bronze;

CREATE TABLE ev_specs_bronze (
    id INT AUTO_INCREMENT PRIMARY KEY,

    company            VARCHAR(100),
    model              VARCHAR(200),
    drivetrain         VARCHAR(50),
    class              VARCHAR(50),
    seat               INT,

    price_raw          VARCHAR(50),
    range_raw          VARCHAR(50),
    efficiency         VARCHAR(50),
    weight             VARCHAR(50),
    zero_to_sixty      VARCHAR(50),
    one_stop_range     VARCHAR(50),
    battery            VARCHAR(50),
    rapidcharge        VARCHAR(50),
    towing             VARCHAR(50),
    boot_space         VARCHAR(50),
    price_range        VARCHAR(50)
);
