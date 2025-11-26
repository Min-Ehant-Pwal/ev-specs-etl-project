-- ============================================================
-- SILVER LAYER – CLEANED & STANDARDIZED DATA
-- ============================================================

CREATE DATABASE IF NOT EXISTS DataWarehouse_silver;
USE DataWarehouse_silver;

SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS silver_specs;
DROP TABLE IF EXISTS silver_vehicle;
DROP TABLE IF EXISTS silver_manufacturer;

SET FOREIGN_KEY_CHECKS = 1;

-- -------------------------------
-- Manufacturer Dimension
-- -------------------------------
CREATE TABLE silver_manufacturer (
    manufacturer_id INT AUTO_INCREMENT PRIMARY KEY,
    manufacturer_name VARCHAR(100) UNIQUE
);

-- -------------------------------
-- Vehicle Dimension
-- -------------------------------
CREATE TABLE silver_vehicle (
    vehicle_id INT AUTO_INCREMENT PRIMARY KEY,
    manufacturer_id INT NOT NULL,
    model_name VARCHAR(200),
    drivetrain VARCHAR(50),
    class VARCHAR(50),
    seat INT,

    FOREIGN KEY (manufacturer_id)
        REFERENCES silver_manufacturer(manufacturer_id)
);

-- -------------------------------
-- Vehicle Specifications (Clean)
-- -------------------------------
CREATE TABLE silver_specs (
    spec_id INT AUTO_INCREMENT PRIMARY KEY,
    vehicle_id INT NOT NULL,

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
