USE DataWarehouse_gold;

-- ======================================================
-- VIEW 1: Top Value EVs (Best Range Per £)
-- ======================================================
CREATE OR REPLACE VIEW vw_top_value_evs AS
SELECT
    manufacturer_name,
    model_name,
    price_gbp,
    range_miles,
    value_score
FROM gold_ev_summary
WHERE value_score IS NOT NULL
ORDER BY value_score DESC
LIMIT 20;

-- ======================================================
-- VIEW 2: Best Price Per kWh (Battery Value Ranking)
-- ======================================================
CREATE OR REPLACE VIEW vw_best_price_per_kwh AS
SELECT
    manufacturer_name,
    model_name,
    battery_kwh,
    price_gbp,
    price_per_kwh
FROM gold_ev_summary
WHERE price_per_kwh IS NOT NULL
ORDER BY price_per_kwh ASC
LIMIT 20;

-- ======================================================
-- VIEW 3: Performance Ranking (0–62 mph)
-- ======================================================
CREATE OR REPLACE VIEW vw_top_performance AS
SELECT
    manufacturer_name,
    model_name,
    zero_to_sixty_sec,
    performance_score
FROM gold_ev_summary
WHERE zero_to_sixty_sec IS NOT NULL
ORDER BY zero_to_sixty_sec ASC
LIMIT 20;

-- ======================================================
-- VIEW 4: Best Efficiency (Lower Wh/mi = Better)
-- ======================================================
CREATE OR REPLACE VIEW vw_best_efficiency AS
SELECT
    manufacturer_name,
    model_name,
    efficiency_whpm,
    efficiency_score
FROM gold_ev_summary
WHERE efficiency_whpm IS NOT NULL
ORDER BY efficiency_whpm ASC
LIMIT 20;

-- ======================================================
-- VIEW 5: Range vs Price (Scatter Plot)
-- ======================================================
CREATE OR REPLACE VIEW vw_range_vs_price AS
SELECT
    manufacturer_name,
    model_name,
    range_miles,
    price_gbp,
    price_per_mile,
    value_score
FROM gold_ev_summary
WHERE range_miles IS NOT NULL AND price_gbp IS NOT NULL;

-- ======================================================
-- VIEW 6: Charging Speed Ranking
-- ======================================================
CREATE OR REPLACE VIEW vw_top_charging_vehicles AS
SELECT
    manufacturer_name,
    model_name,
    rapidcharge_kw,
    battery_kwh,
    charging_score
FROM gold_ev_summary
WHERE charging_score IS NOT NULL
ORDER BY charging_score DESC
LIMIT 20;

-- ======================================================
-- VIEW 7: Cheapest EVs
-- ======================================================
CREATE OR REPLACE VIEW vw_cheapest_evs AS
SELECT
    manufacturer_name,
    model_name,
    price_gbp,
    battery_kwh,
    range_miles
FROM gold_ev_summary
WHERE price_gbp IS NOT NULL AND price_gbp > 0
ORDER BY price_gbp ASC
LIMIT 20;

-- ======================================================
-- VIEW 8: Premium EVs (Most Expensive)
-- ======================================================
CREATE OR REPLACE VIEW vw_premium_evs AS
SELECT
    manufacturer_name,
    model_name,
    price_gbp,
    battery_kwh,
    range_miles
FROM gold_ev_summary
WHERE price_gbp IS NOT NULL
ORDER BY price_gbp DESC
LIMIT 20;

-- ======================================================
-- VIEW 9: Brand Averages (Summary)
-- ======================================================
CREATE OR REPLACE VIEW vw_brand_averages AS
SELECT
    manufacturer_name,
    model_count,
    avg_price_gbp,
    avg_range_miles,
    avg_battery_kwh,
    avg_efficiency_whpm,
    avg_zero_to_sixty_sec,
    min_price_gbp,
    max_price_gbp
FROM gold_brand_summary;

-- ======================================================
-- VIEW 10: Best Price per Weight (Lightweight Value)
-- ======================================================
CREATE OR REPLACE VIEW vw_best_price_per_weight AS
SELECT
    manufacturer_name,
    model_name,
    price_gbp,
    weight_kg,
    price_per_weight
FROM gold_ev_summary
WHERE price_per_weight IS NOT NULL
ORDER BY price_per_weight ASC
LIMIT 20;

-- ======================================================
-- VIEW 11: Full Analytics Dataset (Power BI)
-- ======================================================
CREATE OR REPLACE VIEW vw_ev_analytics AS
SELECT
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
FROM gold_ev_summary;
