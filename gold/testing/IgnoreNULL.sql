
SELECT 
    m.manufacturer_name,
    AVG(f.range_miles)       AS avg_range_miles,
    AVG(f.efficiency_whpm)   AS avg_efficiency_whpm,
    AVG(f.battery_kwh)       AS avg_battery_kwh,
    COUNT(*) AS models_used_for_average
FROM DataWarehouse_gold.fact_vehicle_specs f
JOIN DataWarehouse_gold.dim_vehicle v ON f.vehicle_id = v.vehicle_id
JOIN DataWarehouse_gold.dim_manufacturer m ON v.manufacturer_id = m.manufacturer_id
WHERE f.range_miles IS NOT NULL
  AND f.efficiency_whpm IS NOT NULL
  AND f.battery_kwh IS NOT NULL
GROUP BY m.manufacturer_name;
