SELECT 
    m.manufacturer_name,
    v.model_name,
    f.range_miles
FROM DataWarehouse_gold.fact_vehicle_specs f
JOIN DataWarehouse_gold.dim_vehicle v ON f.vehicle_id = v.vehicle_id
JOIN DataWarehouse_gold.dim_manufacturer m ON v.manufacturer_id = m.manufacturer_id
ORDER BY f.range_miles DESC