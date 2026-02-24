CREATE MATERIALIZED VIEW dim_sensors
COMMENT 'Sensors dimensional table'
AS SELECT * FROM parquet.`./data/sensors.parquet`;

CREATE MATERIALIZED VIEW fact_readings
COMMENT 'Fact readings table'
AS SELECT * FROM parquet.`./data/readings.parquet`;

CREATE MATERIALIZED VIEW fact_warehouse_temperature
COMMENT 'Fact table of temperature statistics per location'
AS SELECT
    s.location,
    MAX(r.value) AS max_temperature
FROM fact_readings r
JOIN dim_sensors s ON r.sensor_id = s.sensor_id
WHERE s.sensor_type = 'temperature'
GROUP BY s.location;
