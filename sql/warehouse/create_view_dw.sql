CREATE OR REPLACE VIEW staging.v_realtime_population_for_dw AS
SELECT
    p.population_snapshot_id,
    p.ingestion_id,

    p.area_code,
    h.area_name,

    p.observed_at,
    h.collected_at,

    p.congestion_level,
    p.congestion_message,

    p.population_min,
    p.population_max,

    p.male_ratio,
    p.female_ratio,
    p.resident_ratio,
    p.non_resident_ratio,

    p.loaded_at AS staging_loaded_at
FROM staging.realtime_population_snapshot AS p
JOIN staging.realtime_city_header AS h
  ON h.ingestion_id = p.ingestion_id
 AND h.area_code = p.area_code
WHERE h.is_success = true;

