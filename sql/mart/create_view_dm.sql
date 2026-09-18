CREATE OR REPLACE VIEW mart.city_population_hourly AS
SELECT
    f.location_id,
    l.area_code,
    l.area_name,

    f.date_key,
    d.full_date,

    f.hour_key,
    h.hour_label,

    COUNT(*) AS snapshot_count,

    ROUND(
        AVG(f.population_min)::numeric,
        2
    ) AS avg_population_min,

    ROUND(
        AVG(f.population_max)::numeric,
        2
    ) AS avg_population_max,

    ROUND(
        AVG(
            (f.population_min + f.population_max) / 2.0
        )::numeric,
        2
    ) AS avg_estimated_population,

    MIN(f.population_min) AS min_population_min,

    MAX(f.population_max) AS peak_population_max,

    COUNT(*) FILTER (
        WHERE f.congestion_level = '여유'
    ) AS relaxed_count,

    COUNT(*) FILTER (
        WHERE f.congestion_level = '보통'
    ) AS normal_count,

    COUNT(*) FILTER (
        WHERE f.congestion_level = '약간 붐빔'
    ) AS slightly_crowded_count,

    COUNT(*) FILTER (
        WHERE f.congestion_level = '붐빔'
    ) AS crowded_count,

    COUNT(*) FILTER (
        WHERE f.congestion_level = '매우 붐빔'
    ) AS very_crowded_count,

    MAX(f.dw_loaded_at) AS latest_fact_loaded_at

FROM warehouse.fact_city_snapshot AS f
JOIN warehouse.dim_location AS l
  ON l.location_id = f.location_id
JOIN warehouse.dim_date AS d
  ON d.date_key = f.date_key
JOIN warehouse.dim_hour AS h
  ON h.hour_key = f.hour_key

GROUP BY
    f.location_id,
    l.area_code,
    l.area_name,
    f.date_key,
    d.full_date,
    f.hour_key,
    h.hour_label;

CREATE OR REPLACE VIEW mart.city_weather_hourly AS
SELECT
    f.location_id,
    l.area_code,
    l.area_name,

    f.date_key,
    d.full_date,

    f.hour_key,
    h.hour_label,

    COUNT(*) AS weather_snapshot_count,

    ROUND(
        AVG(f.temperature_c)::NUMERIC,
        2
    ) AS avg_temperature_c,

    ROUND(
        AVG(f.feels_like_c)::NUMERIC,
        2
    ) AS avg_feels_like_c,

    ROUND(
        AVG(f.humidity_pct)::NUMERIC,
        2
    ) AS avg_humidity_pct,

    ROUND(
        AVG(f.wind_speed_mps)::NUMERIC,
        2
    ) AS avg_wind_speed_mps,

    MAX(f.wind_speed_mps) AS max_wind_speed_mps,

    ROUND(
        AVG(f.pm10)::NUMERIC,
        2
    ) AS avg_pm10,

    ROUND(
        AVG(f.pm25)::NUMERIC,
        2
    ) AS avg_pm25,

    MAX(f.precipitation_mm) AS max_precipitation_mm,

    COUNT(*) FILTER (
        WHERE f.precipitation_type = '없음'
    ) AS no_precipitation_count,

    COUNT(*) FILTER (
        WHERE f.precipitation_type IS NOT NULL
          AND f.precipitation_type <> '없음'
    ) AS precipitation_observed_count,

    MODE() WITHIN GROUP (
        ORDER BY f.precipitation_type
    ) AS dominant_precipitation_type,

    MODE() WITHIN GROUP (
        ORDER BY f.air_quality_index
    ) AS dominant_air_quality_index,

    MAX(f.dw_loaded_at) AS latest_weather_fact_loaded_at

FROM warehouse.fact_weather_snapshot AS f
JOIN warehouse.dim_location AS l
  ON l.location_id = f.location_id
JOIN warehouse.dim_date AS d
  ON d.date_key = f.date_key
JOIN warehouse.dim_hour AS h
  ON h.hour_key = f.hour_key

GROUP BY
    f.location_id,
    l.area_code,
    l.area_name,
    f.date_key,
    d.full_date,
    f.hour_key,
    h.hour_label;

CREATE OR REPLACE VIEW mart.city_population_weather_hourly AS
SELECT
    p.location_id,
    p.area_code,
    p.area_name,

    p.date_key,
    p.full_date,

    p.hour_key,
    p.hour_label,

    p.snapshot_count AS population_snapshot_count,
    p.avg_population_min,
    p.avg_population_max,
    p.avg_estimated_population,
    p.min_population_min,
    p.peak_population_max,

    p.relaxed_count,
    p.normal_count,
    p.slightly_crowded_count,
    p.crowded_count,
    p.very_crowded_count,

    w.weather_snapshot_count,
    w.avg_temperature_c,
    w.avg_feels_like_c,
    w.avg_humidity_pct,
    w.avg_wind_speed_mps,
    w.max_wind_speed_mps,
    w.avg_pm10,
    w.avg_pm25,
    w.max_precipitation_mm,
    w.no_precipitation_count,
    w.precipitation_observed_count,
    w.dominant_precipitation_type,
    w.dominant_air_quality_index,

    p.latest_fact_loaded_at AS latest_population_fact_loaded_at,
    w.latest_weather_fact_loaded_at

FROM mart.city_population_hourly AS p
LEFT JOIN mart.city_weather_hourly AS w
  ON w.location_id = p.location_id
 AND w.date_key = p.date_key
 AND w.hour_key = p.hour_key;