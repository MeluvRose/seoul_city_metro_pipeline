CREATE TABLE staging.realtime_weather_snapshot (
    ingestion_id          bigint PRIMARY KEY
                          REFERENCES staging.realtime_city_header (ingestion_id),

    area_code             text NOT NULL,
    observed_at           timestamptz NOT NULL,

    temperature_c         numeric(5, 2),
    feels_like_c          numeric(5, 2),
    humidity_pct          numeric(5, 2),

    precipitation_type    text,
    precipitation_mm      numeric(7, 2),

    wind_speed_mps        numeric(6, 2),
    wind_direction_deg    numeric(6, 2),

    pm10                  numeric(8, 2),
    pm25                  numeric(8, 2),
    air_quality_index     text,

    weather_description   text,
    loaded_at             timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT chk_humidity_range
        CHECK (
            humidity_pct IS NULL
            OR humidity_pct BETWEEN 0 AND 100
        ),

    CONSTRAINT chk_precipitation_nonnegative
        CHECK (
            precipitation_mm IS NULL
            OR precipitation_mm >= 0
        ),

    CONSTRAINT chk_pm10_nonnegative
        CHECK (pm10 IS NULL OR pm10 >= 0),

    CONSTRAINT chk_pm25_nonnegative
        CHECK (pm25 IS NULL OR pm25 >= 0)
);