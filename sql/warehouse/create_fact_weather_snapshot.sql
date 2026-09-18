CREATE TABLE IF NOT EXISTS warehouse.fact_weather_snapshot (
    weather_snapshot_id BIGINT GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    location_id BIGINT NOT NULL
        REFERENCES warehouse.dim_location(location_id),

    date_key INTEGER NOT NULL
        REFERENCES warehouse.dim_date(date_key),

    hour_key SMALLINT NOT NULL
        REFERENCES warehouse.dim_hour(hour_key),

    observed_at TIMESTAMPTZ NOT NULL,

    collected_at TIMESTAMPTZ NOT NULL,

    temperature_c NUMERIC(5, 2),
    feels_like_c NUMERIC(5, 2),
    humidity_pct NUMERIC(5, 2),

    precipitation_type TEXT,
    precipitation_mm NUMERIC(7, 2),

    wind_speed_mps NUMERIC(6, 2),
    wind_direction_deg NUMERIC(6, 2),

    pm10 NUMERIC(8, 2),
    pm25 NUMERIC(8, 2),

    air_quality_index TEXT,
    weather_description TEXT,

    source_ingestion_id BIGINT NOT NULL,

    dw_loaded_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    dw_updated_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_fact_weather_location_observed
        UNIQUE (location_id, observed_at),

    CONSTRAINT ck_fact_weather_humidity
        CHECK (
            humidity_pct IS NULL
            OR humidity_pct BETWEEN 0 AND 100
        ),

    CONSTRAINT ck_fact_weather_precipitation_nonnegative
        CHECK (
            precipitation_mm IS NULL
            OR precipitation_mm >= 0
        ),

    CONSTRAINT ck_fact_weather_wind_speed_nonnegative
        CHECK (
            wind_speed_mps IS NULL
            OR wind_speed_mps >= 0
        ),

    CONSTRAINT ck_fact_weather_pm10_nonnegative
        CHECK (
            pm10 IS NULL
            OR pm10 >= 0
        ),

    CONSTRAINT ck_fact_weather_pm25_nonnegative
        CHECK (
            pm25 IS NULL
            OR pm25 >= 0
        )
);

CREATE INDEX IF NOT EXISTS ix_fact_weather_snapshot_observed_at
ON warehouse.fact_weather_snapshot (observed_at);

CREATE INDEX IF NOT EXISTS ix_fact_weather_snapshot_location_date_hour
ON warehouse.fact_weather_snapshot (
    location_id,
    date_key,
    hour_key
);