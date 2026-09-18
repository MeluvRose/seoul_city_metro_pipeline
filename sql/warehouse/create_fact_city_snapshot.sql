CREATE TABLE IF NOT EXISTS warehouse.fact_city_snapshot (
    city_snapshot_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    location_id           bigint NOT NULL
        REFERENCES warehouse.dim_location(location_id),

    date_key              integer NOT NULL
        REFERENCES warehouse.dim_date(date_key),

    hour_key              smallint NOT NULL
        REFERENCES warehouse.dim_hour(hour_key),

    observed_at           timestamptz NOT NULL,
    collected_at          timestamptz NOT NULL,

    congestion_level      text,
    congestion_message    text,

    population_min        integer,
    population_max        integer,

    male_ratio            numeric,
    female_ratio          numeric,
    resident_ratio        numeric,
    non_resident_ratio    numeric,

    source_ingestion_id   bigint NOT NULL,

    dw_loaded_at          timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dw_updated_at         timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT ck_fact_city_population_range
        CHECK (
            population_min IS NULL
            OR population_max IS NULL
            OR population_min <= population_max
        ),

    CONSTRAINT uq_fact_city_location_observed
        UNIQUE (location_id, observed_at)
);

CREATE INDEX IF NOT EXISTS ix_fact_city_snapshot_observed_at
    ON warehouse.fact_city_snapshot (observed_at);

CREATE INDEX IF NOT EXISTS ix_fact_city_snapshot_analysis
    ON warehouse.fact_city_snapshot (
        location_id,
        date_key,
        hour_key
    );