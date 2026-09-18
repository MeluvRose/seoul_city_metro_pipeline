CREATE TABLE staging.realtime_population_snapshot (
    population_snapshot_id BIGINT
        GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    ingestion_id BIGINT NOT NULL
        REFERENCES staging.realtime_city_header (ingestion_id),

    area_code TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,

    congestion_level TEXT,
    congestion_message TEXT,

    population_min INTEGER,
    population_max INTEGER,

    male_ratio NUMERIC(5, 2),
    female_ratio NUMERIC(5, 2),

    resident_ratio NUMERIC(5, 2),
    non_resident_ratio NUMERIC(5, 2),

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_realtime_population_area_time
        UNIQUE (area_code, observed_at),

    CONSTRAINT chk_population_range
        CHECK (
            population_min IS NULL
            OR population_max IS NULL
            OR population_min <= population_max
        )
);