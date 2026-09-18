CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key          integer PRIMARY KEY,
    full_date         date NOT NULL UNIQUE,
    year              smallint NOT NULL,
    quarter           smallint NOT NULL
        CHECK (quarter BETWEEN 1 AND 4),
    month             smallint NOT NULL
        CHECK (month BETWEEN 1 AND 12),
    day_of_month      smallint NOT NULL
        CHECK (day_of_month BETWEEN 1 AND 31),
    iso_day_of_week   smallint NOT NULL
        CHECK (iso_day_of_week BETWEEN 1 AND 7),
    is_weekend        boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_hour (
    hour_key          smallint PRIMARY KEY
        CHECK (hour_key BETWEEN 0 AND 23),
    hour_start        time NOT NULL UNIQUE,
    hour_label        varchar(5) NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_location (
    location_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    area_code         text NOT NULL UNIQUE,
    area_name         text NOT NULL,
    area_category     text,
    latitude          numeric(9, 6),
    longitude         numeric(9, 6),
    timezone_name     text NOT NULL DEFAULT 'Asia/Seoul',
    is_active         boolean NOT NULL DEFAULT true,
    created_at        timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_dim_location_latitude
        CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
    CONSTRAINT ck_dim_location_longitude
        CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180)
);