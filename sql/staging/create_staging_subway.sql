CREATE TABLE staging.subway_station_daily (
    subway_usage_id       BIGSERIAL PRIMARY KEY,

    raw_id                BIGINT NOT NULL
                          REFERENCES raw.api_response (raw_id),

    usage_date            DATE NOT NULL,
    line_name             VARCHAR(100) NOT NULL,

    station_id            VARCHAR(50) NOT NULL,
    station_name          VARCHAR(100) NOT NULL,

    boardings             INTEGER NOT NULL,
    alightings            INTEGER NOT NULL,

    source_registered_at  DATE,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_subway_station_daily
        UNIQUE (usage_date, line_name, station_id),

    CONSTRAINT chk_subway_boardings_nonnegative
        CHECK (boardings >= 0),

    CONSTRAINT chk_subway_alightings_nonnegative
        CHECK (alightings >= 0)
);

CREATE INDEX idx_stg_subway_station_daily_date_station
    ON staging.subway_station_daily (usage_date, station_id);