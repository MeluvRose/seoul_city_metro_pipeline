CREATE TABLE staging.realtime_city_header (
    ingestion_id       bigint PRIMARY KEY
                       REFERENCES raw.api_response (raw_id),

    area_code          text NOT NULL,
    area_name          text NOT NULL,

    observed_at        timestamptz,
    collected_at       timestamptz NOT NULL,

    result_code        text,
    result_message     text,
    is_success         boolean NOT NULL,

    loaded_at          timestamptz NOT NULL DEFAULT now()
);