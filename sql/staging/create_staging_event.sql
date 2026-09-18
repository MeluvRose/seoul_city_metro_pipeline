CREATE TABLE staging.realtime_event (
    staging_id       bigint NOT NULL
                       REFERENCES staging.city_header (staging_id),

    area_code          text NOT NULL,

    event_name         text NOT NULL,
    event_start_at     timestamptz,
    event_end_at       timestamptz,

    venue_name         text,
    event_category     text,
    event_url          text,

    source_event_id    text,
    loaded_at          timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (staging_id, event_name, event_start_at)
);