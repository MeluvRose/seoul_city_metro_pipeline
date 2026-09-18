CREATE TABLE raw.api_response (
    raw_id       bigserial PRIMARY KEY,

    source_name        text NOT NULL DEFAULT 'seoul_realtime_city',
    area_code          text,
    area_name          text NOT NULL,

    requested_at       timestamptz NOT NULL,
    received_at        timestamptz NOT NULL DEFAULT now(),
    http_status        integer,
    result_code        text,
    result_message     text,

    payload            jsonb NOT NULL,
    payload_hash       text,

    is_success         boolean NOT NULL,
    error_message      text
);