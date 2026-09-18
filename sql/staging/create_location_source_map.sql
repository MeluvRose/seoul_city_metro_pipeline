CREATE TABLE stg.location_source_map (
    source_name          text NOT NULL,
    source_area_code     text NOT NULL,
    source_area_name     text NOT NULL,

    analysis_area_code   text NOT NULL,
    analysis_area_name   text NOT NULL,

    area_type            text NOT NULL,
    district_name        text,

    is_active            boolean NOT NULL DEFAULT true,
    valid_from           date NOT NULL DEFAULT current_date,
    valid_to             date,

    PRIMARY KEY (source_name, source_area_code)
);