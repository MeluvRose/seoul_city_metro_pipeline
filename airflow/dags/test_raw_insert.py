from __future__ import annotations

import json
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException

POSTGRES_CONN_ID = "seoul_city_postgres"


@dag(
    dag_id="test_raw_insert",
    description="테스트 JSON을 Raw 테이블에 저장하는 Airflow DAG",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["learning", "raw", "postgres"],
)
def test_raw_insert():
    @task
    def extract_dummy_to_raw() -> int:
        sample_payload = {
            "CITYDATA": {
                "AREA_NM": "용산역",
                "AREA_CD": "YONGSAN",
                "LIVE_PPLTN_STTS": [
                    {
                        "PPLTN_TIME": "2026-09-14 11:00:00",
                        "AREA_CONGEST_LVL": "보통",
                        "AREA_PPLTN_MIN": "30000",
                        "AREA_PPLTN_MAX": "42000",
                    }
                ],
            }
        }

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        result = hook.get_first(
            """
            INSERT INTO raw.api_response (
                source_name,
                area_code,
                area_name,
                requested_at,
                http_status,
                is_success,
                payload
            )
            VALUES (
                %(source_name)s,
                %(area_code)s,
                %(area_name)s,
                now(),
                %(http_status)s,
                %(is_success)s,
                %(payload)s::jsonb
            )
            RETURNING raw_id;
            """,
            parameters={
                "source_name": "test_dummy",
                "area_code": "GANGNAM",
                "area_name": "강남역",
                "http_status": 200,
                "is_success": True,
                "payload": json.dumps(sample_payload, ensure_ascii=False),
            },
        )

        raw_id = result[0]

        print(f"Raw 데이터 저장 성공 | raw_id={raw_id}")
        return raw_id

    @task
    def validate_raw_payload(raw_id: int) -> int:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        row = hook.get_first(
            """
            SELECT
                raw_id,
                source_name,
                area_code,
                area_name,
                is_success,
                payload
            FROM raw.api_response
            WHERE raw_id = %(raw_id)s;
            """,
            parameters={"raw_id": raw_id},
        )
        if row is None:
            raise AirflowFailException(
                f"검증 실패: Raw 데이터가 존재하지 않습니다. raw_id={raw_id}"
            )
        (
            saved_raw_id,
            source_name,
            area_code,
            area_name,
            is_success,
            payload,
        ) = row
        if not is_success:
            raise AirflowFailException(
                f"검증 실패: 수집 실패 Raw입니다. raw_id={saved_raw_id}"
            )

        if payload is None:
            raise AirflowFailException(
                f"검증 실패: payload가 없습니다. raw_id={saved_raw_id}"
            )

        if isinstance(payload, str):
            payload = json.loads(payload)

        city_data = payload.get("CITYDATA")

        if not isinstance(city_data, dict):
            raise AirflowFailException(
                f"검증 실패: CITYDATA 객체가 없습니다. raw_id={saved_raw_id}"
            )

        required_citydata_keys = [
            "AREA_NM",
            "AREA_CD",
            "LIVE_PPLTN_STTS",
        ]

        missing_keys = [
            key
            for key in required_citydata_keys
            if key not in city_data
        ]

        if missing_keys:
            raise AirflowFailException(
                "검증 실패: CITYDATA 필수 키 누락 | "
                f"raw_id={saved_raw_id}, missing={missing_keys}"
            )

        population_rows = city_data["LIVE_PPLTN_STTS"]

        if not isinstance(population_rows, list) or not population_rows:
            raise AirflowFailException(
                "검증 실패: LIVE_PPLTN_STTS가 비어 있거나 배열이 아닙니다. "
                f"raw_id={saved_raw_id}"
            )

        population = population_rows[0]

        required_population_keys = [
            "PPLTN_TIME",
            "AREA_CONGEST_LVL",
            "AREA_PPLTN_MIN",
            "AREA_PPLTN_MAX",
        ]

        missing_population_keys = [
            key
            for key in required_population_keys
            if key not in population
        ]

        if missing_population_keys:
            raise AirflowFailException(
                "검증 실패: 인구 데이터 필수 키 누락 | "
                f"raw_id={saved_raw_id}, "
                f"missing={missing_population_keys}"
            )

        try:
            population_min = int(population["AREA_PPLTN_MIN"])
            population_max = int(population["AREA_PPLTN_MAX"])
        except (TypeError, ValueError) as exc:
            raise AirflowFailException(
                "검증 실패: 인구 범위가 정수로 변환되지 않습니다. "
                f"raw_id={saved_raw_id}"
            ) from exc

        if population_min > population_max:
            raise AirflowFailException(
                "검증 실패: 최소 인구가 최대 인구보다 큽니다. "
                f"raw_id={saved_raw_id}, "
                f"min={population_min}, max={population_max}"
            )

        print(
            "Raw 검증 성공 | "
            f"raw_id={saved_raw_id}, "
            f"source_name={source_name}, "
            f"area_code={area_code}, "
            f"area_name={area_name}, "
            f"api_area={city_data['AREA_NM']}, "
            f"population_range={population_min}~{population_max}"
        )

        return saved_raw_id

    @task
    def load_population_to_staging(raw_id: int) -> None:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        row = hook.get_first(
            """
            SELECT
                raw_id,
                area_code,
                area_name,
                received_at,
                is_success,
                payload
            FROM raw.api_response
            WHERE raw_id = %(raw_id)s;
            """,
            parameters={"raw_id": raw_id},
        )

        if row is None:
            raise AirflowFailException(
                f"Staging 적재 실패: Raw 데이터가 없습니다. raw_id={raw_id}"
            )

        (
            saved_raw_id,
            raw_area_code,
            raw_area_name,
            collected_at,
            is_success,
            payload,
        ) = row

        if not is_success:
            raise AirflowFailException(
                f"Staging 적재 실패: 수집 실패 Raw입니다. raw_id={saved_raw_id}"
            )

        if isinstance(payload, str):
            payload = json.loads(payload)

        city_data = payload["CITYDATA"]
        population = city_data["LIVE_PPLTN_STTS"][0]

        area_code = city_data.get("AREA_CD", raw_area_code)
        area_name = city_data.get("AREA_NM", raw_area_name)

        observed_at = datetime.fromisoformat(
            population["PPLTN_TIME"]
        )

        population_min = int(population["AREA_PPLTN_MIN"])
        population_max = int(population["AREA_PPLTN_MAX"])

        # 1. 부모: Header를 먼저 UPSERT
        hook.run(
            """
            INSERT INTO staging.realtime_city_header (
                ingestion_id,
                area_code,
                area_name,
                observed_at,
                collected_at,
                result_code,
                result_message,
                is_success,
                loaded_at
            )
            VALUES (
                %(ingestion_id)s,
                %(area_code)s,
                %(area_name)s,
                %(observed_at)s,
                %(collected_at)s,
                %(result_code)s,
                %(result_message)s,
                %(is_success)s,
                now()
            )
            ON CONFLICT (ingestion_id)
            DO UPDATE SET
                area_code = EXCLUDED.area_code,
                area_name = EXCLUDED.area_name,
                observed_at = EXCLUDED.observed_at,
                collected_at = EXCLUDED.collected_at,
                result_code = EXCLUDED.result_code,
                result_message = EXCLUDED.result_message,
                is_success = EXCLUDED.is_success,
                loaded_at = now();
            """,
            parameters={
                "ingestion_id": saved_raw_id,
                "area_code": area_code,
                "area_name": area_name,
                "observed_at": observed_at,
                "collected_at": collected_at,
                "result_code": None,
                "result_message": None,
                "is_success": is_success,
            },
        )

        # 2. 자식: Header가 존재한 뒤 Population Snapshot UPSERT
        hook.run(
            """
            INSERT INTO staging.realtime_population_snapshot (
                ingestion_id,
                area_code,
                observed_at,
                congestion_level,
                congestion_message,
                population_min,
                population_max,
                male_ratio,
                female_ratio,
                resident_ratio,
                non_resident_ratio,
                loaded_at
            )
            VALUES (
                %(ingestion_id)s,
                %(area_code)s,
                %(observed_at)s,
                %(congestion_level)s,
                %(congestion_message)s,
                %(population_min)s,
                %(population_max)s,
                %(male_ratio)s,
                %(female_ratio)s,
                %(resident_ratio)s,
                %(non_resident_ratio)s,
                now()
            )
            ON CONFLICT (area_code, observed_at)
            DO UPDATE SET
                ingestion_id = EXCLUDED.ingestion_id,
                congestion_level = EXCLUDED.congestion_level,
                congestion_message = EXCLUDED.congestion_message,
                population_min = EXCLUDED.population_min,
                population_max = EXCLUDED.population_max,
                male_ratio = EXCLUDED.male_ratio,
                female_ratio = EXCLUDED.female_ratio,
                resident_ratio = EXCLUDED.resident_ratio,
                non_resident_ratio = EXCLUDED.non_resident_ratio,
                loaded_at = now();
            """,
            parameters={
                "ingestion_id": saved_raw_id,
                "area_code": area_code,
                "observed_at": observed_at,
                "congestion_level": population.get("AREA_CONGEST_LVL"),
                "congestion_message": population.get("AREA_CONGEST_MSG"),
                "population_min": population_min,
                "population_max": population_max,
                "male_ratio": population.get("MALE_PPLTN_RATE"),
                "female_ratio": population.get("FEMALE_PPLTN_RATE"),
                "resident_ratio": population.get("RESNT_PPLTN_RATE"),
                "non_resident_ratio": population.get("NON_RESNT_PPLTN_RATE"),
            },
        )

        print(
            "Staging 적재 성공 | "
            f"raw_id={saved_raw_id}, "
            f"area={area_name}, "
            f"observed_at={observed_at}, "
            f"population={population_min}~{population_max}"
        )

    raw_id = extract_dummy_to_raw()
    validated_raw_id = validate_raw_payload(raw_id)
    load_population_to_staging(validated_raw_id)

test_raw_insert()