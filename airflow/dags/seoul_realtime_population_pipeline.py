from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import requests
import xmltodict

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "seoul_city_postgres"
SOURCE_NAME = "seoul_realtime_city"
TARGET_AREA_NAME = "용산역"


@dag(
    dag_id="seoul_realtime_population_pipeline",
    description="서울시 실시간 도시데이터 XML을 JSONB Raw 계층에 수집하는 DAG",
    start_date=datetime(2026, 9, 1),
    schedule=None,
    catchup=False,
    tags=["seoul", "realtime", "xml", "population"],
)
def seoul_realtime_population_pipeline():
    @task
    def extract_real_api_to_raw() -> int:
        api_key = Variable.get("SEOUL_OPEN_API_KEY")

        url = (
            "http://openapi.seoul.go.kr:8088/"
            f"{api_key}/xml/citydata/1/5/{TARGET_AREA_NAME}"
        )

        try:
            requested_at = datetime.now(timezone.utc)
            response = requests.get(url, timeout=30)
            http_status = response.status_code
            payload_xml = response.text
            payload_dict = xmltodict.parse(payload_xml)

        except requests.RequestException as exc:
            raise AirflowFailException(
                f"서울시 API 호출 실패: area={TARGET_AREA_NAME}, error={exc}"
            ) from exc

        except Exception as exc:
            raise AirflowFailException(
                f"서울시 API XML→JSON 변환 실패: area={TARGET_AREA_NAME}, error={exc}"
            ) from exc
        
        # 성공 응답: <SeoulRtd.citydata><CITYDATA>...</CITYDATA>
        city_response = payload_dict.get("SeoulRtd.citydata")

        # 오류 응답: <RESULT><CODE>...</CODE><MESSAGE>...</MESSAGE></RESULT>
        if city_response is None:
            error_response = payload_dict.get("RESULT", {})

            result_code = error_response.get("CODE")
            result_message = error_response.get("MESSAGE")

            area_code = None
            area_name = TARGET_AREA_NAME

            is_success = False
        else:
            result = city_response.get("RESULT", {})
            result_code = result.get("RESULT.CODE")
            result_message = result.get("RESULT.MESSAGE")

            city_data = city_response.get("CITYDATA", {})

            area_code = city_data.get("AREA_CD")
            area_name = city_data.get("AREA_NM") or TARGET_AREA_NAME

            is_success = (
                http_status == 200
                and result_code == "INFO-000"
                and bool(city_data)
            )

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
                payload_xml,
                payload
            )
            VALUES (
                %(source_name)s,
                %(area_code)s,
                %(area_name)s,
                %(requested_at)s,
                %(http_status)s,
                %(is_success)s,
                %(payload_xml)s,
                %(payload)s::jsonb
            )
            RETURNING raw_id;
            """,
            parameters={
                "source_name": SOURCE_NAME,
                "area_code": area_code,
                "area_name": area_name,
                "requested_at": requested_at,
                "http_status": http_status,
                "is_success": is_success,
                "payload_xml": payload_xml,
                "payload": json.dumps(
                    payload_dict,
                    ensure_ascii=False,
                ),
            },
        )

        raw_id = result[0]

        if not is_success:
            raise AirflowFailException(
                "서울시 API 오류 응답을 Raw에 저장했습니다 | "
                f"raw_id={raw_id}, "
                f"result_code={result_code}, "
                f"result_message={result_message}"
            )

        print(
            "실제 XML·JSONB Raw 적재 성공 | "
            f"raw_id={raw_id}, "
            f"result_code={result_code}, "
            f"area_code={area_code}, "
            f"area_name={area_name}, "
            f"http_status={http_status}"
        )

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
                f"검증 실패: Raw 데이터가 없습니다. raw_id={raw_id}"
            )

        (
            saved_raw_id,
            source_name,
            raw_area_code,
            raw_area_name,
            is_success,
            payload,
        ) = row

        if source_name != SOURCE_NAME:
            raise AirflowFailException(
                "검증 실패: 예상과 다른 source입니다. "
                f"raw_id={saved_raw_id}, source={source_name}"
            )

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

        city_response = payload.get("SeoulRtd.citydata")

        if not isinstance(city_response, dict):
            raise AirflowFailException(
                "검증 실패: SeoulRtd.citydata 객체가 없습니다. "
                f"raw_id={saved_raw_id}"
            )

        result = city_response.get("RESULT", {})
        result_code = result.get("RESULT.CODE")

        if result_code != "INFO-000":
            raise AirflowFailException(
                "검증 실패: API 성공 코드가 아닙니다. "
                f"raw_id={saved_raw_id}, result_code={result_code}"
            )

        city_data = city_response.get("CITYDATA")

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
            if not city_data.get(key)
        ]

        if missing_keys:
            raise AirflowFailException(
                "검증 실패: CITYDATA 필수 키 누락 | "
                f"raw_id={saved_raw_id}, missing={missing_keys}"
            )

        population_container = city_data.get("LIVE_PPLTN_STTS")

        if not isinstance(population_container, dict):
            raise AirflowFailException(
                "검증 실패: LIVE_PPLTN_STTS 컨테이너가 딕셔너리가 아닙니다. "
                f"raw_id={saved_raw_id}"
            )

        population = population_container.get("LIVE_PPLTN_STTS")

        if not isinstance(population, dict):
            raise AirflowFailException(
                "검증 실패: 현재 인구 객체를 찾지 못했습니다. "
                f"raw_id={saved_raw_id}"
            )

        required_population_keys = [
            "PPLTN_TIME",
            "AREA_CONGEST_LVL",
            "AREA_PPLTN_MIN",
            "AREA_PPLTN_MAX",
        ]

        missing_population_keys = [
            key
            for key in required_population_keys
            if population.get(key) in (None, "")
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
                "검증 실패: 인구 범위 숫자 변환 실패 | "
                f"raw_id={saved_raw_id}"
            ) from exc

        if population_min > population_max:
            raise AirflowFailException(
                "검증 실패: 최소 인구가 최대 인구보다 큽니다 | "
                f"raw_id={saved_raw_id}, "
                f"min={population_min}, max={population_max}"
            )

        print(
            "실제 XML 변환 JSONB 검증 성공 | "
            f"raw_id={saved_raw_id}, "
            f"area_code={city_data['AREA_CD']}, "
            f"area_name={city_data['AREA_NM']}, "
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
                requested_at,
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
            requested_at,
            is_success,
            payload,
        ) = row

        if not is_success:
            raise AirflowFailException(
                f"Staging 적재 실패: 수집 실패 Raw입니다. raw_id={saved_raw_id}"
            )

        if isinstance(payload, str):
            payload = json.loads(payload)

        city_response = payload["SeoulRtd.citydata"]
        result = city_response["RESULT"]
        city_data = city_response["CITYDATA"]
        population = city_data["LIVE_PPLTN_STTS"]["LIVE_PPLTN_STTS"]

        area_code = city_data.get("AREA_CD", raw_area_code)
        area_name = city_data.get("AREA_NM", raw_area_name)

        observed_at = datetime.strptime(
            population["PPLTN_TIME"],
            "%Y-%m-%d %H:%M",
        ).replace(
            tzinfo=timezone.utc,
        )

        population_min = int(population["AREA_PPLTN_MIN"])
        population_max = int(population["AREA_PPLTN_MAX"])

        def to_float_or_none(value: str | None) -> float | None:
            if value is None:
                return None

            value = str(value).strip()

            if value in {"", "*", "\\N"}:
                return None

            return float(value)

        # 1. 부모: Raw의 raw_id를 Header의 ingestion_id로 먼저 적재
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
                "collected_at": requested_at,
                "result_code": result.get("RESULT.CODE"),
                "result_message": result.get("RESULT.MESSAGE"),
                "is_success": is_success,
            },
        )

        # 2. 자식: Header 존재 후 Snapshot을 UPSERT
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
                "male_ratio": to_float_or_none(
                    population.get("MALE_PPLTN_RATE")
                ),
                "female_ratio": to_float_or_none(
                    population.get("FEMALE_PPLTN_RATE")
                ),
                "resident_ratio": to_float_or_none(
                    population.get("RESNT_PPLTN_RATE")
                ),
                "non_resident_ratio": to_float_or_none(
                    population.get("NON_RESNT_PPLTN_RATE")
                ),
            },
        )

        print(
            "실제 XML 변환 JSONB Staging 적재 성공 | "
            f"raw_id={saved_raw_id}, "
            f"area={area_name}, "
            f"observed_at={observed_at}, "
            f"population={population_min}~{population_max}"
        )

    raw_id = extract_real_api_to_raw()
    validated_raw_id = validate_raw_payload(raw_id)
    load_population_to_staging(validated_raw_id)

seoul_realtime_population_pipeline()