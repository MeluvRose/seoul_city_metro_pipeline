from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal, InvalidOperation

import requests
import xmltodict
import logging

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException, AirflowException
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "seoul_city_postgres"
SOURCE_NAME = "seoul_realtime_city"
TARGET_AREA_NAMES = ["용산역", "여의도", "광화문·덕수궁"]
KST = ZoneInfo("Asia/Seoul")

def to_decimal_or_none(value: object) -> Decimal | None:
    """
    서울시 API의 숫자형 문자열을 Decimal로 변환하고, 
    None, 빈 문자열, '-', '없음' 등 수치가 아닌 표기는 None으로 처리한다.
    """
    if value is None:
        return None
    
    text = str(value).strip()
    
    if text in ('', "-", "없음", "null", "None"):
        return None
    
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"숫자 변환 불가: value={value!r}") from exc

DEFAULT_ARGS = {
    "owner": "jinsu",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}

@dag(
    dag_id="seoul_realtime_population_pipeline",
    description="서울시 실시간 도시데이터 XML을 JSONB Raw 계층에 수집하는 DAG",
    start_date=datetime(2026, 9, 21),
    schedule="30 1,3,5,7,9,11,13,21,23 * * *",
    # schedule="*/15 * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["seoul", "realtime", "xml", "population"],
)
def seoul_realtime_population_pipeline():
    @task
    def extract_real_api_to_raw(target_area: str) -> int:
        api_key = Variable.get("SEOUL_OPEN_API_KEY")

        url = (
            "http://openapi.seoul.go.kr:8088/"
            f"{api_key}/xml/citydata/1/5/{target_area}"
        )

        try:
            requested_at = datetime.now(timezone.utc)
            response = requests.get(url, timeout=30)
            http_status = response.status_code
            payload_xml = response.text
            payload_dict = xmltodict.parse(payload_xml)

        except requests.RequestException as exc:
            raise AirflowFailException(
                f"서울시 API 호출 실패: area={target_area}, error={exc}"
            ) from exc

        except Exception as exc:
            raise AirflowFailException(
                f"서울시 API XML→JSON 변환 실패: area={target_area}, error={exc}"
            ) from exc
        
        # 성공 응답: <SeoulRtd.citydata><CITYDATA>...</CITYDATA>
        city_response = payload_dict.get("SeoulRtd.citydata")

        # 오류 응답: <RESULT><CODE>...</CODE><MESSAGE>...</MESSAGE></RESULT>
        if city_response is None:
            error_response = payload_dict.get("RESULT", {})

            result_code = error_response.get("CODE")
            result_message = error_response.get("MESSAGE")

            area_code = None
            area_name = target_area

            is_success = False
        else:
            result = city_response.get("RESULT", {})
            result_code = result.get("RESULT.CODE")
            result_message = result.get("RESULT.MESSAGE")

            city_data = city_response.get("CITYDATA", {})

            area_code = city_data.get("AREA_CD")
            area_name = city_data.get("AREA_NM") or target_area

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

    # 재시도 검증용 태스크 (임시)
    # @task
    # def retry_probe() -> str:
    #     raise AirflowException(
    #         "Intentional retry test: verify task retry configuration"
    #     )

    @task
    def load_population_to_staging(raw_id: int) -> int:
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
            tzinfo=ZoneInfo("Asia/Seoul"),
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

        return saved_raw_id
    
    @task
    def load_weather_to_staging(ingestion_id: int) -> int:
        """
        현재 ingestion_id에 연결된 Raw JSONB에서 WEATHER_STTS를 추출해
        staging.realtime_weather_snapshot에 UPSERT한다.

        WEATHER_STTS가 없으면 인구 파이프라인을 실패시키지 않고,
        경고 로그를 남긴 뒤 같은 ingestion_id를 반환한다.
        """

        logger = logging.getLogger(__name__)

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID,
        )

        source_row = hook.get_first(
            """
            SELECT
                h.ingestion_id,
                h.area_code,
                h.area_name,
                h.observed_at AS header_observed_at,
                r.payload
            FROM staging.realtime_city_header AS h
            JOIN raw.api_response AS r
            ON r.raw_id = h.ingestion_id
            WHERE h.ingestion_id = %(ingestion_id)s
            AND h.is_success = true;
            """,
            parameters={
                "ingestion_id": ingestion_id,
            },
        )

        if source_row is None:
            raise AirflowException(
                "날씨 Staging 적재 실패: "
                f"성공 Header 또는 Raw payload를 찾을 수 없습니다. "
                f"ingestion_id={ingestion_id}"
            )

        (
            source_ingestion_id,
            area_code,
            area_name,
            header_observed_at,
            payload,
        ) = source_row

        # psycopg 환경에 따라 JSONB가 dict 또는 JSON 문자열로 반환될 수 있다.
        if isinstance(payload, str):
            payload = json.loads(payload)

        city_response = payload.get("SeoulRtd.citydata", {})
        city_data = city_response.get("CITYDATA", {})
        weather_wrapper = city_data.get("WEATHER_STTS", {})

        if not isinstance(weather_wrapper, dict):
            logger.warning(
                "WEATHER_STTS wrapper 형식 오류: "
                "ingestion_id=%s, area_code=%s, actual_type=%s",
                ingestion_id,
                area_code,
                type(weather_wrapper).__name__,
            )
            return ingestion_id

        weather = weather_wrapper.get("WEATHER_STTS", {})

        if not isinstance(weather, dict) or not weather:
            logger.warning(
                "내부 WEATHER_STTS 누락: Weather Snapshot을 적재하지 않습니다. "
                "ingestion_id=%s, area_code=%s, area_name=%s",
                ingestion_id,
                area_code,
                area_name,
            )
            return ingestion_id

        weather_time_text = weather.get("WEATHER_TIME")

        if not weather_time_text:
            raise AirflowException(
                "WEATHER_STTS.WEATHER_TIME 누락: "
                f"ingestion_id={ingestion_id}, area_code={area_code}"
            )

        try:
            weather_observed_at = (
                datetime.strptime(
                    str(weather_time_text),
                    "%Y-%m-%d %H:%M",
                )
                .replace(tzinfo=KST)
                .astimezone(timezone.utc)
            )
        except ValueError as exc:
            raise AirflowException(
                "WEATHER_TIME 파싱 실패: "
                f"ingestion_id={ingestion_id}, "
                f"area_code={area_code}, "
                f"value={weather_time_text!r}"
            ) from exc

        try:
            temperature_c = to_decimal_or_none(weather.get("TEMP"))
            humidity_pct = to_decimal_or_none(weather.get("HUMIDITY"))
            precipitation_mm = to_decimal_or_none(
                weather.get("PRECIPITATION")
            )
            wind_speed_mps = to_decimal_or_none(
                weather.get("WIND_SPD")
            )
            pm10 = to_decimal_or_none(weather.get("PM10"))
            pm25 = to_decimal_or_none(weather.get("PM25"))
            weather_description = "\n".join([
                weather.get("AIR_MSG"),
                weather.get("PCP_MSG"),
                weather.get("UV_MSG"),
            ])
        except ValueError as exc:
            raise AirflowException(
                "WEATHER_STTS 수치 필드 변환 실패: "
                f"ingestion_id={ingestion_id}, "
                f"area_code={area_code}, error={exc}"
            ) from exc

        precipitation_type = weather.get("PRECPT_TYPE")
        air_quality_index = weather.get("AIR_IDX")

        hook.run(
            """
            INSERT INTO staging.realtime_weather_snapshot (
                ingestion_id,
                area_code,
                observed_at,
                temperature_c,
                feels_like_c,
                humidity_pct,
                precipitation_type,
                precipitation_mm,
                wind_speed_mps,
                wind_direction_deg,
                pm10,
                pm25,
                air_quality_index,
                weather_description
            )
            VALUES (
                %(ingestion_id)s,
                %(area_code)s,
                %(observed_at)s,
                %(temperature_c)s,
                NULL,
                %(humidity_pct)s,
                %(precipitation_type)s,
                %(precipitation_mm)s,
                %(wind_speed_mps)s,
                NULL,
                %(pm10)s,
                %(pm25)s,
                %(air_quality_index)s,
                %(weather_description)s
            )
            ON CONFLICT (ingestion_id)
            DO UPDATE SET
                area_code = EXCLUDED.area_code,
                observed_at = EXCLUDED.observed_at,
                temperature_c = EXCLUDED.temperature_c,
                feels_like_c = EXCLUDED.feels_like_c,
                humidity_pct = EXCLUDED.humidity_pct,
                precipitation_type = EXCLUDED.precipitation_type,
                precipitation_mm = EXCLUDED.precipitation_mm,
                wind_speed_mps = EXCLUDED.wind_speed_mps,
                wind_direction_deg = EXCLUDED.wind_direction_deg,
                pm10 = EXCLUDED.pm10,
                pm25 = EXCLUDED.pm25,
                air_quality_index = EXCLUDED.air_quality_index,
                weather_description = EXCLUDED.weather_description,
                loaded_at = CURRENT_TIMESTAMP;
            """,
            parameters={
                "ingestion_id": source_ingestion_id,
                "area_code": area_code,
                "observed_at": weather_observed_at,
                "temperature_c": temperature_c,
                "humidity_pct": humidity_pct,
                "precipitation_type": precipitation_type,
                "precipitation_mm": precipitation_mm,
                "wind_speed_mps": wind_speed_mps,
                "pm10": pm10,
                "pm25": pm25,
                "air_quality_index": air_quality_index,
                "weather_description": weather_description,
            },
        )

        logger.info(
            "Weather Staging UPSERT 성공 | "
            "ingestion_id=%s, area_code=%s, area_name=%s, "
            "weather_observed_at=%s, header_observed_at=%s, "
            "temperature_c=%s, humidity_pct=%s, pm10=%s, pm25=%s",
            source_ingestion_id,
            area_code,
            area_name,
            weather_observed_at,
            header_observed_at,
            temperature_c,
            humidity_pct,
            pm10,
            pm25,
        )

        return ingestion_id

    @task
    def load_dimensions(ingestion_id: int) -> int:
        """
        현재 ingestion_id에 포함된 장소를 dim_location에 적재한다.

        dim_date와 dim_hour는 정적 차원이므로,
        최초 SQL 실행으로 생성된 상태를 전제한다.
        """
        sql = """
        INSERT INTO warehouse.dim_location (
            area_code,
            area_name,
            timezone_name,
            is_active,
            updated_at
        )
        SELECT DISTINCT
            area_code,
            area_name,
            'Asia/Seoul',
            true,
            CURRENT_TIMESTAMP
        FROM staging.v_realtime_population_for_dw
        WHERE ingestion_id = %(ingestion_id)s

        ON CONFLICT (area_code)
        DO UPDATE SET
            area_name = EXCLUDED.area_name,
            timezone_name = EXCLUDED.timezone_name,
            is_active = EXCLUDED.is_active,
            updated_at = CURRENT_TIMESTAMP
        WHERE warehouse.dim_location.area_name
                IS DISTINCT FROM EXCLUDED.area_name
        OR warehouse.dim_location.timezone_name
                IS DISTINCT FROM EXCLUDED.timezone_name
        OR warehouse.dim_location.is_active
                IS DISTINCT FROM EXCLUDED.is_active;
        """

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, {"ingestion_id": ingestion_id})

        return ingestion_id

    @task
    def load_fact_city_snapshot(ingestion_id: int) -> int:
        """
        Staging의 현재 ingestion_id 데이터를
        Warehouse의 원자 Fact로 적재한다.

        Fact grain:
        한 생활권(location)의 한 observed_at 시점에 대한
        인구·혼잡 Snapshot 1건
        """
        sql = """
        WITH src AS (
            SELECT
                s.*,
                s.observed_at AT TIME ZONE 'Asia/Seoul' AS observed_at_kst
            FROM staging.v_realtime_population_for_dw AS s
            WHERE s.ingestion_id = %(ingestion_id)s
        )
        INSERT INTO warehouse.fact_city_snapshot (
            location_id,
            date_key,
            hour_key,
            observed_at,
            collected_at,
            congestion_level,
            congestion_message,
            population_min,
            population_max,
            male_ratio,
            female_ratio,
            resident_ratio,
            non_resident_ratio,
            source_ingestion_id
        )
        SELECT
            l.location_id,
            TO_CHAR(src.observed_at_kst::date, 'YYYYMMDD')::integer,
            EXTRACT(HOUR FROM src.observed_at_kst)::smallint,
            src.observed_at,
            src.collected_at,
            src.congestion_level,
            src.congestion_message,
            src.population_min,
            src.population_max,
            src.male_ratio,
            src.female_ratio,
            src.resident_ratio,
            src.non_resident_ratio,
            src.ingestion_id
        FROM src
        JOIN warehouse.dim_location AS l
        ON l.area_code = src.area_code

        ON CONFLICT ON CONSTRAINT uq_fact_city_location_observed
        DO UPDATE SET
            date_key = EXCLUDED.date_key,
            hour_key = EXCLUDED.hour_key,
            collected_at = EXCLUDED.collected_at,
            congestion_level = EXCLUDED.congestion_level,
            congestion_message = EXCLUDED.congestion_message,
            population_min = EXCLUDED.population_min,
            population_max = EXCLUDED.population_max,
            male_ratio = EXCLUDED.male_ratio,
            female_ratio = EXCLUDED.female_ratio,
            resident_ratio = EXCLUDED.resident_ratio,
            non_resident_ratio = EXCLUDED.non_resident_ratio,
            source_ingestion_id = EXCLUDED.source_ingestion_id,
            dw_updated_at = CURRENT_TIMESTAMP
        WHERE EXCLUDED.collected_at >= warehouse.fact_city_snapshot.collected_at;
        """

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        with hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, {"ingestion_id": ingestion_id})

        return ingestion_id

    @task
    def validate_dw_load(ingestion_id: int) -> int:
        """
        현재 DAG 실행의 ingestion_id가 Warehouse에
        정상 적재되었는지 검증한다.

        검증 실패 시 AirflowException을 발생시켜
        DAG 실행을 실패 처리한다.
        """
        sql = """
        WITH current_fact AS (
            SELECT
                f.city_snapshot_id,
                f.location_id,
                f.date_key,
                f.hour_key,
                f.observed_at,
                f.collected_at,
                f.population_min,
                f.population_max,
                f.source_ingestion_id
            FROM warehouse.fact_city_snapshot AS f
            WHERE f.source_ingestion_id = %(ingestion_id)s
        )

        SELECT
            COUNT(*) AS fact_count,

            COUNT(*) FILTER (
                WHERE l.location_id IS NULL
            ) AS missing_location_count,

            COUNT(*) FILTER (
                WHERE d.date_key IS NULL
            ) AS missing_date_count,

            COUNT(*) FILTER (
                WHERE h.hour_key IS NULL
            ) AS missing_hour_count,

            COUNT(*) FILTER (
                WHERE f.population_min IS NOT NULL
                AND f.population_max IS NOT NULL
                AND f.population_min > f.population_max
            ) AS invalid_population_range_count,

            COUNT(*) FILTER (
                WHERE f.observed_at > f.collected_at
            ) AS future_observation_count,

            COUNT(*) FILTER (
                WHERE f.date_key <> TO_CHAR(
                    (f.observed_at AT TIME ZONE 'Asia/Seoul')::DATE,
                    'YYYYMMDD'
                )::INTEGER
                OR f.hour_key <> EXTRACT(
                    HOUR FROM f.observed_at AT TIME ZONE 'Asia/Seoul'
                )::SMALLINT
            ) AS time_key_mismatch_count,

            COUNT(*) FILTER (
                WHERE p.population_snapshot_id IS NULL
            ) AS missing_staging_population_count,

            COUNT(*) FILTER (
                WHERE p.population_snapshot_id IS NOT NULL
                AND (
                        f.population_min IS DISTINCT FROM p.population_min
                    OR f.population_max IS DISTINCT FROM p.population_max
                )
            ) AS staging_fact_population_mismatch_count

        FROM current_fact AS f

        LEFT JOIN warehouse.dim_location AS l
            ON l.location_id = f.location_id

        LEFT JOIN warehouse.dim_date AS d
            ON d.date_key = f.date_key

        LEFT JOIN warehouse.dim_hour AS h
            ON h.hour_key = f.hour_key

        LEFT JOIN staging.realtime_population_snapshot AS p
            ON p.ingestion_id = f.source_ingestion_id
            AND p.area_code = l.area_code
            AND p.observed_at = f.observed_at;
        """

        logger = logging.getLogger(__name__)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        row = hook.get_first(
            sql,
            parameters={"ingestion_id": ingestion_id},
        )

        (
            fact_count,
            missing_location_count,
            missing_date_count,
            missing_hour_count,
            invalid_population_range_count,
            future_observation_count,
            time_key_mismatch_count,
            missing_staging_population_count,
            staging_fact_population_mismatch_count,
        ) = row

        metrics = {
        "ingestion_id": ingestion_id,
        "fact_count": fact_count,
        "missing_location_count": missing_location_count,
        "missing_date_count": missing_date_count,
        "missing_hour_count": missing_hour_count,
        "invalid_population_range_count": invalid_population_range_count,
        "future_observation_count": future_observation_count,
        "time_key_mismatch_count": time_key_mismatch_count,
        "missing_staging_population_count": missing_staging_population_count,
        "staging_fact_population_mismatch_count": staging_fact_population_mismatch_count,
        }

        if fact_count != 1:
            raise AirflowException(
                f"DW Fact 건수 오류: ingestion_id={ingestion_id}, "
                f"expected=1, actual={fact_count}"
            )

        failed_checks = {
            "missing_location_count": missing_location_count,
            "missing_date_count": missing_date_count,
            "missing_hour_count": missing_hour_count,
            "invalid_population_range_count": invalid_population_range_count,
            "future_observation_count": future_observation_count,
            "time_key_mismatch_count": time_key_mismatch_count,
            "staging_fact_population_mismatch_count":
                staging_fact_population_mismatch_count,
        }

        violations = {
            name: count
            for name, count in failed_checks.items()
            if count != 0
        }

        if violations:
            raise AirflowException(
                f"DW 데이터 품질 검증 실패: "
                f"ingestion_id={ingestion_id}, violations={violations}"
            )

        logger.info("DW 데이터 품질 검증 성공. metrics=%s", metrics)
        return ingestion_id

    @task
    def load_fact_weather_snapshot(ingestion_id: int) -> int:
        """
        Staging Weather Snapshot을 Warehouse Weather Fact에 UPSERT한다.

        Fact grain:
        한 지역의 한 실제 날씨 관측 시각에 대한 날씨·대기질 Snapshot 1건.

        Business key:
        (location_id, observed_at)
        """

        logger = logging.getLogger(__name__)

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID,
        )

        # hook.get_first()에 서로 다른 작업의 여러 SQL 문장을 전달하는
        # 방식은 환경에 따라 마지막 결과까지 안정적으로 받지 못할 수 있음
        # 따라서, UPSERT와 건수 조회를 분리하는 것을 권장

        upsert_sql = """
        WITH source_weather AS (
            SELECT
                w.ingestion_id,
                w.area_code,
                w.observed_at,
                w.temperature_c,
                w.feels_like_c,
                w.humidity_pct,
                w.precipitation_type,
                w.precipitation_mm,
                w.wind_speed_mps,
                w.wind_direction_deg,
                w.pm10,
                w.pm25,
                w.air_quality_index,
                w.weather_description,
                h.collected_at,
                w.observed_at AT TIME ZONE 'Asia/Seoul'
                    AS observed_at_kst
            FROM staging.realtime_weather_snapshot AS w
            JOIN staging.realtime_city_header AS h
            ON h.ingestion_id = w.ingestion_id
            WHERE w.ingestion_id = %(ingestion_id)s
            AND h.is_success = true
        )
        INSERT INTO warehouse.fact_weather_snapshot (
            location_id,
            date_key,
            hour_key,
            observed_at,
            collected_at,
            temperature_c,
            feels_like_c,
            humidity_pct,
            precipitation_type,
            precipitation_mm,
            wind_speed_mps,
            wind_direction_deg,
            pm10,
            pm25,
            air_quality_index,
            weather_description,
            source_ingestion_id
        )
        SELECT
            l.location_id,
            TO_CHAR(
                s.observed_at_kst::DATE,
                'YYYYMMDD'
            )::INTEGER,
            EXTRACT(
                HOUR FROM s.observed_at_kst
            )::SMALLINT,
            s.observed_at,
            s.collected_at,
            s.temperature_c,
            s.feels_like_c,
            s.humidity_pct,
            s.precipitation_type,
            s.precipitation_mm,
            s.wind_speed_mps,
            s.wind_direction_deg,
            s.pm10,
            s.pm25,
            s.air_quality_index,
            s.weather_description,
            s.ingestion_id
        FROM source_weather AS s
        JOIN warehouse.dim_location AS l
        ON l.area_code = s.area_code
        ON CONFLICT ON CONSTRAINT uq_fact_weather_location_observed
        DO UPDATE
        SET
            date_key = EXCLUDED.date_key,
            hour_key = EXCLUDED.hour_key,
            collected_at = EXCLUDED.collected_at,
            temperature_c = EXCLUDED.temperature_c,
            feels_like_c = EXCLUDED.feels_like_c,
            humidity_pct = EXCLUDED.humidity_pct,
            precipitation_type = EXCLUDED.precipitation_type,
            precipitation_mm = EXCLUDED.precipitation_mm,
            wind_speed_mps = EXCLUDED.wind_speed_mps,
            wind_direction_deg = EXCLUDED.wind_direction_deg,
            pm10 = EXCLUDED.pm10,
            pm25 = EXCLUDED.pm25,
            air_quality_index = EXCLUDED.air_quality_index,
            weather_description = EXCLUDED.weather_description,
            source_ingestion_id = EXCLUDED.source_ingestion_id,
            dw_updated_at = CURRENT_TIMESTAMP
        WHERE EXCLUDED.collected_at >=
            warehouse.fact_weather_snapshot.collected_at;
        """

        count_sql = """
        SELECT COUNT(*)
        FROM warehouse.fact_weather_snapshot
        WHERE source_ingestion_id = %(ingestion_id)s;
        """

        hook.run(
            upsert_sql,
            parameters={
                "ingestion_id": ingestion_id,
            },
        )

        result = hook.get_first(
            count_sql,
            parameters={
                "ingestion_id": ingestion_id,
            },
        )

        fact_count = result[0]

        logger.info(
            "Weather Fact UPSERT 완료 | "
            "ingestion_id=%s, fact_count=%s",
            ingestion_id,
            fact_count,
        )

        if fact_count != 1:
            raise AirflowException(
                "Weather Fact 적재 건수 오류: "
                f"ingestion_id={ingestion_id}, "
                f"expected=1, actual={fact_count}"
            )

        return ingestion_id

    @task
    def validate_weather_dw_load(ingestion_id: int) -> int:
        """
        현재 ingestion_id의 Weather Staging → Weather Fact 적재 결과를 검증한다.

        검증 실패 시 AirflowException을 발생시켜
        해당 DAG Run을 실패 처리한다.
        """

        logger = logging.getLogger(__name__)

        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID,
        )

        sql = """
        WITH current_fact AS (
            SELECT
                f.weather_snapshot_id,
                f.location_id,
                f.date_key,
                f.hour_key,
                f.observed_at,
                f.collected_at,
                f.temperature_c,
                f.humidity_pct,
                f.precipitation_mm,
                f.wind_speed_mps,
                f.pm10,
                f.pm25,
                f.source_ingestion_id
            FROM warehouse.fact_weather_snapshot AS f
            WHERE f.source_ingestion_id = %(ingestion_id)s
        )
        SELECT
            COUNT(*) AS fact_count,

            COUNT(*) FILTER (
                WHERE l.location_id IS NULL
            ) AS missing_location_count,

            COUNT(*) FILTER (
                WHERE d.date_key IS NULL
            ) AS missing_date_count,

            COUNT(*) FILTER (
                WHERE h.hour_key IS NULL
            ) AS missing_hour_count,

            COUNT(*) FILTER (
                WHERE f.humidity_pct IS NOT NULL
                AND f.humidity_pct NOT BETWEEN 0 AND 100
            ) AS invalid_humidity_count,

            COUNT(*) FILTER (
                WHERE f.precipitation_mm IS NOT NULL
                AND f.precipitation_mm < 0
            ) AS invalid_precipitation_count,

            COUNT(*) FILTER (
                WHERE f.wind_speed_mps IS NOT NULL
                AND f.wind_speed_mps < 0
            ) AS invalid_wind_speed_count,

            COUNT(*) FILTER (
                WHERE f.pm10 IS NOT NULL
                AND f.pm10 < 0
            ) AS invalid_pm10_count,

            COUNT(*) FILTER (
                WHERE f.pm25 IS NOT NULL
                AND f.pm25 < 0
            ) AS invalid_pm25_count,

            COUNT(*) FILTER (
                WHERE f.observed_at > f.collected_at
            ) AS future_observation_count,

            COUNT(*) FILTER (
                WHERE f.date_key <> TO_CHAR(
                    (f.observed_at AT TIME ZONE 'Asia/Seoul')::DATE,
                    'YYYYMMDD'
                )::INTEGER
                OR f.hour_key <> EXTRACT(
                    HOUR FROM f.observed_at AT TIME ZONE 'Asia/Seoul'
                )::SMALLINT
            ) AS time_key_mismatch_count,

            COUNT(*) FILTER (
                WHERE w.ingestion_id IS NULL
            ) AS missing_staging_weather_count,

            COUNT(*) FILTER (
                WHERE w.ingestion_id IS NOT NULL
                AND (
                        f.observed_at IS DISTINCT FROM w.observed_at
                    OR f.temperature_c IS DISTINCT FROM w.temperature_c
                    OR f.humidity_pct IS DISTINCT FROM w.humidity_pct
                    OR f.precipitation_mm IS DISTINCT FROM w.precipitation_mm
                    OR f.wind_speed_mps IS DISTINCT FROM w.wind_speed_mps
                    OR f.pm10 IS DISTINCT FROM w.pm10
                    OR f.pm25 IS DISTINCT FROM w.pm25
                )
            ) AS staging_fact_weather_mismatch_count

        FROM current_fact AS f

        LEFT JOIN warehouse.dim_location AS l
        ON l.location_id = f.location_id

        LEFT JOIN warehouse.dim_date AS d
        ON d.date_key = f.date_key

        LEFT JOIN warehouse.dim_hour AS h
        ON h.hour_key = f.hour_key

        LEFT JOIN staging.realtime_weather_snapshot AS w
        ON w.ingestion_id = f.source_ingestion_id
        AND w.area_code = l.area_code
        AND w.observed_at = f.observed_at;
        """

        row = hook.get_first(
            sql,
            parameters={
                "ingestion_id": ingestion_id,
            },
        )

        (
            fact_count,
            missing_location_count,
            missing_date_count,
            missing_hour_count,
            invalid_humidity_count,
            invalid_precipitation_count,
            invalid_wind_speed_count,
            invalid_pm10_count,
            invalid_pm25_count,
            future_observation_count,
            time_key_mismatch_count,
            missing_staging_weather_count,
            staging_fact_weather_mismatch_count,
        ) = row

        metrics = {
            "ingestion_id": ingestion_id,
            "fact_count": fact_count,
            "missing_location_count": missing_location_count,
            "missing_date_count": missing_date_count,
            "missing_hour_count": missing_hour_count,
            "invalid_humidity_count": invalid_humidity_count,
            "invalid_precipitation_count": invalid_precipitation_count,
            "invalid_wind_speed_count": invalid_wind_speed_count,
            "invalid_pm10_count": invalid_pm10_count,
            "invalid_pm25_count": invalid_pm25_count,
            "future_observation_count": future_observation_count,
            "time_key_mismatch_count": time_key_mismatch_count,
            "missing_staging_weather_count": missing_staging_weather_count,
            "staging_fact_weather_mismatch_count":
                staging_fact_weather_mismatch_count,
        }

        if fact_count != 1:
            raise AirflowException(
                "Weather Fact 건수 오류: "
                f"ingestion_id={ingestion_id}, "
                f"expected=1, actual={fact_count}, "
                f"metrics={metrics}"
            )

        failed_checks = {
            "missing_location_count": missing_location_count,
            "missing_date_count": missing_date_count,
            "missing_hour_count": missing_hour_count,
            "invalid_humidity_count": invalid_humidity_count,
            "invalid_precipitation_count": invalid_precipitation_count,
            "invalid_wind_speed_count": invalid_wind_speed_count,
            "invalid_pm10_count": invalid_pm10_count,
            "invalid_pm25_count": invalid_pm25_count,
            "future_observation_count": future_observation_count,
            "time_key_mismatch_count": time_key_mismatch_count,
            "missing_staging_weather_count": missing_staging_weather_count,
            "staging_fact_weather_mismatch_count":
                staging_fact_weather_mismatch_count,
        }

        violations = {
            name: count
            for name, count in failed_checks.items()
            if count != 0
        }

        if violations:
            raise AirflowException(
                "Weather DW 데이터 품질 검증 실패: "
                f"ingestion_id={ingestion_id}, "
                f"violations={violations}, "
                f"metrics={metrics}"
            )

        logger.info(
            "Weather DW 데이터 품질 검증 성공. metrics=%s",
            metrics,
        )

        return ingestion_id

    @task_group
    def location_etl_pipeline(area_name: str) -> int:
        raw_id = extract_real_api_to_raw(area_name)
        validated_raw_id = validate_raw_payload(raw_id)
        
        # validated_raw_id = 741
        # retry_probe()
        
        ingestion_id = load_population_to_staging(validated_raw_id)
        dimension_ingestion_id = load_dimensions(ingestion_id)
        weather_ingestion_id = load_weather_to_staging(ingestion_id)
        fact_ingestion_id = load_fact_city_snapshot(dimension_ingestion_id)
        city_validation_ingestion_id = validate_dw_load(fact_ingestion_id)
        weather_fact_ingestion_id = load_fact_weather_snapshot(weather_ingestion_id)
        
        return validate_weather_dw_load(weather_fact_ingestion_id)
         

    location_etl_pipeline.override(group_id="yongsan_station")(area_name="용산역")
    location_etl_pipeline.override(group_id="yeouido")(area_name="여의도")
    location_etl_pipeline.override(group_id="guanghwamun")(area_name="광화문·덕수궁")


seoul_realtime_population_pipeline()