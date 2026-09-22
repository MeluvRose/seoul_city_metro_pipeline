# 서울 주요 생활권 실시간 인구·혼잡·날씨·지하철 데이터 파이프라인

서울시 실시간 도시데이터 API와 서울시 지하철 호선별 역별
승하차 인원 정보 API의 응답 데이터를 수집해,
서울 주요 생활권의 실시간 인구·혼잡·날씨 및 대기질, 날짜
및 시간대별 지하철 이용량 데이터를
`Raw` → `Staging` → `Warehouse` → `Mart` 계층으로 적재하는
Airflow 기반 데이터 파이프라인 프로젝트입니다.

현재 광화문·덕수궁, 명동 관광특구, 강남 MICE 관광특구,
여의도, 홍대 관광특구, 용산역, 잠실 관광특구,
성수카페거리 등 8개 지역을 대상으로 수집합니다.

## 구현 과정

### Raw to Staging ETL

서울시 실시간 도시데이터 API와 서울시 지하철 호선별 역별 승하차 인원 정보 
API의 응답을 Airflow로 수집하고,
원문의 응답에 포함되는 문서 형식에 따라 JSON 형식으로 변환한 결과를 
PostgreSQL Raw 계층에 함께 보존했습니다.

**(이 다음(아래)으로 전부 수정 필요)**

이후 Validate 태스크에서 API 성공 코드, 응답 구조, 필수 필드,
인구 범위 규칙을 검증하고, 통과한 데이터만 Staging 계층의
`realtime_city_header`와 `realtime_population_snapshot`에 적재했습니다.

- Airflow TaskFlow API 기반 DAG 구성
- `extract → validate → load_staging` 태스크 의존성 구현
- Raw XML 및 JSONB 이중 보존
- `raw_id → ingestion_id` 기반 데이터 계보 추적
- Header → Population Snapshot 외래키 관계 적용
- `(area_code, observed_at)` 기준 UPSERT로 중복 적재 방지
- 정상·비정상 데이터에 대한 파이프라인 동작 검증

💡 hello_airflow.py와 test_raw_insert.py는 "학습 및 회귀 테스트 목적"으로 생성된 더미 DAG이므로, 실제 운영 환경에서는 제거할 예정입니다.

### Airflow 기반 Staging → Warehouse 적재

기존 Raw → Validate → Staging 파이프라인 뒤에 Warehouse 적재 단계를 추가했다.

```text
extract_real_api_to_raw
        ↓
validate_raw_payload
        ↓
load_population_to_staging
        ↓
load_dimensions
        ↓
load_fact_city_snapshot
        ↓
validate_dw_load
```

- `load_dimensions`: `area_code`를 자연 키로 사용해 `warehouse.dim_location`을 UPSERT한다.
- `load_fact_city_snapshot`: 생활권별 실제 관측 시각의 인구·혼잡 상태를 원자 Fact로 적재한다.
- Fact의 업무 키는 `(location_id, observed_at)`이며, 동일 관측값의 재처리 시 중복 INSERT 대신 UPSERT를 수행한다.
- `observed_at`은 서울 현지 시간으로 해석한 뒤 UTC `timestamptz`로 저장한다.
- `date_key`, `hour_key`는 `Asia/Seoul` 기준 `observed_at`에서 파생한다.
- `validate_dw_load`: Dimension 참조, 날짜·시간 키, 인구 범위, 관측·수집 시각 순서를 검증한다.

### 3개 지역 확장

기존 용산역 단일 지역 파이프라인을 다음 3개 생활권으로 확장했다.

- `POI009`: 광화문·덕수궁
- `POI046`: 용산역
- `POI072`: 여의도

API 요청에는 장소명 `area_name`을 사용하고, 성공 응답의 `CITYDATA.AREA_CD`를 추출해 Staging 및 Warehouse의 자연 키로 사용했다.

각 지역은 독립적으로 Raw → Validate → Staging → Dimension → Fact → DW Validation 체인을 수행한다. Warehouse Fact는 `UNIQUE (location_id, observed_at)` 제약과 UPSERT를 사용하므로, 동일 지역·동일 관측 시각의 재처리에서도 중복 행이 생성되지 않는다.

최신 검증에서 세 지역은 모두 동일한 관측 시각인 2026-09-16 14:55 KST에 대해 Fact 1건씩 적재되었으며, 업무 키 중복은 발생하지 않았다.

### 운영 스케줄 검증

Airflow Scheduler가 UTC 기준 운영 cron에 따라 실제 DagRun을
자동 생성하고, 파이프라인이 정상 완료되는지 검증했다.

- 운영 Cron: `30 21,23,1,3,5,7,9,11,13 * * *` (UTC)
- KST 실행 시각:
  06:30, 08:30, 10:30, 12:30, 14:30,
  16:30, 18:30, 20:30, 22:30
- 실행 횟수: 하루 9회
- 최신 DagRun 유형: `Scheduled`
- 최신 DagRun 상태: `Success`
- `catchup=False`: 과거 실행 구간의 실시간 API 호출을 방지
- `max_active_runs=1`: 이전 실행이 끝나기 전 다음 실행이
  중첩되지 않도록 제한

검증 결과, 수동 Trigger 없이 Airflow Scheduler가 설정된 cron에 따라
DAG를 생성하고 실행하며, Raw → Staging → DW → Mart 흐름이
정상 완료됨을 확인했다.

## 구현 상태

### 완료

- 서울시 실시간 도시데이터 API의 XML 응답 수집
- XML 원문 및 XML-to-JSON 변환 결과(JSONB) Raw 보관
- Raw → Validate → Staging Airflow 파이프라인 구현
- 인구 범위 및 필수 필드 기반 데이터 품질 게이트 구현
- Staging Header와 Population Snapshot의 외래키 관계 구현
- `(area_code, observed_at)` 기준 Staging UPSERT 구현
- `dim_date`, `dim_hour`, `dim_location` 차원 테이블 구현
- `fact_city_snapshot`, `fact_weather_snapshot` Fact 적재
- `(location_id, observed_at)` 기준 DW Fact 중복 방지 구현
- 서울 시간 기준 날짜·시간 차원 키 생성
- 동일 Staging 입력 재처리 시 DW Fact 1행 유지 검증
- Airflow 재시도 정책 검증
- Airflow Scheduler 기반 예약 실행 검증
- 설정 테이블 기반 8개 지역 Dynamic Task Mapping 구현
- 지역·시간대별 인구·혼잡·날씨·대기질 Mart 조회 구현

### 다음 작업

- 8개 지역별 API 응답 구조 및 데이터 품질 차이 분석
- `location_source_map` 컬럼명과 실제 의미 정리
- 서울시 현재 날씨와 24시간 예보 데이터 그레인 분리
- Open-Meteo 추가 수집 목적 및 Fact 설계 결정
- 지하철 승하차·생활인구 데이터 소스 추가
- 데이터 품질 검증 SQL의 자동화
- 지역별 실패 격리 및 재처리 전략 고도화

## 수집 대상 설정

수집 대상은 DAG 코드에 하드코딩하지 않고
`staging.location_source_map`에서 관리합니다.

Airflow는 현재 활성 상태이며 유효 기간 안에 있는 지역을 조회한 뒤,
각 지역에 대해 Dynamic Task Mapping으로 독립적인 ETL TaskGroup을 생성합니다.

### 요청 값과 응답 값의 역할

| 구분 | 출처 | 역할 |
|---|---|---|
| `request_area_name` | `location_source_map.source_area_name` | 서울시 API 요청 파라미터 |
| `AREA_NM` | 서울시 API 응답 | 실제 API 반환 장소명 |
| `AREA_CD` | 서울시 API 응답 | Staging·DW 장소 식별 키 |
| `analysis_area_code` | `location_source_map` | 분석 생활권 분류 키 |
| `location_id` | `warehouse.dim_location` | DW Fact 조인용 대체 키 |

예를 들어 용산역의 경우 API 요청에는 `용산역`을 사용하고,
API 응답에서 `AREA_CD = POI046`을 받아 Staging과 DW의 장소 관계를 구성합니다.

### 현재 활성 수집 대상

| API 요청 장소명 | 분석 생활권 | 지역 유형 | 자치구 |
|---|---|---|---|
| 강남 MICE 관광특구 | 강남 생활권 | commercial | 강남구 |
| 광화문·덕수궁 | 광화문 생활권 | event | 중구 |
| 명동 관광특구 | 명동 생활권 | commercial | 중구 |
| 성수카페거리 | 성수 생활권 | commercial | 성동구 |
| 여의도 | 여의도 생활권 | business | 영등포구 |
| 용산역 | 용산 생활권 | business | 용산구 |
| 잠실 관광특구 | 잠실 생활권 | commercial | 송파구 |
| 홍대 관광특구 | 홍대 생활권 | event | 마포구 |

## 데이터 아키텍처

```text
staging.location_source_map
- 활성 수집 대상
- API 요청용 공식 장소명
- 분석용 생활권 속성
        ↓
get_active_location_requests
        ↓
extract_request_area_names
        ↓
Airflow Dynamic Task Mapping
        ↓
location_etl_pipeline[0..7]
        ↓
서울시 실시간 도시데이터 API
        ↓ XML
raw.api_response
- 요청 메타데이터
- XML 원문
- JSONB 변환 결과
        ↓
validate_raw_payload
- API 성공 응답 확인
- 필수 필드 확인
- population_min <= population_max 검증
        ↓
staging.realtime_city_header
staging.realtime_population_snapshot
staging.realtime_weather_snapshot
        ↓
warehouse.dim_date
warehouse.dim_hour
warehouse.dim_location
        ↓
warehouse.fact_city_snapshot
warehouse.fact_weather_snapshot
        ↓
mart.city_population_weather_hourly
```

## 지역 확장 방식

초기에는 광화문·덕수궁, 용산역, 여의도 3개 지역을
DAG 코드에서 각각 선언해 처리했습니다.

8개 지역 확장에서는 `staging.location_source_map`의 활성 지역 목록을
Airflow TaskFlow API로 조회하고, Dynamic Task Mapping을 사용해
지역별 TaskGroup을 런타임에 생성하도록 변경했습니다.

```text
get_active_location_requests
        ↓
extract_request_area_names
        ↓
location_etl_pipeline.expand(area_name=request_area_names)
```

이 구조로 수집 대상 추가·비활성화 시 DAG Python 코드를 수정하지 않고,
설정 테이블의 `is_active`와 유효 기간을 변경하는 방식으로 운영할 수 있습니다.

## 운영 정책

### 예약 실행

Airflow Scheduler는 UTC 기준 cron으로 DAG를 예약 실행합니다.

```text
30 21,23,1,3,5,7,9,11,13 * * *
```

위 표현식은 KST 기준 아래 시간에 실행됩니다.

```text
06:30, 08:30, 10:30, 12:30, 14:30,
16:30, 18:30, 20:30, 22:30
```

### 재시도 및 동시 실행 제어

- `retries=2`
- `retry_delay=3 minutes`
- `retry_exponential_backoff=True`
- `max_retry_delay=15 minutes`
- `catchup=False`
- `max_active_runs=1`

`catchup=False`는 실시간 API의 과거 시점 데이터를
자동으로 재호출하지 않도록 설정한 것입니다.
`max_active_runs=1`은 이전 수집이 끝나기 전에 다음 배치가
겹쳐 실행되는 것을 방지합니다.

## 검증 결과

| 검증 항목 | 방법 | 결과 |
|---|---|---|
| 정상 적재 | 실제 XML API 수집 후 Raw → Staging 적재 | 통과 |
| 품질 게이트 | `population_min > population_max` 오류 데이터 주입 | Validate 실패 및 하위 적재 차단 |
| Staging 중복 방지 | 동일 `(area_code, observed_at)` 재적재 | UPSERT로 1행 유지 |
| DW 중복 방지 | 동일 `(location_id, observed_at)` 재적재 | Fact 1행 유지 |
| 시간대 정규화 | UTC 저장 시각과 KST date/hour key 비교 | 일치 |
| 참조 무결성 | Fact의 Location·Date·Hour 차원 조인 검사 | 누락 0건 |
| Staging-DW 계보 | 관측 시각·인구 범위·혼잡도 비교 | 일치 |
| 재시도 | 의도적 실패 2회 후 3차 시도 성공 | 통과 |
| 예약 실행 | 15분 주기 검증 후 운영 cron 적용 | Scheduled DagRun 성공 |
| 8개 지역 확장 | 설정 테이블 기반 Dynamic Task Mapping | Scheduled DagRun 성공 |

### 8개 지역 확장

- 들어가야 할 내용 주제
    - 설정 테이블의 활성 지역 8개를 읽어 Dynamic Task Mapping으로 지역별 TaskGroup을 생성했다.
    - Scheduled DagRun에서 8개 지역의 Raw, Staging, DW Fact 적재가 성공했다.