# Data Dictionary

**(목표: 테이블의 목적, 그레인, 키, 중복 규칙을 기록합니다.)**

## Schema Overview

| Schema | Purpose |
|---|---|
| `raw` | API 요청·응답 원본 및 호출 이력 보관 |
| `staging` | 소스 응답 구조화, 데이터 품질 검증 후 표준화 |
| `warehouse` | 분석용 차원 모델 및 원자 Fact 저장 |
| `mart` | 분석 질의에 최적화된 집계·결합 데이터 |

## raw.api_response

| 항목 | 설명 |
|---|---|
| 목적 | 서울시 API 호출 단위의 원문 XML 및 JSONB 변환 결과 보관 |
| 그레인 | API 요청·응답 1건 |
| 기본키 | `raw_id` |
| 주요 시간 | 요청·수집 시각, 응답 수신 시각 |
| 주요 데이터 | 요청 대상, XML 원문, JSONB payload, 성공 여부 |
| 중복 정책 | 호출 이력 보존을 위해 매 요청마다 INSERT |
| 사용 목적 | 재처리, 오류 분석, 원본 감사, 응답 구조 변경 대응 |

## staging.location_source_map

| 컬럼 | 의미 | 사용 위치 |
|---|---|---|
| `source_name` | 데이터 소스 식별자 | 활성 설정 조회 |
| `source_area_name` | 서울시 API 요청용 공식 장소명 | `request_area_name` |
| `analysis_area_code` | 프로젝트 분석용 생활권 코드 | Mart 분석·그룹화 |
| `analysis_area_name` | 분석용 생활권 이름 | 대시보드·결과 표시 |
| `area_type` | 생활권 분류 | 상업·업무·행사 지역 비교 |
| `district_name` | 자치구 | 지역 분석 |
| `is_active` | 수집 활성화 여부 | Airflow 대상 제어 |
| `valid_from` | 설정 유효 시작일 | 대상 조회 조건 |
| `valid_to` | 설정 유효 종료일 | 대상 조회 조건 |

> 주의: 현재 `source_area_code` 컬럼에는 코드가 아닌 API URL 형태의 값이 저장돼 있다.
> 해당 컬럼은 API 응답의 `AREA_CD`와 같은 의미로 사용하지 않으며,
> 향후 `request_url_template` 또는 적절한 이름으로 리팩터링할 예정이다.

## staging.realtime_city_header

| 항목 | 설명 |
|---|---|
| 목적 | API 응답의 장소·관측 시각·수집 메타데이터 저장 |
| 그레인 | 장소별 실제 관측 시각의 도시데이터 Header 1건 |
| 기본키 | `ingestion_id` |
| 원본 참조 | `raw.api_response.raw_id` |
| 주요 컬럼 | `area_code`, `area_name`, `observed_at`, `collected_at`, `is_success` |
| 역할 | Population·Weather Snapshot의 부모 테이블 |

## staging.realtime_population_snapshot

| 항목 | 설명 |
|---|---|
| 목적 | 실시간 인구·혼잡도 데이터 표준화 |
| 그레인 | API 응답 장소별 실제 관측 시각의 인구 Snapshot 1건 |
| 기본키 | `population_snapshot_id` |
| 외래키 | `ingestion_id → realtime_city_header.ingestion_id` |
| 업무 키 | `(area_code, observed_at)` |
| 중복 정책 | 동일 업무 키 충돌 시 UPSERT |
| 품질 규칙 | `population_min <= population_max` |
| 주요 컬럼 | 혼잡도, 최소·최대 인구, 성별 비율, 상주·비상주 비율 |

## staging.realtime_weather_snapshot

| 항목 | 설명 |
|---|---|
| 목적 | 서울시 API 응답의 현재 날씨·대기질 데이터 표준화 |
| 그레인 | 장소별 실제 관측 시각의 날씨·대기질 Snapshot 1건 |
| 주요 값 | 기온, 습도, 풍속, PM10, PM2.5, 강수 형태, 대기질 등급 |
| 주의 | 24시간 예보 배열은 현재 관측값과 그레인이 다르므로 별도 Fact 설계 대상으로 관리 |

## staging 테이블 추가 예정...

## warehouse.dim_date

| 항목 | 설명 |
|---|---|
| 그레인 | 달력 날짜 1일 |
| 기본키 | `date_key` (`YYYYMMDD`) |
| 주요 속성 | 연도, 분기, 월, 일, 요일, 주말 여부 |

## warehouse.dim_hour

| 항목 | 설명 |
|---|---|
| 그레인 | 하루 중 시간대 1개 |
| 기본키 | `hour_key` (`0~23`) |
| 주요 속성 | 시간 시작값, 표시용 시간 라벨 |

## warehouse.dim_location

| 항목 | 설명 |
|---|---|
| 그레인 | API 응답의 장소 코드 기준 분석 위치 1개 |
| 기본키 | `location_id` |
| 업무 키 | API 응답 `area_code` |
| 주요 속성 | API 응답 장소명, 활성 상태, 향후 좌표·생활권 속성 |
| 역할 | 도시·날씨 Fact의 공통 Location Dimension |

## warehouse.fact_city_snapshot

| 항목 | 설명 |
|---|---|
| 목적 | 지역별 실시간 인구·혼잡도 분석 |
| 그레인 | `location_id`와 실제 `observed_at` 기준 도시 Snapshot 1건 |
| 기본키 | `city_snapshot_id` |
| 외래키 | `location_id`, `date_key`, `hour_key` |
| 업무 키 | `(location_id, observed_at)` |
| 중복 정책 | 동일 업무 키 충돌 시 UPSERT |
| 주요 측정값 | `population_min`, `population_max`, `congestion_level` |
| 계보 | `source_ingestion_id`로 Staging·Raw 추적 |

## warehouse.fact_weather_snapshot

| 항목 | 설명 |
|---|---|
| 목적 | 지역별 현재 날씨·대기질 분석 |
| 그레인 | `location_id`와 실제 `observed_at` 기준 날씨 Snapshot 1건 |
| 기본키 | `weather_snapshot_id` |
| 외래키 | `location_id`, `date_key`, `hour_key` |
| 주요 측정값 | 기온, 습도, 풍속, PM10, PM2.5, 강수 형태, 대기질 등급 |
| 계보 | `source_ingestion_id`로 Staging·Raw 추적 |

(warehouse 테이블 추가 예정...)

(또한, Mart(DM) 테이블 또는 뷰도 이름과 속성에 맞게 내용 수정 필요)

## mart.city_population_weather_hourly

| 항목 | 설명 |
|---|---|
| 목적 | 지역·시간대별 인구, 혼잡도, 날씨·대기질 통합 분석 |
| 그레인 | 지역별 서울 현지 시간 1시간 단위 |
| 주요 지표 | 평균 추정 인구, 최대 인구, 혼잡도 분포, 기온, 습도, 풍속, PM10, PM2.5 |
| 결합 방식 | 도시 Snapshot을 시간 단위로 집계한 뒤, 동일 지역·시간 기준 Weather Fact와 결합 |