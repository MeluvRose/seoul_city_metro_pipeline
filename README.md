## Milestone 1 — Raw to Staging ETL

서울시 실시간 도시데이터 API의 XML 응답을 Airflow로 수집하고,
원문 XML과 XML-to-JSON 변환 결과를 PostgreSQL Raw 계층에 함께 보존했습니다.

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

### 예약 실행 정책

- 검증 스케줄: `*/15 * * * *` (UTC 기준, 1시간 동안 자동 실행 확인)
- 운영 스케줄: `0 0,6,12,18 * * *` (UTC 기준)
- 운영 시간: 09:00, 15:00, 21:00, 03:00 KST
- `catchup=False`: 과거 실행 구간의 실시간 API 수집을 방지
- `max_active_runs=1`: 이전 실행이 끝나기 전 다음 실행이 겹치지 않도록 제한

### Airflow 예약 실행 검증

실제 서울시 실시간 인구 파이프라인에 검증용 cron
`*/15 * * * *`를 적용하고, 약 1시간 동안 예약 실행을 관찰했다.

- DAG: `seoul_realtime_population_pipeline`
- 검증 스케줄: 15분 간격
- 확인된 Scheduled DagRun: 4회
- 각 DagRun 상태: `success`
- Run Type: 모두 `scheduled`
- 결론: 수동 Trigger 없이 Airflow Scheduler가 설정된 cron에 따라
  DAG를 주기적으로 생성하고 실행함을 확인했다.

검증 완료 후 운영 스케줄을
`0 0,6,12,18 * * *`로 변경한다.

- UTC: 00:00, 06:00, 12:00, 18:00
- KST: 09:00, 15:00, 21:00, 다음 날 03:00
- `catchup=False`로 과거 실시간 수집 구간의 자동 재실행을 방지한다.