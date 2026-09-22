# Architecture Decision Record

## 1. Raw 원본 보존

서울시 API는 XML 응답을 제공한다. Raw 계층에는 XML 원문과
XML-to-JSON 변환 결과를 함께 저장한다.

이 결정으로 파싱 로직 변경, 검증 규칙 변경, Staging 재구축이 필요할 때
외부 API를 다시 호출하지 않고 원본으로부터 재처리할 수 있다.

## 2. 요청 식별자와 응답 식별자 분리

API 요청에는 서울시가 허용한 공식 장소명 문자열을 사용한다.
응답의 `AREA_NM`, `AREA_CD`는 호출 이후 API가 반환하는 데이터 속성이다.

- 요청: `source_area_name → request_area_name`
- 응답: `AREA_NM`, `AREA_CD`
- DW 조인: `AREA_CD → dim_location.location_id`
- 분석 분류: `analysis_area_code`, `analysis_area_name`

이 구조로 API 계약, 원본 데이터 계보, 분석용 지역 분류를 분리했다.

## 3. 증분 적재와 멱등성

- Raw는 호출 이력 보존을 위해 매 요청마다 INSERT한다.
- Staging은 `(area_code, observed_at)` 업무 키 기준 UPSERT한다.
- Warehouse Fact는 `(location_id, observed_at)` 업무 키 기준 UPSERT한다.
- 동일 `ingestion_id` 재처리 시 Fact가 1행으로 유지되는 것을 검증했다.

## 4. 시간대 정책

- API의 실제 관측 시각은 `Asia/Seoul` 기준으로 해석한다.
- 데이터베이스에는 UTC `timestamptz`로 저장한다.
- `date_key`, `hour_key`는 `observed_at AT TIME ZONE 'Asia/Seoul'` 기준으로 생성한다.

## 5. 설정 기반 지역 확장

수집 대상 지역은 `staging.location_source_map`에서 관리한다.
Airflow는 활성화 상태와 유효 기간을 기준으로 수집 대상을 읽고,
Dynamic Task Mapping으로 지역별 TaskGroup을 생성한다.

새 지역을 추가하거나 비활성화할 때 DAG 코드 대신 설정 데이터를 수정한다.

## 분석 SQL 예시(주요 검증 SQL 쿼리 2~4개 정도를 최종적으로 선별할 것)

### 지역·시간대별 평균 추정 인구

```sql
SELECT
    l.area_name,
    d.full_date,
    h.hour_label,
    ROUND(
        AVG((f.population_min + f.population_max) / 2.0),
        1
    ) AS avg_estimated_population,
    MAX(f.population_max) AS peak_population_max
FROM warehouse.fact_city_snapshot AS f
JOIN warehouse.dim_location AS l
  ON l.location_id = f.location_id
JOIN warehouse.dim_date AS d
  ON d.date_key = f.date_key
JOIN warehouse.dim_hour AS h
  ON h.hour_key = f.hour_key
GROUP BY
    l.area_name,
    d.full_date,
    h.hour_key,
    h.hour_label
ORDER BY
    d.full_date,
    h.hour_key,
    l.area_name;
```

### 동일 관측 이벤트 중복 확인

```sql
SELECT
    location_id,
    observed_at,
    COUNT(*) AS row_count
FROM warehouse.fact_city_snapshot
GROUP BY location_id, observed_at
HAVING COUNT(*) > 1;
```

기대 결과는 0행이다.