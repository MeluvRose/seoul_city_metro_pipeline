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