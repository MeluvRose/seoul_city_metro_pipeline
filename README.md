## Milestone 1: Raw-to-Staging Pipeline

- Airflow TaskFlow API 기반 `extract → validate → load_staging` DAG 구현
- API 원본 응답을 PostgreSQL Raw 계층의 JSONB 컬럼에 보존
- Raw 식별자(`raw_id`)를 기준으로 검증 및 Staging 변환 수행
- `population_min <= population_max` 품질 규칙 구현
- 검증 실패 시 downstream Staging 적재 태스크가 실행되지 않도록 의존성 구성
- `(area_code, observed_at)` 유니크 제약조건과 UPSERT로 중복 적재 방지
- 정상·비정상 입력 데이터에 대해 성공 및 실패 경로 검증