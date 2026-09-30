# Databricks 비용 최적화

Databricks 공식 비용 최적화 문서의 한국어 요약과 참고 툴을 모아 둔 페이지입니다. GitHub Pages로 서빙됩니다.

**주소**: https://myplatforms.github.io/databricks-cost-optimization/

> 상세 가이드는 개선 중입니다. 준비가 끝나면 같은 주소에서 교체해 공개합니다.

## 내용

- **공식 문서 요약** (`index.html`): 비용 최적화 4원칙과 원칙별 실천 방법 — 비공식 한국어 요약이며, 정확한 내용은 원문을 확인하세요
  - [Cost optimization for Databricks](https://docs.databricks.com/aws/en/lakehouse-architecture/cost-optimization/)
  - [Best practices for cost optimization](https://docs.databricks.com/aws/en/lakehouse-architecture/cost-optimization/best-practices)

## 참고 툴

페이지 오른쪽 위 **참고 툴** 버튼과 목차의 "참고 툴" 그룹에서도 열 수 있습니다.

| 툴 | 설명 | 형식 | 대상 |
|---|---|---|---|
| [SQL Warehouse 가격 시뮬레이터](tools/sql-warehouse-price-simulator/) | Classic / Pro / Serverless 비용을 Databricks(DBU)와 클라우드 인프라(VM + 디스크)로 나눠 비교. 통합 버전 + AWS 전용 버전(`_AWS`) | Databricks 노트북 | AWS 서울 · Azure Korea Central |

새 툴은 `tools/<툴 이름>/` 폴더에 README와 함께 추가하고, 이 표와 페이지의 "참고 툴" 그룹에 등록합니다.

## 로컬 미리보기

```bash
open index.html
```

단일 정적 HTML(self-contained)이라 빌드 스텝이 없습니다. `main` 브랜치 루트에서 GitHub Pages가 직접 서빙합니다.
