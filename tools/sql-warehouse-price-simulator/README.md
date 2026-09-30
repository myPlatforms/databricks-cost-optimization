# SQL Warehouse 가격 시뮬레이터 (AWS 서울 · Azure Korea Central)

Classic / Pro / Serverless SQL Warehouse 비용을 **Databricks(DBU)** 와 **클라우드 인프라(VM + 디스크)** 로 나눠 비교하는 Databricks 노트북입니다.

> **참고 툴입니다.** 비용 구조를 이해하고 규모를 가늠하는 용도이며, 견적이나 청구 금액을 보장하지 않습니다.
> 모든 금액은 공개 list price 기준(2026-10-01 점검)이고, 계약 단가·약정 할인은 반영하지 않습니다.
> 실제 계약 조건에 맞춘 비용은 Databricks 담당자와 확인하세요.

> "Classic이 무조건 싸다"는 DBU 단가만 비교했을 때 생기는 착시입니다. Classic / Pro는 VM·디스크가 클라우드에 따로 청구되고, Serverless는 인프라 비용이 DBU에 포함됩니다.

## 파일

| 파일 | 대상 | 설명 |
|---|---|---|
| `sql_warehouse_price_simulator.py` | **AWS 서울 · Azure Korea Central** | 통합 버전. 워크스페이스 주소로 클라우드를 자동 판별 (`cloud` 위젯으로 지정 가능) |
| `sql_warehouse_price_simulator_AWS.py` | AWS 서울 | 기존 AWS 전용 버전 (계산 결과는 통합 버전의 AWS와 동일) |

## 구성

| 섹션 | 내용 |
|---|---|
| 1. 단가 | DBU는 `system.billing.list_prices`, VM·디스크는 클라우드 공개 가격 API에서 실시간 조회. 실패하면 내장 점검값(2026-10-01)으로 대체하고 표에 출처를 표시 |
| 2. 시뮬레이션 | 위젯의 사이즈·사용 시간 기준 월 비용 비교(표 + 차트), 전체 사이즈(2X-Small ~ 4X-Large) 비교표 |
| 3. 실측 비교 | 현재 워크스페이스 웨어하우스의 최근 N일 DBU 사용량 → 실제 비용 추정, Serverless / Pro로 바꿨을 때 비용 |

## 가져오기

1. 노트북 파일 다운로드
2. 워크스페이스 **Workspace → Import → File** 로 업로드 (Databricks 노트북 소스 형식)

   또는 CLI:
   ```bash
   databricks workspace import /Users/<me>/sql_warehouse_price_simulator \
     --file sql_warehouse_price_simulator.py --format SOURCE --language PYTHON
   ```
3. Serverless(환경 버전 6) 또는 클래식 클러스터에서 실행

## 필요 조건

| 항목 | 없을 때 |
|---|---|
| `system.billing.list_prices` 읽기 권한 | DBU 단가를 내장 점검값으로 대체 |
| `system.billing.usage` 읽기 권한 | 3. 실측 비교가 비어 있음 |
| 인터넷 아웃바운드 — AWS: `b0.p.awsstatic.com`, `website.spot.ec2.aws.a2z.com` / Azure: `prices.azure.com` | VM·디스크 단가를 내장 점검값으로 대체 (폐쇄망은 대부분 이 경우) |

## 위젯

| 위젯 | 기본값 | 설명 |
|---|---|---|
| 클라우드 | AUTO | 워크스페이스 주소로 판별. 다른 클라우드를 지정하면 시뮬레이션만 하고 실측 비교는 건너뜀 (DBU 단가는 내장 점검값) |
| Edition | PREMIUM | AWS는 Premium·Enterprise(SQL SKU 동일 단가), Azure는 Premium만 — Azure에서 Enterprise를 고르면 Premium으로 계산 |
| 사이즈 | Small | 시뮬레이션 대상 사이즈 |
| 하루 사용 시간 / 월 사용 일수 / 평균 클러스터 수 | 9 / 22 / 1 | 월 클러스터-시간 = 곱 |
| 단가 소스 | AUTO | `FALLBACK`이면 조회 없이 내장 점검값만 사용 |
| 실측 조회 기간 | 30 | 일 단위, 월 환산은 `× 30 / 기간` |
| Serverless→Pro 전환 시 스팟 정책 | COST_OPTIMIZED | Serverless 웨어하우스를 Pro로 바꿨을 때의 가정 (AWS만 해당) |

## 계산 기준과 출처

| 항목 | AWS 서울 | Azure Korea Central |
|---|---|---|
| 사이즈별 DBU/h | 2XS 4 · XS 6 · S 12 · M 24 · L 40 · XL 80 · 2XL 144 · 3XL 272 · 4XL 528, Classic/Pro에도 동일 적용 ([SQL Serverless SKU](https://learn.microsoft.com/en-us/azure/databricks/resources/pricing#sql-serverless-sku)) | 동일 |
| DBU 단가 (2026-10-01) | Classic $0.22 · Pro $0.74 · Serverless $0.95 | 동일 |
| Edition | Premium · Enterprise | Premium만 |
| 클러스터 구성 | 드라이버 i3.2xlarge~i3.16xlarge(사이즈별) + 워커 i3.2xlarge ([docs](https://docs.databricks.com/aws/en/compute/sql-warehouse/warehouse-behavior#classic-and-pro-sql-warehouses)) | 드라이버 Standard_E8ds_v4~E64ds_v4(사이즈별) + 워커 Standard_E8ds_v4 ([docs](https://learn.microsoft.com/en-us/azure/databricks/compute/sql-warehouse/warehouse-behavior#classic-and-pro-sql-warehouses)) |
| 디스크 | 노드당 EBS 30GB + 150GB, gp3 기본 성능이라 IOPS·처리량 추가 과금 없음 ([docs](https://docs.databricks.com/aws/en/compute/configure#default-ebs-volumes)) | 노드당 256GB Premium SSD LRS (P15), 시간당 과금 ([docs](https://learn.microsoft.com/en-us/azure/databricks/compute/sql-warehouse/warehouse-behavior#classic-and-pro-sql-warehouses)) |
| 스팟 ([SDK `SpotInstancePolicy`](https://databricks-sdk-py.readthedocs.io/en/stable/dbdataclasses/sql.html)) | Cost optimized(기본값) = 드라이버 온디맨드 + 워커 스팟 | **정책과 관계없이 전부 온디맨드** |
| 인프라 단가 | [EC2 On-Demand](https://aws.amazon.com/ec2/pricing/on-demand/) · [Spot](https://aws.amazon.com/ec2/spot/pricing/) · [EBS](https://aws.amazon.com/ebs/pricing/) | [Azure Retail Prices API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices) (Linux 종량제, P15 LRS) |

1시간 · 클러스터 1개 기준 예시 (USD):

| 사이즈 | AWS Pro 온디맨드 | AWS Pro 스팟 | Azure Pro | Serverless (양쪽 동일) |
|---|---|---|---|---|
| 2X-Small | 4.47 | 3.96 | 4.45 | 3.80 |
| Small | 13.38 | 11.35 | 13.29 | 11.40 |
| Medium | 26.75 | 22.67 | 26.53 | 22.80 |

Azure는 스팟이 없어서 Pro가 항상 Serverless보다 약 17% 높습니다.

## 한계

- **AWS 서울 · Azure Korea Central 전용**입니다. 다른 리전은 노트북 상단 `PROFILES`의 리전 값과 `fallback` 단가를 바꿔야 합니다.
- DBU 단가는 **리전 전용 SKU**를 찾고, 리전 구분이 없는 SKU만 있을 때 그것을 씁니다. 다른 리전 단가로는 대체하지 않으며, 찾지 못하면 내장 점검값을 씁니다.
- 워크스페이스의 청구 SKU 리전이 시뮬레이터 리전과 다르면(예: Azure US West 2 워크스페이스), 실측 비교에서 **Databricks 실제 비용만** 표시하고 인프라 추정·전환 비용은 비워 둡니다. AWS 전용 버전(`_AWS`)에는 이 검사가 없으므로 서울 리전 워크스페이스에서만 쓰세요.
- 실측 비교의 인프라 비용은 **추정치**입니다. 가동 클러스터-시간을 `DBU ÷ 사이즈별 DBU/h`로 역산하므로, 조회 기간 중 사이즈나 유형을 바꾸면 오차가 생깁니다 (유형 변경은 경고로 표시).
- 전환 비용은 같은 클러스터-시간을 쓴다고 가정합니다. Serverless는 기동이 빠르고 auto-stop을 짧게 잡을 수 있어 실제 가동 시간이 줄어드는 경우가 많습니다.
- 모든 금액은 **list price** 기준입니다. 미반영: NAT·데이터 전송, 오브젝트 스토리지, 클러스터 기동 소요 시간, RI/Savings Plan·Databricks 약정 할인, 부가세.
- AWS 스팟 단가는 수시로 바뀌며, 스팟 용량이 부족하면 실제 비용은 온디맨드 쪽으로 올라갈 수 있습니다.
