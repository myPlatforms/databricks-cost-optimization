# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SQL Warehouse 가격 시뮬레이터 (AWS 서울)
# MAGIC
# MAGIC Classic / Pro / Serverless SQL Warehouse 비용을 **Databricks(DBU)** 와 **AWS(EC2 + EBS)** 로 나눠 계산합니다.
# MAGIC
# MAGIC | 섹션 | 내용 |
# MAGIC |---|---|
# MAGIC | 1. 단가 | DBU는 `system.billing.list_prices`, EC2·EBS는 AWS 공개 가격 피드에서 조회 (실패 시 2026-10-01 점검값 사용) |
# MAGIC | 2. 시뮬레이션 | 위젯에서 고른 사이즈·사용 시간 기준 월 비용 비교 + 전체 사이즈 비교표 |
# MAGIC | 3. 실측 비교 | 이 워크스페이스 웨어하우스의 실제 DBU 사용량 → 실제 비용 추정 · 유형 전환 시 비용 |
# MAGIC
# MAGIC **계산 기준**
# MAGIC - Classic / Pro는 DBU와 AWS 비용이 각각 청구되고, Serverless는 인프라 비용이 DBU에 포함됩니다.
# MAGIC - 사이즈별 DBU/h: [SQL Serverless SKU](https://learn.microsoft.com/en-us/azure/databricks/resources/pricing#sql-serverless-sku) — Classic/Pro에도 동일 적용
# MAGIC - 클러스터 구성: [warehouse-behavior](https://docs.databricks.com/aws/en/compute/sql-warehouse/warehouse-behavior#classic-and-pro-sql-warehouses) — 드라이버는 사이즈별, 워커는 모두 i3.2xlarge
# MAGIC - 스팟: Cost optimized(기본값) = 드라이버 온디맨드 + 워커 스팟 ([SDK SpotInstancePolicy](https://databricks-sdk-py.readthedocs.io/en/stable/dbdataclasses/sql.html))
# MAGIC - EBS: 노드당 30GB + 150GB ([Default EBS volumes](https://docs.databricks.com/aws/en/compute/configure#default-ebs-volumes)), gp3 기본 성능(3,000 IOPS / 125 MB/s)이라 IOPS·처리량 추가 과금 없음
# MAGIC - 미반영: NAT·데이터 전송, S3, 클러스터 기동 소요 시간, RI/Savings Plan·약정 할인, 부가세

# COMMAND ----------

import gzip
import json
import urllib.parse
import urllib.request

import pandas as pd

# AWS Seoul only: SKU suffix / AWS price-feed location / region code
REGION_SKU = "AP_SEOUL"
AWS_LOCATION = "Asia Pacific (Seoul)"
AWS_REGION = "ap-northeast-2"

# size -> (DBU per hour per cluster, driver instance, worker count); workers are i3.2xlarge
# DBU/h: https://learn.microsoft.com/en-us/azure/databricks/resources/pricing#sql-serverless-sku (same for Classic/Pro)
# Instances: https://docs.databricks.com/aws/en/compute/sql-warehouse/warehouse-behavior#classic-and-pro-sql-warehouses
SIZES = {
    "2X-Small": (4, "i3.2xlarge", 1),
    "X-Small": (6, "i3.2xlarge", 2),
    "Small": (12, "i3.4xlarge", 4),
    "Medium": (24, "i3.8xlarge", 8),
    "Large": (40, "i3.8xlarge", 16),
    "X-Large": (80, "i3.16xlarge", 32),
    "2X-Large": (144, "i3.16xlarge", 64),
    "3X-Large": (272, "i3.16xlarge", 128),
    "4X-Large": (528, "i3.16xlarge", 256),
}
WORKER = "i3.2xlarge"
EBS_GB_PER_NODE = 30 + 150
HOURS_PER_MONTH = 730

# Fallback prices, checked 2026-10-01 (Seoul; SQL SKUs are identical for Premium and Enterprise)
FALLBACK_DATE = "2026-10-01"
FALLBACK = {
    "dbu": {"CLASSIC": 0.22, "PRO": 0.74, "SERVERLESS": 0.95},
    "od": {"i3.2xlarge": 0.732, "i3.4xlarge": 1.464, "i3.8xlarge": 2.928, "i3.16xlarge": 5.856},
    "spot": {"i3.2xlarge": 0.2228},
    "gp3": 0.0912,
}

# COMMAND ----------

dbutils.widgets.dropdown("edition", "PREMIUM", ["PREMIUM", "ENTERPRISE"], "01. Edition")
dbutils.widgets.dropdown("size", "Small", list(SIZES), "02. 사이즈")
dbutils.widgets.text("hours_per_day", "9", "03. 하루 사용 시간(h)")
dbutils.widgets.text("days_per_month", "22", "04. 월 사용 일수")
dbutils.widgets.text("clusters", "1", "05. 평균 클러스터 수")
dbutils.widgets.dropdown("price_source", "AUTO", ["AUTO", "FALLBACK"], "06. 단가 소스")
dbutils.widgets.text("lookback_days", "30", "07. 실측 조회 기간(일)")
dbutils.widgets.dropdown("spot_policy_default", "COST_OPTIMIZED", ["COST_OPTIMIZED", "RELIABILITY_OPTIMIZED"], "08. 실측: Serverless→Pro 전환 시 스팟 정책")

EDITION = dbutils.widgets.get("edition")
SIZE = dbutils.widgets.get("size")
HOURS_PER_DAY = float(dbutils.widgets.get("hours_per_day"))
DAYS_PER_MONTH = float(dbutils.widgets.get("days_per_month"))
CLUSTERS = float(dbutils.widgets.get("clusters"))
PRICE_SOURCE = dbutils.widgets.get("price_source")
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
SPOT_POLICY_DEFAULT = dbutils.widgets.get("spot_policy_default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 단가

# COMMAND ----------


def fetch(url, timeout=10):
    raw = urllib.request.urlopen(url, timeout=timeout).read()
    return gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw


def load_dbu_prices(edition):
    skus = {
        "CLASSIC": f"{edition}_SQL_COMPUTE",
        "PRO": f"{edition}_SQL_PRO_COMPUTE_{REGION_SKU}",
        "SERVERLESS": f"{edition}_SERVERLESS_SQL_COMPUTE_{REGION_SKU}",
    }
    rows = spark.sql(
        """
        SELECT sku_name, CAST(pricing.default AS DOUBLE) AS usd
        FROM system.billing.list_prices
        WHERE price_end_time IS NULL AND currency_code = 'USD' AND cloud = 'AWS'
          AND sku_name IN (:classic, :pro, :serverless)
        """,
        args={"classic": skus["CLASSIC"], "pro": skus["PRO"], "serverless": skus["SERVERLESS"]},
    ).collect()
    found = {r.sku_name: r.usd for r in rows}
    missing = [s for s in skus.values() if s not in found]
    if missing:
        raise LookupError(f"list_prices에 없음: {missing}")
    return {k: found[s] for k, s in skus.items()}, skus


def load_aws_prices():
    loc = urllib.parse.quote(AWS_LOCATION)
    od_doc = json.loads(fetch(
        f"https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/{loc}/Linux/index.json"))
    od = {}
    for item in od_doc["regions"][AWS_LOCATION].values():
        it = item.get("Instance Type")
        if it in FALLBACK["od"] and it not in od:
            od[it] = float(item["price"])
    published = od_doc["manifest"]["hawkFilePublicationDate"][:10]

    spot_js = fetch("https://website.spot.ec2.aws.a2z.com/spot.js").decode()
    spot_doc = json.loads(spot_js[spot_js.find("(") + 1: spot_js.rfind(")")])
    spot = {}
    for reg in spot_doc["config"]["regions"]:
        if reg["region"] != AWS_REGION:
            continue
        for itype in reg["instanceTypes"]:
            for s in itype["sizes"]:
                if s["size"] == WORKER:
                    linux = next(v for v in s["valueColumns"] if v["name"] == "linux")
                    spot[WORKER] = float(linux["prices"]["USD"])

    ebs_doc = json.loads(fetch("https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ebs.json"))
    gp3 = float(ebs_doc["regions"][AWS_LOCATION]["Storage General Purpose gp3 GB Mo"]["price"])

    missing = [k for k in FALLBACK["od"] if k not in od] + ([] if WORKER in spot else [f"spot {WORKER}"])
    if missing:
        raise LookupError(f"AWS 가격 피드에 없음: {missing}")
    return {"od": od, "spot": spot, "gp3": gp3}, published


P = {k: (dict(v) if isinstance(v, dict) else v) for k, v in FALLBACK.items()}
SRC = {"dbu": f"fallback (점검 {FALLBACK_DATE})", "aws": f"fallback (점검 {FALLBACK_DATE})"}
SKU_NAMES = {}
if PRICE_SOURCE == "AUTO":
    try:
        P["dbu"], SKU_NAMES = load_dbu_prices(EDITION)
        SRC["dbu"] = "system.billing.list_prices (현재 활성 단가)"
    except Exception as e:
        print(f"[DBU] list_prices 조회 실패 → fallback 사용: {e}")
    try:
        aws, published = load_aws_prices()
        P.update(aws)
        SRC["aws"] = f"AWS 공개 가격 피드 (온디맨드·EBS 게시 {published}, 스팟은 조회 시점 값)"
    except Exception as e:
        print(f"[AWS] 가격 피드 조회 실패 (인터넷 차단 등) → fallback 사용: {type(e).__name__}: {e}")

price_rows = [
    ("Databricks", f"SQL Classic ({SKU_NAMES.get('CLASSIC', '-')})", P["dbu"]["CLASSIC"], "USD/DBU", SRC["dbu"]),
    ("Databricks", f"SQL Pro ({SKU_NAMES.get('PRO', '-')})", P["dbu"]["PRO"], "USD/DBU", SRC["dbu"]),
    ("Databricks", f"SQL Serverless ({SKU_NAMES.get('SERVERLESS', '-')})", P["dbu"]["SERVERLESS"], "USD/DBU", SRC["dbu"]),
    *[("AWS EC2 온디맨드", it, v, "USD/h", SRC["aws"]) for it, v in sorted(P["od"].items(), key=lambda x: x[1])],
    ("AWS EC2 스팟", WORKER, P["spot"][WORKER], "USD/h", SRC["aws"]),
    ("AWS EBS", "gp3", P["gp3"], "USD/GB-월", SRC["aws"]),
    ("AWS EBS", f"노드당 {EBS_GB_PER_NODE}GB 시간 비용", EBS_GB_PER_NODE * P["gp3"] / HOURS_PER_MONTH, "USD/h", "계산 (용량 × gp3 ÷ 730h)"),
]
display(pd.DataFrame(price_rows, columns=["구분", "항목", "단가", "단위", "출처"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 시뮬레이션

# COMMAND ----------


def aws_hourly(size, spot_policy):
    """EC2 and EBS cost per cluster-hour for a Classic/Pro warehouse."""
    _, driver, workers = SIZES[size]
    worker_price = P["spot"][WORKER] if spot_policy == "COST_OPTIMIZED" else P["od"][WORKER]
    ec2 = P["od"][driver] + workers * worker_price
    ebs = (workers + 1) * EBS_GB_PER_NODE * P["gp3"] / HOURS_PER_MONTH
    return ec2, ebs


OPTIONS = [  # label, warehouse type, spot policy
    ("Classic · 온디맨드", "CLASSIC", "RELIABILITY_OPTIMIZED"),
    ("Classic · 스팟", "CLASSIC", "COST_OPTIMIZED"),
    ("Pro · 온디맨드", "PRO", "RELIABILITY_OPTIMIZED"),
    ("Pro · 스팟", "PRO", "COST_OPTIMIZED"),
    ("Serverless", "SERVERLESS", None),
]


def cost(size, wh_type, spot_policy, cluster_hours):
    dbu = SIZES[size][0] * cluster_hours
    dbx = dbu * P["dbu"][wh_type]
    ec2, ebs = aws_hourly(size, spot_policy) if wh_type != "SERVERLESS" else (0.0, 0.0)
    return {"DBU": dbu, "Databricks": dbx, "AWS EC2": ec2 * cluster_hours, "AWS EBS": ebs * cluster_hours,
            "합계": dbx + (ec2 + ebs) * cluster_hours}


CLUSTER_HOURS = HOURS_PER_DAY * DAYS_PER_MONTH * CLUSTERS
_, drv, wk = SIZES[SIZE]
print(f"{SIZE}: {drv} 1대 + {WORKER} {wk}대 · {SIZES[SIZE][0]} DBU/h")
print(f"월 클러스터-시간 = {HOURS_PER_DAY:g}h × {DAYS_PER_MONTH:g}일 × {CLUSTERS:g}개 = {CLUSTER_HOURS:,.0f}h  ({EDITION}, USD)")

sim = pd.DataFrame([{"옵션": label, **cost(SIZE, t, sp, CLUSTER_HOURS)} for label, t, sp in OPTIONS])
sim["Serverless 대비"] = sim["합계"] / sim.loc[sim["옵션"] == "Serverless", "합계"].iloc[0] - 1
display(sim.round({"DBU": 0, "Databricks": 2, "AWS EC2": 2, "AWS EBS": 2, "합계": 2, "Serverless 대비": 3}))

# COMMAND ----------

import matplotlib.pyplot as plt

# English tick labels: cluster runtimes usually ship without a Hangul font
CHART_LABELS = {"Classic · 온디맨드": "Classic\nOn-demand", "Classic · 스팟": "Classic\nSpot",
                "Pro · 온디맨드": "Pro\nOn-demand", "Pro · 스팟": "Pro\nSpot", "Serverless": "Serverless"}
x = sim["옵션"].map(CHART_LABELS)
fig, ax = plt.subplots(figsize=(9, 4))
bottom = pd.Series([0.0] * len(sim))
for col, color in (("Databricks", "#FF3621"), ("AWS EC2", "#1B3139"), ("AWS EBS", "#98A2AA")):
    ax.bar(x, sim[col], bottom=bottom, label=col, color=color)
    bottom += sim[col]
for i, total in enumerate(sim["합계"]):
    ax.text(i, total, f"${total:,.0f}", ha="center", va="bottom", fontsize=9)
ax.set_ylabel("USD / month")
ax.set_title(f"{SIZE} · {CLUSTER_HOURS:,.0f} cluster-hours / month")
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 전체 사이즈 월 비용 (USD, 위젯의 사용 시간 기준)

# COMMAND ----------

matrix = pd.DataFrame(
    {label: {size: cost(size, t, sp, CLUSTER_HOURS)["합계"] for size in SIZES} for label, t, sp in OPTIONS}
)
matrix.insert(0, "DBU/h", [SIZES[s][0] for s in SIZES])
matrix.insert(1, "구성", [f"{SIZES[s][1]} + {WORKER}×{SIZES[s][2]}" for s in SIZES])
display(matrix.round(2).reset_index(names="사이즈"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 실측 비교 — 이 워크스페이스의 SQL Warehouse
# MAGIC
# MAGIC - DBU와 Databricks 비용은 `system.billing.usage` × 사용 시점의 `list_prices` (list price, 약정 할인 미반영)
# MAGIC - AWS 비용은 추정치입니다: 가동 클러스터-시간 = DBU ÷ 사이즈별 DBU/h → EC2 + EBS 단가 적용 (조회 기간 중 사이즈를 바꿨다면 오차 발생)
# MAGIC - 전환 비용은 같은 클러스터-시간을 쓴다고 가정합니다. Serverless는 기동이 빠르고 auto-stop을 짧게 잡을 수 있어 실제로는 가동 시간이 줄어드는 경우가 많습니다.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
WORKSPACE_ID = str(w.get_workspace_id())

meta = {}
for wh in w.warehouses.list():
    wh_type = "SERVERLESS" if wh.enable_serverless_compute else (wh.warehouse_type.value if wh.warehouse_type else "CLASSIC")
    meta[wh.id] = {
        "이름": wh.name,
        "유형": wh_type,
        "사이즈": wh.cluster_size,
        "스팟 정책": "-" if wh_type == "SERVERLESS" else (wh.spot_instance_policy.value if wh.spot_instance_policy else "COST_OPTIMIZED"),
        "클러스터(min~max)": f"{wh.min_num_clusters}~{wh.max_num_clusters}",
        "auto-stop(분)": wh.auto_stop_mins,
    }

usage = spark.sql(
    """
    SELECT u.usage_metadata.warehouse_id AS warehouse_id,
           u.sku_name,
           SUM(u.usage_quantity) AS dbu,
           SUM(u.usage_quantity * CAST(lp.pricing.default AS DOUBLE)) AS dbx_cost
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices lp
      ON lp.sku_name = u.sku_name AND lp.cloud = u.cloud AND lp.usage_unit = u.usage_unit
     AND lp.currency_code = 'USD'
     AND u.usage_start_time >= lp.price_start_time
     AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
    WHERE u.workspace_id = :ws
      AND u.usage_metadata.warehouse_id IS NOT NULL
      AND u.billing_origin_product = 'SQL'
      AND u.usage_date >= date_sub(current_date(), :days)
    GROUP BY ALL
    """,
    args={"ws": WORKSPACE_ID, "days": LOOKBACK_DAYS},
).toPandas()

print(f"워크스페이스 {WORKSPACE_ID} · 최근 {LOOKBACK_DAYS}일 · 웨어하우스 {len(meta)}개 · 사용 이력 {usage['warehouse_id'].nunique()}개")

# COMMAND ----------

MONTH_FACTOR = 30 / LOOKBACK_DAYS
rows = []
mismatches = []
for r in usage.itertuples():
    m = meta.get(r.warehouse_id, {"이름": "(삭제됨/조회 불가)", "유형": None, "사이즈": None, "스팟 정책": SPOT_POLICY_DEFAULT})
    actual_type = "SERVERLESS" if "SERVERLESS" in r.sku_name else ("PRO" if "SQL_PRO" in r.sku_name else "CLASSIC")
    meta_type = m.get("유형")
    if meta_type and actual_type != meta_type:
        mismatches.append(f"{m.get('이름', r.warehouse_id)}: 현재 {meta_type} ↔ 청구 {actual_type}")
    size = m["사이즈"]
    dbu = float(r.dbu)
    dbx = float(r.dbx_cost) if pd.notna(r.dbx_cost) else dbu * P["dbu"][actual_type]
    known_size = size in SIZES
    cluster_hours = dbu / SIZES[size][0] if known_size else None
    # Serverless rows (or a type change within the lookback) carry no usable spot policy
    spot_policy = m["스팟 정책"] if m["스팟 정책"] in ("COST_OPTIMIZED", "RELIABILITY_OPTIMIZED") else SPOT_POLICY_DEFAULT

    aws = 0.0
    if actual_type != "SERVERLESS" and known_size:
        ec2, ebs = aws_hourly(size, spot_policy)
        aws = (ec2 + ebs) * cluster_hours

    to_serverless = dbu * P["dbu"]["SERVERLESS"]
    if known_size:
        ec2, ebs = aws_hourly(size, spot_policy)
        to_pro = dbu * P["dbu"]["PRO"] + (ec2 + ebs) * cluster_hours
    else:
        to_pro = None

    rows.append({
        **{k: m.get(k) for k in ("이름", "사이즈", "스팟 정책", "클러스터(min~max)", "auto-stop(분)")},
        "청구 유형": actual_type,
        "SKU": r.sku_name,
        "DBU": dbu,
        "클러스터-시간": cluster_hours,
        "Databricks (실제)": dbx,
        "AWS (추정)": aws if actual_type != "SERVERLESS" else 0.0,
        "합계 (추정)": dbx + aws,
        "Serverless 전환 시": to_serverless,
        "Pro 전환 시": to_pro,
    })

if not rows:
    print("조회 기간에 SQL Warehouse 사용 이력이 없습니다. 'lookback_days'를 늘리거나 system.billing 권한을 확인하세요.")
else:
    actual = pd.DataFrame(rows).sort_values("합계 (추정)", ascending=False)
    print(f"조회 기간 합계 (USD). 월 환산은 × {MONTH_FACTOR:.2f}")
    display(actual.round(2))

    money = ["Databricks (실제)", "AWS (추정)", "합계 (추정)", "Serverless 전환 시", "Pro 전환 시"]
    monthly = actual[["이름", "청구 유형", "사이즈"] + money].copy()
    monthly[money] = monthly[money].astype(float) * MONTH_FACTOR
    print("월 환산 (USD)")
    display(monthly.round(2))
    if mismatches:
        print("⚠️ 조회 기간 중 웨어하우스 유형이 변경된 것으로 보입니다 (AWS 추정·전환 비용에 오차 가능):")
        for mm in mismatches:
            print(f"  · {mm}")
    if any(s not in SIZES for s in actual["사이즈"]):
        print("※ 사이즈를 알 수 없는 웨어하우스(삭제·AUTO 등)는 AWS 비용과 Pro 전환 비용을 계산하지 않았습니다.")