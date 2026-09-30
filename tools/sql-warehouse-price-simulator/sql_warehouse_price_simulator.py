# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # SQL Warehouse 가격 시뮬레이터 (AWS 서울 · Azure Korea Central)
# MAGIC
# MAGIC Classic / Pro / Serverless SQL Warehouse 비용을 **Databricks(DBU)** 와 **클라우드 인프라(VM + 디스크)** 로 나눠 계산합니다.
# MAGIC 워크스페이스 주소로 클라우드를 자동 판별합니다 (`cloud` 위젯으로 직접 지정 가능).
# MAGIC
# MAGIC | 섹션 | 내용 |
# MAGIC |---|---|
# MAGIC | 1. 단가 | DBU는 `system.billing.list_prices`, VM·디스크는 클라우드 공개 가격 API에서 조회 (실패 시 2026-10-01 점검값 사용) |
# MAGIC | 2. 시뮬레이션 | 위젯에서 고른 사이즈·사용 시간 기준 월 비용 비교 + 전체 사이즈 비교표 |
# MAGIC | 3. 실측 비교 | 이 워크스페이스 웨어하우스의 실제 DBU 사용량 → 실제 비용 추정 · 유형 전환 시 비용 |
# MAGIC
# MAGIC **계산 기준**
# MAGIC - Classic / Pro는 DBU와 클라우드 인프라 비용이 각각 청구되고, Serverless는 인프라 비용이 DBU에 포함됩니다.
# MAGIC - 사이즈별 DBU/h: [SQL Serverless SKU](https://learn.microsoft.com/en-us/azure/databricks/resources/pricing#sql-serverless-sku) — Classic/Pro에도 동일 적용
# MAGIC - 스팟 정책 ([SDK SpotInstancePolicy](https://databricks-sdk-py.readthedocs.io/en/stable/dbdataclasses/sql.html)): AWS는 Cost optimized(기본값) = 드라이버 온디맨드 + 워커 스팟, **Azure는 정책과 관계없이 전부 온디맨드**
# MAGIC
# MAGIC | | AWS 서울 | Azure Korea Central |
# MAGIC |---|---|---|
# MAGIC | 클러스터 구성 | 드라이버 i3 계열(사이즈별) + 워커 i3.2xlarge ([docs](https://docs.databricks.com/aws/en/compute/sql-warehouse/warehouse-behavior#classic-and-pro-sql-warehouses)) | 드라이버 Edsv4 계열(사이즈별) + 워커 Standard_E8ds_v4 ([docs](https://learn.microsoft.com/en-us/azure/databricks/compute/sql-warehouse/warehouse-behavior)) |
# MAGIC | 디스크 | 노드당 EBS 30GB + 150GB, gp3 기본 성능이라 IOPS·처리량 추가 과금 없음 ([docs](https://docs.databricks.com/aws/en/compute/configure#default-ebs-volumes)) | 노드당 256GB Premium SSD LRS (P15), 시간당 과금 ([docs](https://learn.microsoft.com/en-us/azure/databricks/compute/sql-warehouse/warehouse-behavior)) |
# MAGIC | Edition | Premium · Enterprise (SQL SKU는 동일 단가) | Premium만 |
# MAGIC | 가격 조회 | AWS 공개 가격 피드 | [Azure Retail Prices API](https://learn.microsoft.com/en-us/rest/api/cost-management/retail-prices/azure-retail-prices) |
# MAGIC
# MAGIC 미반영: NAT·데이터 전송, 오브젝트 스토리지, 클러스터 기동 소요 시간, RI/Savings Plan·약정 할인, 부가세

# COMMAND ----------

import gzip
import json
import urllib.parse
import urllib.request

import pandas as pd
from databricks.sdk import WorkspaceClient

HOURS_PER_MONTH = 730
FALLBACK_DATE = "2026-10-01"
# DBU/h per cluster (Classic/Pro/Serverless share the same sizing)
DBU_PER_HOUR = {"2X-Small": 4, "X-Small": 6, "Small": 12, "Medium": 24, "Large": 40,
                "X-Large": 80, "2X-Large": 144, "3X-Large": 272, "4X-Large": 528}
WORKER_COUNT = {"2X-Small": 1, "X-Small": 2, "Small": 4, "Medium": 8, "Large": 16,
                "X-Large": 32, "2X-Large": 64, "3X-Large": 128, "4X-Large": 256}

# Per-cloud profile: region, instances, disk, spot behavior and fallback prices (checked 2026-10-01)
PROFILES = {
    "AWS": {
        "label": "AWS 서울 (ap-northeast-2)",
        "editions": ["PREMIUM", "ENTERPRISE"],
        "region_sku": "AP_SEOUL",
        "location": "Asia Pacific (Seoul)",
        "region": "ap-northeast-2",
        "drivers": {"2X-Small": "i3.2xlarge", "X-Small": "i3.2xlarge", "Small": "i3.4xlarge", "Medium": "i3.8xlarge",
                    "Large": "i3.8xlarge", "X-Large": "i3.16xlarge", "2X-Large": "i3.16xlarge",
                    "3X-Large": "i3.16xlarge", "4X-Large": "i3.16xlarge"},
        "worker": "i3.2xlarge",
        "spot_supported": True,
        "vm_label": "AWS EC2",
        "disk_label": "AWS EBS",
        "disk_desc": "gp3 30GB + 150GB",
        "disk_units_per_node": 180,  # GB
        "disk_unit": "USD/GB-월 (gp3)",
        "fallback": {"od": {"i3.2xlarge": 0.732, "i3.4xlarge": 1.464, "i3.8xlarge": 2.928, "i3.16xlarge": 5.856},
                     "spot": {"i3.2xlarge": 0.2228}, "disk": 0.0912},
    },
    "AZURE": {
        "label": "Azure Korea Central (koreacentral)",
        "editions": ["PREMIUM"],
        "region_sku": "KOREA_CENTRAL",
        "region": "koreacentral",
        "drivers": {"2X-Small": "Standard_E8ds_v4", "X-Small": "Standard_E8ds_v4", "Small": "Standard_E16ds_v4",
                    "Medium": "Standard_E32ds_v4", "Large": "Standard_E32ds_v4", "X-Large": "Standard_E64ds_v4",
                    "2X-Large": "Standard_E64ds_v4", "3X-Large": "Standard_E64ds_v4", "4X-Large": "Standard_E64ds_v4"},
        "worker": "Standard_E8ds_v4",
        "spot_supported": False,
        "vm_label": "Azure VM",
        "disk_label": "Azure Disk",
        "disk_desc": "Premium SSD P15 256GB",
        "disk_units_per_node": 1,  # one P15 disk
        "disk_unit": "USD/디스크-월 (P15 LRS)",
        "fallback": {"od": {"Standard_E8ds_v4": 0.692, "Standard_E16ds_v4": 1.384, "Standard_E32ds_v4": 2.768,
                            "Standard_E64ds_v4": 5.536},
                     "spot": {}, "disk": 38.012142},
    },
}
SIZES = list(DBU_PER_HOUR)

# COMMAND ----------

dbutils.widgets.dropdown("cloud", "AUTO", ["AUTO", "AWS", "AZURE"], "00. 클라우드")
dbutils.widgets.dropdown("edition", "PREMIUM", ["PREMIUM", "ENTERPRISE"], "01. Edition")
dbutils.widgets.dropdown("size", "Small", SIZES, "02. 사이즈")
dbutils.widgets.text("hours_per_day", "9", "03. 하루 사용 시간(h)")
dbutils.widgets.text("days_per_month", "22", "04. 월 사용 일수")
dbutils.widgets.text("clusters", "1", "05. 평균 클러스터 수")
dbutils.widgets.dropdown("price_source", "AUTO", ["AUTO", "FALLBACK"], "06. 단가 소스")
dbutils.widgets.text("lookback_days", "30", "07. 실측 조회 기간(일)")
dbutils.widgets.dropdown("spot_policy_default", "COST_OPTIMIZED", ["COST_OPTIMIZED", "RELIABILITY_OPTIMIZED"],
                         "08. 실측: Serverless→Pro 전환 시 스팟 정책 (AWS만)")

w = WorkspaceClient()
HOST = w.config.host or ""
WORKSPACE_CLOUD = "AZURE" if "azuredatabricks.net" in HOST else ("AWS" if "cloud.databricks.com" in HOST else None)
CLOUD = dbutils.widgets.get("cloud")
if CLOUD == "AUTO":
    if WORKSPACE_CLOUD is None:
        raise ValueError(f"클라우드를 판별할 수 없습니다 ({HOST}). 'cloud' 위젯에서 AWS 또는 AZURE를 고르세요.")
    CLOUD = WORKSPACE_CLOUD
PROFILE = PROFILES[CLOUD]

EDITION = dbutils.widgets.get("edition")
if EDITION not in PROFILE["editions"]:
    print(f"※ {CLOUD}에는 {EDITION} Edition이 없어 {PROFILE['editions'][0]}로 계산합니다.")
    EDITION = PROFILE["editions"][0]
SIZE = dbutils.widgets.get("size")
HOURS_PER_DAY = float(dbutils.widgets.get("hours_per_day"))
DAYS_PER_MONTH = float(dbutils.widgets.get("days_per_month"))
CLUSTERS = float(dbutils.widgets.get("clusters"))
PRICE_SOURCE = dbutils.widgets.get("price_source")
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
SPOT_POLICY_DEFAULT = dbutils.widgets.get("spot_policy_default")
WORKER = PROFILE["worker"]
print(f"클라우드: {PROFILE['label']} · Edition: {EDITION}" + ("" if CLOUD == WORKSPACE_CLOUD else f" (워크스페이스는 {WORKSPACE_CLOUD})"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 단가

# COMMAND ----------


def fetch(url, timeout=10):
    raw = urllib.request.urlopen(url, timeout=timeout).read()
    return gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw


def load_dbu_prices(cloud, edition, region_sku):
    """Pick the regional SKU when prices are split by region, else the unsuffixed SKU."""
    rows = spark.sql(
        """
        SELECT sku_name, CAST(pricing.default AS DOUBLE) AS usd
        FROM system.billing.list_prices
        WHERE price_end_time IS NULL AND currency_code = 'USD' AND cloud = :cloud
          AND sku_name RLIKE :pattern
        """,
        args={"cloud": cloud, "pattern": f"^{edition}_(SQL_COMPUTE|SQL_PRO_COMPUTE|SERVERLESS_SQL_COMPUTE)(_.+)?$"},
    ).collect()
    found = {r.sku_name: r.usd for r in rows}
    prices, names = {}, {}
    for wh_type, base in (("CLASSIC", "SQL_COMPUTE"), ("PRO", "SQL_PRO_COMPUTE"), ("SERVERLESS", "SERVERLESS_SQL_COMPUTE")):
        generic, regional = f"{edition}_{base}", f"{edition}_{base}_{region_sku}"
        split_by_region = any(s.startswith(generic + "_") for s in found)
        if regional in found:
            pick = regional
        elif generic in found and not split_by_region:
            pick = generic
        else:
            # Never fall back to another region's price
            raise LookupError(f"list_prices({cloud})에서 {regional} 또는 리전 공통 {generic}을 찾지 못함")
        prices[wh_type], names[wh_type] = found[pick], pick
    return prices, names


def load_aws_prices(profile):
    loc = urllib.parse.quote(profile["location"])
    od_doc = json.loads(fetch(
        f"https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ec2-ondemand-without-sec-sel/{loc}/Linux/index.json"))
    od = {}
    for item in od_doc["regions"][profile["location"]].values():
        it = item.get("Instance Type")
        if it in profile["fallback"]["od"] and it not in od:
            od[it] = float(item["price"])
    published = od_doc["manifest"]["hawkFilePublicationDate"][:10]

    spot_js = fetch("https://website.spot.ec2.aws.a2z.com/spot.js").decode()
    spot_doc = json.loads(spot_js[spot_js.find("(") + 1: spot_js.rfind(")")])
    spot = {}
    for reg in spot_doc["config"]["regions"]:
        if reg["region"] != profile["region"]:
            continue
        for itype in reg["instanceTypes"]:
            for s in itype["sizes"]:
                if s["size"] == profile["worker"]:
                    linux = next(v for v in s["valueColumns"] if v["name"] == "linux")
                    spot[profile["worker"]] = float(linux["prices"]["USD"])

    ebs_doc = json.loads(fetch("https://b0.p.awsstatic.com/pricing/2.0/meteredUnitMaps/ec2/USD/current/ebs.json"))
    gp3 = float(ebs_doc["regions"][profile["location"]]["Storage General Purpose gp3 GB Mo"]["price"])

    missing = [k for k in profile["fallback"]["od"] if k not in od] + ([] if profile["worker"] in spot else [f"spot {profile['worker']}"])
    if missing:
        raise LookupError(f"AWS 가격 피드에 없음: {missing}")
    return {"od": od, "spot": spot, "disk": gp3}, f"AWS 공개 가격 피드 (온디맨드·EBS 게시 {published}, 스팟은 조회 시점 값)"


def azure_retail(filter_expr):
    url = "https://prices.azure.com/api/retail/prices?" + urllib.parse.urlencode({"$filter": filter_expr})
    items = []
    while url:
        doc = json.loads(fetch(url))
        items += doc["Items"]
        url = doc.get("NextPageLink")
    return items


def load_azure_prices(profile):
    region = profile["region"]
    od = {}
    for sku in profile["fallback"]["od"]:
        items = azure_retail(f"armRegionName eq '{region}' and armSkuName eq '{sku}' "
                             "and serviceName eq 'Virtual Machines' and priceType eq 'Consumption'")
        linux = [i for i in items if not any(x in i["productName"] for x in ("Windows", "Cloud Services"))
                 and not any(x in i["meterName"] for x in ("Spot", "Low Priority"))]
        if linux:
            od[sku] = min(float(i["unitPrice"]) for i in linux)
    disks = azure_retail(f"armRegionName eq '{region}' and serviceName eq 'Storage' "
                         "and skuName eq 'P15 LRS' and priceType eq 'Consumption'")
    p15 = [float(i["unitPrice"]) for i in disks if i["productName"] == "Premium SSD Managed Disks" and i["meterName"] == "P15 LRS Disk"]
    missing = [k for k in profile["fallback"]["od"] if k not in od] + ([] if p15 else ["P15 LRS Disk"])
    if missing:
        raise LookupError(f"Azure Retail Prices API에 없음: {missing}")
    return {"od": od, "spot": {}, "disk": p15[0]}, "Azure Retail Prices API (조회 시점 값)"


P = {"dbu": {"CLASSIC": 0.22, "PRO": 0.74, "SERVERLESS": 0.95},  # Seoul / Korea Central, 2026-10-01
     **{k: (dict(v) if isinstance(v, dict) else v) for k, v in PROFILE["fallback"].items()}}
SRC = {"dbu": f"fallback (점검 {FALLBACK_DATE})", "infra": f"fallback (점검 {FALLBACK_DATE})"}
SKU_NAMES = {}
if PRICE_SOURCE == "AUTO":
    try:
        P["dbu"], SKU_NAMES = load_dbu_prices(CLOUD, EDITION, PROFILE["region_sku"])
        SRC["dbu"] = "system.billing.list_prices (현재 활성 단가)"
    except Exception as e:
        print(f"[DBU] list_prices 조회 실패 → fallback 사용: {e}")
    try:
        infra, SRC["infra"] = (load_aws_prices if CLOUD == "AWS" else load_azure_prices)(PROFILE)
        P.update(infra)
    except Exception as e:
        print(f"[{CLOUD}] 가격 API 조회 실패 (인터넷 차단 등) → fallback 사용: {type(e).__name__}: {e}")


def disk_node_hourly():
    return PROFILE["disk_units_per_node"] * P["disk"] / HOURS_PER_MONTH


price_rows = [
    ("Databricks", f"SQL Classic ({SKU_NAMES.get('CLASSIC', '-')})", P["dbu"]["CLASSIC"], "USD/DBU", SRC["dbu"]),
    ("Databricks", f"SQL Pro ({SKU_NAMES.get('PRO', '-')})", P["dbu"]["PRO"], "USD/DBU", SRC["dbu"]),
    ("Databricks", f"SQL Serverless ({SKU_NAMES.get('SERVERLESS', '-')})", P["dbu"]["SERVERLESS"], "USD/DBU", SRC["dbu"]),
    *[(f"{PROFILE['vm_label']} 온디맨드", it, v, "USD/h", SRC["infra"]) for it, v in sorted(P["od"].items(), key=lambda x: x[1])],
    *[(f"{PROFILE['vm_label']} 스팟", it, v, "USD/h", SRC["infra"]) for it, v in P["spot"].items()],
    (PROFILE["disk_label"], PROFILE["disk_desc"], P["disk"], PROFILE["disk_unit"], SRC["infra"]),
    (PROFILE["disk_label"], "노드당 시간 비용", disk_node_hourly(), "USD/h", f"계산 (노드당 {PROFILE['disk_units_per_node']} × 단가 ÷ {HOURS_PER_MONTH}h)"),
]
display(pd.DataFrame(price_rows, columns=["구분", "항목", "단가", "단위", "출처"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 시뮬레이션

# COMMAND ----------


def infra_hourly(size, spot_policy):
    """VM and disk cost per cluster-hour for a Classic/Pro warehouse."""
    driver, workers = PROFILE["drivers"][size], WORKER_COUNT[size]
    use_spot = PROFILE["spot_supported"] and spot_policy == "COST_OPTIMIZED"
    worker_price = P["spot"][WORKER] if use_spot else P["od"][WORKER]
    vm = P["od"][driver] + workers * worker_price
    disk = (workers + 1) * disk_node_hourly()
    return vm, disk


VM, DISK = PROFILE["vm_label"], PROFILE["disk_label"]
if PROFILE["spot_supported"]:
    OPTIONS = [  # label, warehouse type, spot policy, chart label
        ("Classic · 온디맨드", "CLASSIC", "RELIABILITY_OPTIMIZED", "Classic\nOn-demand"),
        ("Classic · 스팟", "CLASSIC", "COST_OPTIMIZED", "Classic\nSpot"),
        ("Pro · 온디맨드", "PRO", "RELIABILITY_OPTIMIZED", "Pro\nOn-demand"),
        ("Pro · 스팟", "PRO", "COST_OPTIMIZED", "Pro\nSpot"),
        ("Serverless", "SERVERLESS", None, "Serverless"),
    ]
else:
    OPTIONS = [
        ("Classic", "CLASSIC", "RELIABILITY_OPTIMIZED", "Classic"),
        ("Pro", "PRO", "RELIABILITY_OPTIMIZED", "Pro"),
        ("Serverless", "SERVERLESS", None, "Serverless"),
    ]


def cost(size, wh_type, spot_policy, cluster_hours):
    dbu = DBU_PER_HOUR[size] * cluster_hours
    dbx = dbu * P["dbu"][wh_type]
    vm, disk = infra_hourly(size, spot_policy) if wh_type != "SERVERLESS" else (0.0, 0.0)
    return {"DBU": dbu, "Databricks": dbx, VM: vm * cluster_hours, DISK: disk * cluster_hours,
            "합계": dbx + (vm + disk) * cluster_hours}


CLUSTER_HOURS = HOURS_PER_DAY * DAYS_PER_MONTH * CLUSTERS
print(f"{SIZE}: {PROFILE['drivers'][SIZE]} 1대 + {WORKER} {WORKER_COUNT[SIZE]}대 · {DBU_PER_HOUR[SIZE]} DBU/h")
print(f"월 클러스터-시간 = {HOURS_PER_DAY:g}h × {DAYS_PER_MONTH:g}일 × {CLUSTERS:g}개 = {CLUSTER_HOURS:,.0f}h  ({CLOUD} · {EDITION}, USD)")
if not PROFILE["spot_supported"]:
    print(f"※ {CLOUD} SQL Warehouse는 스팟 정책과 관계없이 온디맨드로 실행되어 스팟 옵션이 없습니다.")

sim = pd.DataFrame([{"옵션": label, **cost(SIZE, t, sp, CLUSTER_HOURS)} for label, t, sp, _ in OPTIONS])
sim["Serverless 대비"] = sim["합계"] / sim.loc[sim["옵션"] == "Serverless", "합계"].iloc[0] - 1
display(sim.round({"DBU": 0, "Databricks": 2, VM: 2, DISK: 2, "합계": 2, "Serverless 대비": 3}))

# COMMAND ----------

import matplotlib.pyplot as plt

# English tick labels: cluster runtimes usually ship without a Hangul font
x = sim["옵션"].map({label: chart for label, _, _, chart in OPTIONS})
fig, ax = plt.subplots(figsize=(9, 4))
bottom = pd.Series([0.0] * len(sim))
for col, color in (("Databricks", "#FF3621"), (VM, "#1B3139"), (DISK, "#98A2AA")):
    ax.bar(x, sim[col], bottom=bottom, label=col, color=color)
    bottom += sim[col]
for i, total in enumerate(sim["합계"]):
    ax.text(i, total, f"${total:,.0f}", ha="center", va="bottom", fontsize=9)
ax.set_ylabel("USD / month")
ax.set_title(f"{CLOUD} · {SIZE} · {CLUSTER_HOURS:,.0f} cluster-hours / month")
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 전체 사이즈 월 비용 (USD, 위젯의 사용 시간 기준)

# COMMAND ----------

matrix = pd.DataFrame(
    {label: {size: cost(size, t, sp, CLUSTER_HOURS)["합계"] for size in SIZES} for label, t, sp, _ in OPTIONS}
)
matrix.insert(0, "DBU/h", [DBU_PER_HOUR[s] for s in SIZES])
matrix.insert(1, "구성", [f"{PROFILE['drivers'][s]} + {WORKER}×{WORKER_COUNT[s]}" for s in SIZES])
display(matrix.round(2).reset_index(names="사이즈"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 실측 비교 — 이 워크스페이스의 SQL Warehouse
# MAGIC
# MAGIC - DBU와 Databricks 비용은 `system.billing.usage` × 사용 시점의 `list_prices` (list price, 약정 할인 미반영)
# MAGIC - 인프라 비용은 추정치입니다: 가동 클러스터-시간 = DBU ÷ 사이즈별 DBU/h → VM + 디스크 단가 적용 (조회 기간 중 사이즈를 바꿨다면 오차 발생)
# MAGIC - 전환 비용은 같은 클러스터-시간을 쓴다고 가정합니다. Serverless는 기동이 빠르고 auto-stop을 짧게 잡을 수 있어 실제로는 가동 시간이 줄어드는 경우가 많습니다.

# COMMAND ----------

if CLOUD != WORKSPACE_CLOUD:
    dbutils.notebook.exit(f"'cloud' 위젯({CLOUD})이 워크스페이스 클라우드({WORKSPACE_CLOUD})와 달라 실측 비교를 건너뜁니다.")

WORKSPACE_ID = str(w.get_workspace_id())

meta = {}
for wh in w.warehouses.list():
    wh_type = "SERVERLESS" if wh.enable_serverless_compute else (wh.warehouse_type.value if wh.warehouse_type else "CLASSIC")
    spot = wh.spot_instance_policy.value if wh.spot_instance_policy else "COST_OPTIMIZED"
    meta[wh.id] = {
        "이름": wh.name,
        "유형": wh_type,
        "사이즈": wh.cluster_size,
        "스팟 정책": "-" if wh_type == "SERVERLESS" or not PROFILE["spot_supported"] else spot,
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
INFRA_COL = f"인프라 (추정, {VM} + {DISK})"
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
    known_size = size in DBU_PER_HOUR
    cluster_hours = dbu / DBU_PER_HOUR[size] if known_size else None
    # Serverless rows (or a type change within the lookback) carry no usable spot policy
    spot_policy = m["스팟 정책"] if m["스팟 정책"] in ("COST_OPTIMIZED", "RELIABILITY_OPTIMIZED") else SPOT_POLICY_DEFAULT

    infra = 0.0
    if actual_type != "SERVERLESS" and known_size:
        vm, disk = infra_hourly(size, spot_policy)
        infra = (vm + disk) * cluster_hours

    to_serverless = dbu * P["dbu"]["SERVERLESS"]
    if known_size:
        vm, disk = infra_hourly(size, spot_policy)
        to_pro = dbu * P["dbu"]["PRO"] + (vm + disk) * cluster_hours
    else:
        to_pro = None

    rows.append({
        **{k: m.get(k) for k in ("이름", "사이즈", "스팟 정책", "클러스터(min~max)", "auto-stop(분)")},
        "청구 유형": actual_type,
        "SKU": r.sku_name,
        "DBU": dbu,
        "클러스터-시간": cluster_hours,
        "Databricks (실제)": dbx,
        INFRA_COL: infra,
        "합계 (추정)": dbx + infra,
        "Serverless 전환 시": to_serverless,
        "Pro 전환 시": to_pro,
    })

if not rows:
    print("조회 기간에 SQL Warehouse 사용 이력이 없습니다. 'lookback_days'를 늘리거나 system.billing 권한을 확인하세요.")
else:
    actual = pd.DataFrame(rows).sort_values("합계 (추정)", ascending=False)
    print(f"조회 기간 합계 (USD). 월 환산은 × {MONTH_FACTOR:.2f}")
    display(actual.round(2))

    money = ["Databricks (실제)", INFRA_COL, "합계 (추정)", "Serverless 전환 시", "Pro 전환 시"]
    monthly = actual[["이름", "청구 유형", "사이즈"] + money].copy()
    monthly[money] = monthly[money].astype(float) * MONTH_FACTOR
    print("월 환산 (USD)")
    display(monthly.round(2))
    if mismatches:
        print("⚠️ 조회 기간 중 웨어하우스 유형이 변경된 것으로 보입니다 (인프라 추정·전환 비용에 오차 가능):")
        for mm in mismatches:
            print(f"  · {mm}")
    if any(s not in DBU_PER_HOUR for s in actual["사이즈"]):
        print("※ 사이즈를 알 수 없는 웨어하우스(삭제·AUTO 등)는 인프라 비용과 Pro 전환 비용을 계산하지 않았습니다.")
