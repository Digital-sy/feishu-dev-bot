import logging
from datetime import date, datetime
from typing import Optional, Any
from dataclasses import dataclass

import requests

from app.feishu.auth import get_headers, FEISHU_BASE

logger = logging.getLogger(__name__)

EXCLUDE_PROGRESS = {"暂不下单，备用款", "订单取消"}

PROGRESS_RANK = {
    "开发版已批，待下订单": 1,
    "产前版生产中":         2,
    "报价中":               3,
    "待运营确定价格":       4,
    "已定价，待下单":       5,
    "面料采购中":           6,
    "生产中":               7,
    "已出部分":             8,
    "已出完":               9,
    "暂不下单，备用款":     10,
    "订单取消":             11,
}

PROGRESS_COLOR = {
    "开发版已批，待下订单": "blue",
    "产前版生产中":         "blue",
    "报价中":               "blue",
    "待运营确定价格":       "orange",
    "已定价，待下单":       "blue",
    "面料采购中":           "orange",
    "生产中":               "blue",
    "已出部分":             "blue",
    "已出完":               "green",
    "暂不下单，备用款":     "gray",
    "订单取消":             "gray",
}


def get_active_seasons() -> list[str]:
    """
    根据当前月份返回需要展示的季节列表。
    格式：YY-春夏 / YY-秋冬
    1–6月  → 当年-春夏、当年-秋冬、次年-春夏
    7–12月 → 当年-秋冬、次年-春夏、次年-秋冬
    """
    today = date.today()
    y = today.year % 100        # 取后两位，如 2026 → 26
    y_next = (today.year + 1) % 100

    if today.month <= 6:
        return [f"{y}-春夏", f"{y}-秋冬", f"{y_next}-春夏"]
    else:
        return [f"{y}-秋冬", f"{y_next}-春夏", f"{y_next}-秋冬"]


@dataclass
class SampleRecord:
    """开发版明细表-【产品版本池】"""
    auto_id: str               # 自动编号（内部唯一ID）
    sample_no: str             # 记录编号（展示用，如 ZQZ402-初版）
    supplier: str              # 打版工厂
    developer: str             # 开发（人员字段）
    product_type: str          # 品类
    season: str                # 季节
    send_date: Optional[date]  # 下版日期
    return_date: Optional[date] # 回版日期
    review_date: Optional[date] # 审版日期
    notified: bool = False     # 回版通知是否已推送（去重用）


@dataclass
class DevProductRecord:
    """开发产品表-【产品立项】"""
    product_no: str                   # 款号
    product_type: str                 # 品类
    developer: str                    # 开发
    season: str                       # 季节
    has_sent_version: bool            # 下开发版（勾选框）
    version_status: str               # 回版状态（公式字段）
    # version_status 可能的值：
    # 未下版单 / 已下版 / 已回版，审版中 / 已审版，待寄出 / 已定版 / 取消


@dataclass
class BulkOrderRecord:
    """大货表-【生产执行】"""
    style_no: str                      # 款号
    product_type: str                  # 品类
    developer: str                     # 开发
    season: str                        # 季节
    progress_text: str                 # 大货进度（文字）
    expected_delivery: Optional[date]  # 预计最后一批出货日期
    actual_completion: Optional[date]  # 实际出完日期
    supplier: str                      # 供应商

    @property
    def is_excluded(self) -> bool:
        """是否属于取消/备用状态，不进主列表"""
        return self.progress_text in EXCLUDE_PROGRESS

    @property
    def progress_color(self) -> str:
        return PROGRESS_COLOR.get(self.progress_text, "blue")


# ──────────────────────────────────────────────
# 通用翻页拉取
# ──────────────────────────────────────────────

def _fetch_all_records(app_token: str, table_id: str) -> list[dict]:
    """翻页拉取子表全部记录，飞书单次最多 500 条。"""
    url = f"{FEISHU_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    records = []
    page_token = None

    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token

        try:
            resp = requests.get(url, headers=get_headers(), params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            logger.error(f"拉取多维表失败 app_token={app_token} table={table_id}: {e}")
            raise

        if data.get("code") != 0:
            raise RuntimeError(
                f"多维表 API 错误: {data.get('msg')} (code={data.get('code')})"
            )

        items = data.get("data", {}).get("items", [])
        records.extend(items)

        if data["data"].get("has_more"):
            page_token = data["data"]["page_token"]
        else:
            break

    logger.info(f"表 {table_id} 共拉取 {len(records)} 条记录")
    return records


# ──────────────────────────────────────────────
# 字段探查（首次接入用）
# ──────────────────────────────────────────────

def inspect_fields(app_token: str, table_id: str, sample_rows: int = 3) -> dict[str, Any]:
    """拉取前 N 条原始记录，返回字段名、推断类型、样本值。"""
    url = f"{FEISHU_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    resp = requests.get(
        url, headers=get_headers(),
        params={"page_size": sample_rows}, timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != 0:
        raise RuntimeError(f"多维表 API 错误: {data.get('msg')} (code={data.get('code')})")

    items = data.get("data", {}).get("items", [])
    if not items:
        return {}

    field_map: dict[str, dict] = {}
    for item in items:
        for key, val in item.get("fields", {}).items():
            if key not in field_map:
                field_map[key] = {"samples": [], "raw_type": type(val).__name__}
            if len(field_map[key]["samples"]) < 2:
                field_map[key]["samples"].append(val)

    def _infer(info: dict) -> str:
        s = info["samples"]
        if not s:
            return "unknown"
        f = s[0]
        if isinstance(f, list) and f and isinstance(f[0], dict):
            return "person" if "name" in f[0] else "text"
        if isinstance(f, int) and f > 1_000_000_000_000:
            return "date(timestamp_ms)"
        if isinstance(f, (int, float)):
            return "number"
        if isinstance(f, bool):
            return "checkbox"
        if isinstance(f, str):
            return "text(plain)"
        return info["raw_type"]

    for key, info in field_map.items():
        info["type"] = _infer(info)

    return field_map


# ──────────────────────────────────────────────
# 字段解析工具
# ──────────────────────────────────────────────

def _str(fields: dict, key: str) -> str:
    val = fields.get(key)
    if val is None:
        return ""
    if isinstance(val, list):
        return "".join(item.get("text", "") for item in val if isinstance(item, dict))
    return str(val)


def _date(fields: dict, key: str) -> Optional[date]:
    val = fields.get(key)
    if not val:
        return None
    try:
        return datetime.fromtimestamp(int(val) / 1000).date()
    except (ValueError, TypeError, OSError):
        logger.warning(f"日期字段 '{key}' 解析失败，原始值: {val!r}")
        return None


def _int(fields: dict, key: str, default: int = 0) -> int:
    val = fields.get(key)
    if val is None:
        return default
    try:
        return int(float(str(val)))
    except (ValueError, TypeError):
        return default


def _bool(fields: dict, key: str) -> bool:
    """勾选框字段：1 / True = 已勾选"""
    val = fields.get(key)
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val == 1
    return False


def _person(fields: dict, key: str) -> str:
    val = fields.get(key)
    if not val or not isinstance(val, list):
        return ""
    return "、".join(p.get("name", "") for p in val if isinstance(p, dict))


# ──────────────────────────────────────────────
# 三张表读取
# ──────────────────────────────────────────────

def fetch_sample_records(app_token: str, table_id: str) -> list[SampleRecord]:
    """读取「开发版明细表-【产品版本池】」"""
    raw = _fetch_all_records(app_token, table_id)
    results = []
    for item in raw:
        f = item.get("fields", {})
        try:
            results.append(SampleRecord(
                auto_id=str(_int(f, "自动编号")),
                sample_no=_str(f, "记录编号"),
                supplier=_str(f, "打版工厂"),
                developer=_person(f, "开发"),
                product_type=_str(f, "品类"),
                season=_str(f, "季节"),
                send_date=_date(f, "下版日期"),
                return_date=_date(f, "回版日期"),
                review_date=_date(f, "审版日期"),
            ))
        except Exception as e:
            logger.warning(f"跳过异常记录 record_id={item.get('record_id')}: {e}")
    return results


def fetch_dev_product_records(app_token: str, table_id: str) -> list[DevProductRecord]:
    """读取「开发产品表-【产品立项】」"""
    raw = _fetch_all_records(app_token, table_id)
    results = []
    for item in raw:
        f = item.get("fields", {})
        try:
            results.append(DevProductRecord(
                product_no=_str(f, "款号"),
                product_type=_str(f, "品类"),
                developer=_person(f, "开发"),
                season=_str(f, "季节"),
                has_sent_version=_bool(f, "下开发版"),
                version_status=_str(f, "回版状态"),
            ))
        except Exception as e:
            logger.warning(f"跳过异常记录 record_id={item.get('record_id')}: {e}")
    return results


def fetch_bulk_order_records(app_token: str, table_id: str) -> list[BulkOrderRecord]:
    """读取「大货表-【生产执行】」"""
    raw = _fetch_all_records(app_token, table_id)
    results = []
    for item in raw:
        f = item.get("fields", {})
        try:
            results.append(BulkOrderRecord(
                style_no=_str(f, "款号"),
                product_type=_str(f, "品类"),
                developer=_person(f, "开发"),
                season=_str(f, "季节"),
                progress_text=_str(f, "大货进度"),
                expected_delivery=_date(f, "预计最后一批出货日期"),
                actual_completion=_date(f, "实际出完日期"),
                supplier=_str(f, "供应商"),
            ))
        except Exception as e:
            logger.warning(f"跳过异常记录 record_id={item.get('record_id')}: {e}")
    return results


# ──────────────────────────────────────────────
# 季节过滤（早报调用）
# ──────────────────────────────────────────────

def filter_active_seasons(
    records: list[BulkOrderRecord],
) -> tuple[list[BulkOrderRecord], list[BulkOrderRecord]]:
    """
    按季节过滤大货记录，返回 (主列表, 取消/备用列表)。

    主列表：当前活跃季节内、非取消/备用状态的记录。
    取消/备用列表：进度为「暂不下单，备用款」或「订单取消」的记录。

    季节是否"已结束"的判断：
      该季节内所有有效记录（排除取消/备用）的实际出完日期都有值，
      且最晚出完日距今 > 5 天，则认为该季节已结束，移出主列表。
    """
    today = date.today()
    active_seasons = get_active_seasons()

    # 先按季节分桶
    season_records: dict[str, list[BulkOrderRecord]] = {}
    for r in records:
        season_records.setdefault(r.season, []).append(r)

    # 判断哪些活跃季节实际上已经结束
    finished_seasons = set()
    for season in active_seasons:
        season_valid = [
            r for r in season_records.get(season, [])
            if not r.is_excluded
        ]
        if not season_valid:
            continue
        all_completed = all(r.actual_completion for r in season_valid)
        if all_completed:
            latest = max(r.actual_completion for r in season_valid)
            if (today - latest).days > 5:
                finished_seasons.add(season)

    real_active = [s for s in active_seasons if s not in finished_seasons]

    main_list = [
        r for r in records
        if r.season in real_active and not r.is_excluded
    ]
    excluded_list = [
        r for r in records
        if r.season in real_active and r.is_excluded
    ]

    return main_list, excluded_list
