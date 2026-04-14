import logging
from datetime import date, datetime
from typing import Optional, Any
from dataclasses import dataclass

import requests

from app.feishu.auth import get_headers, FEISHU_BASE

logger = logging.getLogger(__name__)

EXCLUDE_PROGRESS = {"暂不下单，备用款", "订单取消"}

PROGRESS_COLOR = {
    "开发版已批，待下订单": "blue",
    "产前版生产中":         "blue",
    "产前版已回版":         "blue",
    "报价中":               "blue",
    "待运营确定价格":       "orange",
    "已定价，待下单":       "blue",
    "面料采购中":           "orange",
    "生产中":               "blue",
    "生产已完成，待出货":   "blue",
    "退厂返工中":           "orange",
    "已出部分":             "blue",
    "已出完":               "green",
    "暂不下单，备用款":     "gray",
    "订单取消":             "gray",
}


# ──────────────────────────────────────────────
# 选项映射缓存（用于回版状态、大货进度等真实选项字段）
# ──────────────────────────────────────────────

_opt_map: dict[str, str] = {}


def init_option_map(app_token: str,
                    table_dev: str,
                    table_bulk: str,
                    table_task: str) -> None:
    """
    启动时调用一次，拉取所有选项字段的映射。
    - 回版状态：从开发产品表拉
    - 大货进度、季节：从大货表拉
    - 品类：从开款任务表拉（来源最完整，90个选项）
    """
    _load_options(app_token, table_dev,  ["回版状态"])
    _load_options(app_token, table_bulk, ["季节"])
    _load_options(app_token, table_task, ["品类"])
    logger.info(f"选项映射加载完成，共 {len(_opt_map)} 个选项")


def _load_options(app_token: str, table_id: str, field_names: list[str]) -> None:
    url = f"{FEISHU_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
    try:
        resp = requests.get(url, headers=get_headers(), timeout=15)
        resp.raise_for_status()
        items = resp.json().get("data", {}).get("items", [])
    except Exception as e:
        logger.error(f"拉取字段元数据失败 table={table_id}: {e}")
        return

    for f in items:
        if f["field_name"] not in field_names:
            continue
        prop = f.get("property", {})
        # 普通选项字段：property.options
        opts = prop.get("options", [])
        # 公式选项字段：property.type.ui_property.options
        if not opts:
            opts = prop.get("type", {}).get("ui_property", {}).get("options", [])
        for o in opts:
            if o.get("id") and o.get("name"):
                _opt_map[o["id"]] = o["name"]


def _opt(opt_id: str) -> str:
    return _opt_map.get(opt_id, "")


# ──────────────────────────────────────────────
# 季节工具
# ──────────────────────────────────────────────

def get_active_seasons() -> list[str]:
    """
    根据当前月份返回需要展示的季节列表，同时排除手动剔除的季节。
    1–6月  → 当年-春夏、当年-秋冬、次年-春夏
    7–12月 → 当年-秋冬、次年-春夏、次年-秋冬
    手动剔除：通过 .env 的 EXCLUDED_SEASONS 配置
    """
    from app.config import config
    today = date.today()
    y = today.year % 100
    y_next = (today.year + 1) % 100
    if today.month <= 6:
        seasons = [f"{y}-春夏", f"{y}-秋冬", f"{y_next}-春夏"]
    else:
        seasons = [f"{y}-秋冬", f"{y_next}-春夏", f"{y_next}-秋冬"]

    excluded = config.excluded_seasons
    if excluded:
        seasons = [s for s in seasons if s not in excluded]
        logger.info(f"已剔除季节：{excluded}，当前活跃：{seasons}")

    return seasons


# ──────────────────────────────────────────────
# 数据模型
# ──────────────────────────────────────────────

@dataclass
class SampleRecord:
    """开发版明细表-【产品版本池】"""
    auto_id: str
    sample_no: str
    supplier: str
    developer: str
    developer_id: str
    product_type: str
    season: str
    send_date: Optional[date]
    return_date: Optional[date]
    review_date: Optional[date]
    notified: bool = False


@dataclass
class DevProductRecord:
    """开发产品表-【产品立项】"""
    product_no: str
    product_type: str
    developer: str
    developer_id: str
    season: str
    has_sent_version: bool
    version_status: str
    launch_batch: str            # 上架批次：首发款 / 预备款 / 空


@dataclass
class BulkOrderRecord:
    """大货表-【生产执行】"""
    style_no: str
    product_type: str
    developer: str
    developer_id: str
    season: str
    progress_text: str
    expected_delivery: Optional[date]
    actual_completion: Optional[date]
    supplier: str
    order_type: str              # 订单类型：首单 / 返单 / 空
    material_progress: str       # 面/辅料进度
    order_qty: str               # 下单数量
    factory_delivery: Optional[date]  # 工厂回复货期

    @property
    def is_excluded(self) -> bool:
        return self.progress_text in EXCLUDE_PROGRESS

    @property
    def is_reorder(self) -> bool:
        """是否为返单，返单不推送给开发"""
        return self.order_type == "返单"

    @property
    def progress_color(self) -> str:
        return PROGRESS_COLOR.get(self.progress_text, "blue")


# ──────────────────────────────────────────────
# 通用翻页拉取
# ──────────────────────────────────────────────

def _fetch_all_records(app_token: str, table_id: str) -> list[dict]:
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
            logger.error(f"拉取多维表失败 table={table_id}: {e}")
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
# 字段探查
# ──────────────────────────────────────────────

def inspect_fields(app_token: str, table_id: str, sample_rows: int = 3) -> dict[str, Any]:
    url = f"{FEISHU_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    resp = requests.get(url, headers=get_headers(),
                        params={"page_size": sample_rows}, timeout=15)
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
        if isinstance(f, dict) and "users" in f:
            return "person(users{})"
        if isinstance(f, list) and f and isinstance(f[0], dict):
            if "name" in f[0]:
                return "person"
            if "text" in f[0]:
                return "text(rich)"
            if "record_ids" in f[0]:
                return "relation"
        if isinstance(f, list) and f and isinstance(f[0], str):
            if f[0].startswith("opt"):
                return "option(opt_id)"
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
    """
    普通文本字段。兼容两种返回格式：
    - 普通 records API：[{"text": "xxx", "type": "text"}]
    - search API：{"type": 1, "value": [{"text": "xxx", "type": "text"}]}
    """
    val = fields.get(key)
    if val is None:
        return ""
    # search API 格式：{"type": 1, "value": [...]}
    if isinstance(val, dict) and "value" in val:
        val = val["value"]
    if isinstance(val, list) and val and isinstance(val[0], dict):
        return "".join(item.get("text", "") for item in val)
    return str(val)



def _relation_text(fields: dict, key: str) -> str:
    """
    关联字段，取第一条关联记录的 text 值。
    兼容两种格式：
    - 普通 records API：[{"text": "ZQZ402", "record_ids": [...]}]
    - search API：{"type": 1, "value": [{"text": "ZQZ402", ...}]}
    """
    val = fields.get(key)
    if val is None:
        return ""
    # search API 格式
    if isinstance(val, dict) and "value" in val:
        val = val["value"]
    if not val or not isinstance(val, list):
        return ""
    first = val[0]
    if isinstance(first, dict):
        return first.get("text", "")
    return ""

def _option(fields: dict, key: str) -> str:
    """单选字段，通过 opt_map 转成文字"""
    val = fields.get(key)
    if not val or not isinstance(val, list):
        return ""
    return _opt(val[0]) if val else ""


def _task_season_category(fields: dict) -> tuple[str, str]:
    """
    从「开款任务」关联字段解析季节和品类。
    文本格式：「季节-店铺-品线-品类」，如 26-秋冬-REORIA-基础款-连体衣
    季节 = 第一段（YY-春夏 / YY-秋冬 / 历史-春夏 等）
    品类 = 最后一段
    """
    val = fields.get("开款任务")
    if not val or not isinstance(val, list):
        return "", ""

    text = val[0].get("text", "") if isinstance(val[0], dict) else ""
    if not text:
        return "", ""

    parts = text.split("-")
    if len(parts) < 2:
        return text, ""

    # 季节：前两段拼起来，如 "26" + "秋冬" → "26-秋冬"
    season = f"{parts[0]}-{parts[1]}" if len(parts) >= 2 else parts[0]
    # 品类：最后一段
    category = parts[-1].strip() if parts[-1].strip() else ""

    return season, category


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
    val = fields.get(key)
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val == 1
    return False


def _person(fields: dict, key: str) -> tuple[str, str]:
    """
    人员字段，返回 (姓名, 飞书用户ID)。
    兼容两种结构：
      {"users": [{"name": "张三", "id": "ou_xxx"}]}   开发版明细表
      [{"name": "张三", "id": "ou_xxx"}]              开发产品表/大货表
    """
    val = fields.get(key)
    if not val:
        return "", ""

    if isinstance(val, dict) and "users" in val:
        users = val["users"]
    elif isinstance(val, list) and val and isinstance(val[0], dict):
        users = val
    else:
        return "", ""

    if not users:
        return "", ""

    first = users[0]
    name = first.get("name") or first.get("enName", "")
    uid = first.get("id", "")
    return name, uid


# ──────────────────────────────────────────────
# 今日回版记录（回版通知专用，只拉今日数据）
# ──────────────────────────────────────────────

def _search_str(fields: dict, key: str) -> str:
    """search API 文本字段解析：{"type":1, "value":[{"text":"xxx"}]}"""
    val = fields.get(key)
    if val is None:
        return ""
    if isinstance(val, dict) and "value" in val:
        items = val["value"]
        if isinstance(items, list):
            return "".join(i.get("text", "") for i in items if isinstance(i, dict))
    return _str(fields, key)


def _search_person(fields: dict, key: str) -> tuple[str, str]:
    """search API 人员字段解析：{"type":11, "value":[{"name":"xxx","id":"ou_xxx"}]}"""
    val = fields.get(key)
    if val is None:
        return "", ""
    if isinstance(val, dict) and "value" in val:
        users = val["value"]
        if users and isinstance(users[0], dict):
            first = users[0]
            return first.get("name", ""), first.get("id", "")
    return _person(fields, key)


def _search_date(fields: dict, key: str) -> Optional[date]:
    """search API 日期字段解析：{"type":5, "value":1234567890000}"""
    val = fields.get(key)
    if val is None:
        return None
    if isinstance(val, dict) and "value" in val:
        val = val["value"]
    if not val:
        return None
    try:
        return datetime.fromtimestamp(int(val) / 1000).date()
    except (ValueError, TypeError, OSError):
        return None


def fetch_today_return_samples(
    app_token: str,
    table_id: str,
    dev_product_map: dict[str, tuple[str, str]] | None = None,
) -> list[SampleRecord]:
    """
    只拉取回版日期=今日的样衣记录，用于回版实时通知。
    使用飞书 search API + Today 关键字过滤，避免拉全表。
    search API 返回的字段格式与普通 records API 不同，需单独解析。
    """
    url = f"{FEISHU_BASE}/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
    payload = {
        "filter": {
            "conjunction": "and",
            "conditions": [
                {
                    "field_name": "回版日期",
                    "operator": "is",
                    "value": ["Today"],
                }
            ]
        },
        "page_size": 500,
    }

    try:
        resp = requests.post(url, headers=get_headers(), json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.error(f"拉取今日回版记录失败: {e}")
        raise

    if data.get("code") != 0:
        raise RuntimeError(f"多维表 API 错误: {data.get('msg')} (code={data.get('code')})")

    items = data.get("data", {}).get("items", [])
    logger.info(f"今日回版记录：{len(items)} 条")

    # 款号关联通过 record_id 查找，需要拿到款号文字
    # search API 的款号字段只返回 link_record_ids，拿不到文字
    # 改为：从 dev_product_map 的 record_id 反查款号
    # 实际上更简单：直接从记录编号里截取款号（记录编号格式：款号-版本，如 ZQZ408-初版）
    def _extract_style_no(sample_no: str) -> str:
        """从记录编号提取款号，如 ZQZ408-初版 → ZQZ408"""
        if not sample_no:
            return ""
        # 找最后一个 - 之前的部分（复版的格式：ZSY913-复版2）
        # 款号本身不含中文，版本描述含中文
        parts = sample_no.split("-")
        style_parts = []
        for p in parts:
            if any('一' <= c <= '鿿' for c in p):
                break
            style_parts.append(p)
        return "-".join(style_parts) if style_parts else parts[0]

    results = []
    for item in items:
        f = item.get("fields", {})
        try:
            name, uid = _search_person(f, "开发")
            sample_no = _search_str(f, "记录编号")
            style_no = _extract_style_no(sample_no)
            season, category = ("", "")
            if dev_product_map and style_no:
                season, category = dev_product_map.get(style_no, ("", ""))
            results.append(SampleRecord(
                auto_id=str(_search_str(f, "自动编号") or _int(f, "自动编号")),
                sample_no=sample_no,
                supplier=_search_str(f, "打版工厂"),
                developer=name,
                developer_id=uid,
                product_type=category,
                season=season,
                send_date=_search_date(f, "下版日期"),
                return_date=_search_date(f, "回版日期"),
                review_date=_search_date(f, "审版日期"),
            ))
        except Exception as e:
            logger.warning(f"跳过异常记录 record_id={item.get('record_id')}: {e}")

    return results


# ──────────────────────────────────────────────
# 三张表读取
# ──────────────────────────────────────────────

def fetch_sample_records(
    app_token: str,
    table_id: str,
    dev_product_map: dict[str, tuple[str, str]] | None = None,
) -> list[SampleRecord]:
    """
    读取「开发版明细表-【产品版本池】」。
    dev_product_map: 款号 → (season, product_type) 的映射，
                     由外部传入（从开发产品表预先构建），用于补全季节和品类。
    """
    raw = _fetch_all_records(app_token, table_id)
    results = []
    for item in raw:
        f = item.get("fields", {})
        try:
            name, uid = _person(f, "开发")
            # 从款号关联字段里取款号文字
            style_no = _relation_text(f, "款号")
            # 通过款号从开发产品表映射里拿季节和品类
            season, category = ("", "")
            if dev_product_map and style_no:
                season, category = dev_product_map.get(style_no, ("", ""))
            results.append(SampleRecord(
                auto_id=str(_int(f, "自动编号")),
                sample_no=_str(f, "记录编号"),
                supplier=_str(f, "打版工厂"),
                developer=name,
                developer_id=uid,
                product_type=category,
                season=season,
                send_date=_date(f, "下版日期"),
                return_date=_date(f, "回版日期"),
                review_date=_date(f, "审版日期"),
            ))
        except Exception as e:
            logger.warning(f"跳过异常记录 record_id={item.get('record_id')}: {e}")
    return results


def build_dev_product_map(records: list) -> dict[str, tuple[str, str]]:
    """
    从开发产品表记录构建 款号 → (season, product_type) 的映射。
    供 fetch_sample_records 使用。
    """
    return {
        r.product_no: (r.season, r.product_type)
        for r in records
        if r.product_no
    }


def fetch_dev_product_records(app_token: str, table_id: str) -> list[DevProductRecord]:
    """读取「开发产品表-【产品立项】」"""
    raw = _fetch_all_records(app_token, table_id)
    results = []
    for item in raw:
        f = item.get("fields", {})
        try:
            name, uid = _person(f, "开发")
            season, category = _task_season_category(f)
            results.append(DevProductRecord(
                product_no=_str(f, "款号"),
                product_type=category,
                developer=name,
                developer_id=uid,
                season=season,
                has_sent_version=_bool(f, "下开发版"),
                version_status=_option(f, "回版状态"),
                launch_batch=_str(f, "上架批次"),
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
            name, uid = _person(f, "开发")
            results.append(BulkOrderRecord(
                style_no=_str(f, "款号"),
                product_type=_option(f, "品类"),
                developer=name,
                developer_id=uid,
                season=_option(f, "季节"),
                progress_text=_str(f, "大货进度"),
                expected_delivery=_date(f, "预计最后一批出货日期"),
                actual_completion=_date(f, "实际出完日期"),
                supplier=_str(f, "供应商"),
                order_type=_str(f, "订单类型"),
                material_progress=_str(f, "面/辅料进度"),
                order_qty=_str(f, "下单数量"),
                factory_delivery=_date(f, "工厂回复货期"),
            ))
        except Exception as e:
            logger.warning(f"跳过异常记录 record_id={item.get('record_id')}: {e}")
    return results


# ──────────────────────────────────────────────
# 季节过滤
# ──────────────────────────────────────────────

def filter_active_seasons(
    records: list[BulkOrderRecord],
) -> tuple[list[BulkOrderRecord], list[BulkOrderRecord]]:
    """
    返回 (主列表, 取消/备用列表)。
    季节结束条件：该季节所有有效记录的实际出完日期都有值，且最晚出完日距今 > 5 天。
    """
    today = date.today()
    active_seasons = get_active_seasons()

    season_records: dict[str, list[BulkOrderRecord]] = {}
    for r in records:
        season_records.setdefault(r.season, []).append(r)

    finished_seasons = set()
    for season in active_seasons:
        valid = [r for r in season_records.get(season, []) if not r.is_excluded]
        if not valid:
            continue
        if all(r.actual_completion for r in valid):
            latest = max(r.actual_completion for r in valid)
            if (today - latest).days > 5:
                finished_seasons.add(season)

    real_active = [s for s in active_seasons if s not in finished_seasons]
    main_list = [
        r for r in records
        if r.season in real_active and not r.is_excluded and not r.is_reorder
    ]
    excluded_list = [
        r for r in records
        if r.season in real_active and r.is_excluded
    ]

    return main_list, excluded_list
