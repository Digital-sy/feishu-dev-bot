"""
产品经理播报。
每日 10:00 推送，按季节+店铺分组，统计开发进度数据。
口径：只统计首发款（排除预备款），所有完成率以计划数为分母。
"""
import logging
import os
from datetime import date, timedelta
from dataclasses import dataclass

from app.config import config
from app.feishu.bitable import (
    _fetch_all_records,
    _str, _date, _int, _opt, _option,
    _task_season_category,
    get_active_seasons,
    init_option_map,
)
from app.feishu.message import send_card

logger = logging.getLogger(__name__)

EXCLUDE_PROGRESS = {"暂不下单，备用款", "订单取消"}


@dataclass
class PmRow:
    season: str
    shop: str
    # 板块1
    total_plan: int
    sent_version: int
    yesterday_sent: int
    completion_rate: str
    # 板块2
    finalized: int
    yesterday_finalized: int
    finalize_rate: str
    # 板块3 进行中
    in_production: int
    yesterday_ordered: int
    production_ratio: str
    # 板块3 已出完
    completed: int
    yesterday_delivered: int
    bulk_completion_rate: str


def _pct(a: int, b: int) -> str:
    return f"{round(a / b * 100)}%" if b else "—"


def calc_pm_data() -> list[PmRow]:
    today = date.today()
    yesterday = today - timedelta(days=1)
    active = get_active_seasons()

    # ── 开款任务表：计划数 ──
    task_raw = _fetch_all_records(config.bitable_app_token, config.table_task)
    plan_map: dict[tuple, int] = {}
    for item in task_raw:
        f = item.get("fields", {})
        season = _str(f, "开款计划季节")
        shop = _str(f, "店铺")
        if season not in active or not shop:
            continue
        try:
            qty = int(float(str(f.get("开款数量") or 0)))
        except (ValueError, TypeError):
            qty = 0
        key = (season, shop)
        plan_map[key] = plan_map.get(key, 0) + qty

    # ── 开发产品表：已下版单 + 建立款号→季节/店铺/上架批次映射 ──
    dev_raw = _fetch_all_records(config.bitable_app_token, config.table_dev_product)
    sent_map: dict[tuple, int] = {}
    dev_season_map: dict[str, str] = {}   # 款号 → 季节
    dev_launch_map: dict[str, str] = {}   # 款号 → 上架批次（首发款/预备款/空）
    for item in dev_raw:
        f = item.get("fields", {})
        season, _ = _task_season_category(f)
        shop = _str(f, "店铺")
        product_no = _str(f, "款号")
        launch_batch = _str(f, "上架批次")

        # 建立款号映射（供大货表和定版明细表关联用）
        if season and product_no:
            dev_season_map[product_no] = season
        if product_no:
            dev_launch_map[product_no] = launch_batch

        if season not in active or not shop:
            continue

        # ★ 只统计首发款，预备款跳过
        if launch_batch == "预备款":
            continue

        status = _option(f, "回版状态")
        if status not in ("未下版单", "取消", ""):
            key = (season, shop)
            sent_map[key] = sent_map.get(key, 0) + 1

    # ── 开发版明细表：昨日下版单（下版日期=昨日）──
    sample_raw = _fetch_all_records(config.bitable_app_token, config.table_sample_detail)
    yesterday_sent_map: dict[tuple, int] = {}
    for item in sample_raw:
        f = item.get("fields", {})
        send_date = _date(f, "下版日期")
        if send_date != yesterday:
            continue
        style_val = f.get("款号")
        style_no = ""
        if isinstance(style_val, list) and style_val:
            first = style_val[0]
            if isinstance(first, dict):
                style_no = first.get("text", "")
        season = dev_season_map.get(style_no, "")

        # ★ 只统计首发款
        if dev_launch_map.get(style_no, "") == "预备款":
            continue

        shop_id = f.get("店铺")
        shop = _opt(shop_id[0]) if isinstance(shop_id, list) and shop_id else ""
        if season not in active or not shop:
            continue
        key = (season, shop)
        yesterday_sent_map[key] = yesterday_sent_map.get(key, 0) + 1

    # ── 定版明细表：已定版、昨日定版 ──
    fin_raw = _fetch_all_records(config.bitable_app_token, config.table_finalized)
    finalized_map: dict[tuple, int] = {}
    yesterday_fin_map: dict[tuple, int] = {}
    for item in fin_raw:
        f = item.get("fields", {})
        style_val = f.get("款号")
        style_no = ""
        if isinstance(style_val, list) and style_val:
            first = style_val[0]
            if isinstance(first, dict):
                style_no = first.get("text", "")
        season = dev_season_map.get(style_no, "")

        # ★ 只统计首发款
        if dev_launch_map.get(style_no, "") == "预备款":
            continue

        shop_id = f.get("店铺")
        shop = _opt(shop_id[0]) if isinstance(shop_id, list) and shop_id else ""
        if season not in active or not shop:
            continue
        key = (season, shop)
        finalized_map[key] = finalized_map.get(key, 0) + 1
        fin_date = _date(f, "定版日期")
        if fin_date == yesterday:
            yesterday_fin_map[key] = yesterday_fin_map.get(key, 0) + 1

    # ── 大货表：生产中、昨日下单、已出完、昨日交货 ──
    bulk_raw = _fetch_all_records(config.bitable_app_token, config.table_bulk_order)
    in_prod_map: dict[tuple, int] = {}
    yesterday_order_map: dict[tuple, int] = {}
    completed_map: dict[tuple, int] = {}
    yesterday_delivery_map: dict[tuple, int] = {}

    for item in bulk_raw:
        f = item.get("fields", {})
        style_no = _str(f, "款号")
        season = dev_season_map.get(style_no, "")
        shop_id = f.get("品牌")
        shop = _opt(shop_id[0]) if isinstance(shop_id, list) and shop_id else ""
        progress = _str(f, "大货进度")
        order_type = _str(f, "订单类型")

        if season not in active or not shop:
            continue
        if progress in EXCLUDE_PROGRESS or order_type == "返单":
            continue

        # ★ 只统计首发款
        if dev_launch_map.get(style_no, "") == "预备款":
            continue

        key = (season, shop)

        if progress == "已出完":
            completed_map[key] = completed_map.get(key, 0) + 1
            if _date(f, "实际出完日期") == yesterday:
                yesterday_delivery_map[key] = yesterday_delivery_map.get(key, 0) + 1
        else:
            in_prod_map[key] = in_prod_map.get(key, 0) + 1

        if _date(f, "跟单下单时间") == yesterday:
            yesterday_order_map[key] = yesterday_order_map.get(key, 0) + 1

    # ── 汇总 ──
    all_keys = (
        set(plan_map) | set(sent_map) |
        set(finalized_map) | set(in_prod_map) | set(completed_map)
    )
    rows = []
    for key in sorted(all_keys):
        season, shop = key
        total = plan_map.get(key, 0)
        sent = sent_map.get(key, 0)
        fin = finalized_map.get(key, 0)
        in_prod = in_prod_map.get(key, 0)
        comp = completed_map.get(key, 0)

        rows.append(PmRow(
            season=season, shop=shop,
            total_plan=total,
            sent_version=sent,
            yesterday_sent=yesterday_sent_map.get(key, 0),
            completion_rate=_pct(sent, total),
            finalized=fin,
            yesterday_finalized=yesterday_fin_map.get(key, 0),
            finalize_rate=_pct(fin, total),
            in_production=in_prod,
            yesterday_ordered=yesterday_order_map.get(key, 0),
            production_ratio=_pct(in_prod, total),      # ★ 分母改为计划数
            completed=comp,
            yesterday_delivered=yesterday_delivery_map.get(key, 0),
            bulk_completion_rate=_pct(comp, total),     # ★ 分母改为计划数
        ))

    return rows


def build_pm_card(rows: list[PmRow]) -> dict:
    today_str = date.today().strftime("%Y-%m-%d")

    def table(header, data_rows):
        return {
            "tag": "table",
            "page_size": max(len(data_rows), 50),
            "row_height": "low",
            "header_style": {"background_style": "grey", "bold": True, "lines": 1},
            "columns": [
                {"name": f"col{i}", "display_name": col,
                 "width": "auto", "horizontal_align": "left", "data_type": "text"}
                for i, col in enumerate(header)
            ],
            "rows": [
                {f"col{i}": cell for i, cell in enumerate(row)}
                for row in data_rows
            ],
        }

    elements = []

    # ── 板块1：待下版单 ──
    elements.append({"tag": "markdown", "content": "**📋 板块 1 · 待下版单跟进**"})
    rows1 = [
        [r.season, r.shop, str(r.total_plan), str(r.sent_version),
         str(r.yesterday_sent), r.completion_rate]
        for r in rows
    ]
    if rows1:
        elements.append(table(
            ["季节", "店铺", "计划数", "已下版单", "昨日下版单", "新品完成率"],
            rows1,
        ))
    elements.append({"tag": "hr"})

    # ── 板块2：回版预报 ──
    elements.append({"tag": "markdown", "content": "**📅 板块 2 · 开发版回版预报**"})
    rows2 = [
        [r.season, r.shop, str(r.total_plan), str(r.finalized),
         str(r.yesterday_finalized), r.finalize_rate]
        for r in rows
    ]
    if rows2:
        elements.append(table(
            ["季节", "店铺", "计划数", "已定版", "昨日定版数", "定版完成率"],
            rows2,
        ))
    elements.append({"tag": "hr"})

    # ── 板块3：大货生产进度 ──
    elements.append({"tag": "markdown", "content": "**🏭 板块 3 · 大货生产进度**"})
    elements.append({"tag": "markdown", "content": "**进行中：**"})
    rows3a = [
        [r.season, r.shop, str(r.total_plan), str(r.in_production),
         str(r.yesterday_ordered), r.production_ratio]
        for r in rows
    ]
    if rows3a:
        elements.append(table(
            ["季节", "店铺", "计划数", "生产中", "昨日下单数", "生产中占比"],
            rows3a,
        ))

    elements.append({"tag": "markdown", "content": "**已出完：**"})
    rows3b = [
        [r.season, r.shop, str(r.total_plan), str(r.completed),
         str(r.yesterday_delivered), r.bulk_completion_rate]
        for r in rows
    ]
    if rows3b:
        elements.append(table(
            ["季节", "店铺", "计划数", "已完成大货", "昨日交货数", "大货完成率"],
            rows3b,
        ))

    return {
        "schema": "2.0",
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": f"产品经理播报  {today_str}"},
            "template": "wathet",
        },
        "body": {
            "elements": [
                {"tag": "markdown", "content": "以下为当季各店铺**首发款**开发进度概览（已排除预备款）。"},
                {"tag": "hr"},
                *elements,
            ]
        },
    }


def run_pm_report():
    logger.info("产品经理播报开始")

    init_option_map(
        config.bitable_app_token,
        config.table_dev_product,
        config.table_bulk_order,
        config.table_task,
    )

    rows = calc_pm_data()
    logger.info(f"共 {len(rows)} 个季节+店铺组合")

    if not rows:
        logger.warning("无数据，跳过推送")
        return

    card = build_pm_card(rows)
    from app.feishu.message import send_card
    for uid in config.pm_receiver_ids:
        ok = send_card(user_id=uid, card=card)
        logger.info(f"产品经理播报推送 {uid[:16]}... {'成功' if ok else '失败'}")
