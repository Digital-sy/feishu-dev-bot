import logging
from datetime import date, timedelta

from app.config import config
from app.feishu.bitable import (
    fetch_sample_records,
    fetch_dev_product_records,
    fetch_bulk_order_records,
    build_dev_product_map,
    get_active_seasons,
    init_option_map,
)
from app.feishu.message import send_card

logger = logging.getLogger(__name__)


def get_return_forecast(samples: list, developer: str) -> list[dict]:
    today = date.today()
    deadline = today + timedelta(days=3)
    active_seasons = get_active_seasons()
    result = []
    for r in samples:
        if r.developer != developer:
            continue
        if r.season not in active_seasons:
            continue
        if not r.send_date:
            continue
        if r.return_date and today <= r.return_date <= deadline:
            result.append({
                "sample_no":   r.sample_no,
                "supplier":    r.supplier,
                "return_date": r.return_date.strftime("%m月%d日"),
                "review_date": r.review_date.strftime("%m月%d日") if r.review_date else "—",
                "source":      "已确认",
            })
    return sorted(result, key=lambda x: x["return_date"])


def get_pending_versions(dev_products: list, developer: str) -> tuple[list, list, list, list]:
    active_seasons = get_active_seasons()
    pending = []
    no_info = []
    reserve = []
    cancelled = []

    for r in dev_products:
        if r.developer != developer:
            continue
        if r.season not in active_seasons:
            continue

        base = {
            "product_no":   r.product_no,
            "product_type": r.product_type,
            "season":       r.season,
            "launch_batch": r.launch_batch,
        }

        if r.version_status == "取消":
            cancelled.append(base)
            continue

        if r.launch_batch == "预备款":
            reserve.append({**base, "version_status": r.version_status})
            continue

        if r.version_status == "无信息":
            no_info.append(base)
            continue

        if r.version_status == "未下版单":
            pending.append(base)

    return pending, no_info, reserve, cancelled


def get_bulk_progress(bulk_orders: list, developer: str) -> tuple[list, list]:
    today = date.today()
    active_seasons = get_active_seasons()
    main_list = []

    for r in bulk_orders:
        if r.developer != developer:
            continue
        if r.season not in active_seasons:
            continue
        if r.is_excluded or r.is_reorder:
            continue

        days_left = None
        if r.expected_delivery:
            days_left = (r.expected_delivery - today).days

        is_completed = r.progress_text == "已出完"

        main_list.append({
            "style_no":          r.style_no,
            "product_type":      r.product_type,
            "supplier":          r.supplier,
            "progress_text":     r.progress_text or "—",
            "progress_color":    r.progress_color,
            "expected_delivery": r.expected_delivery.strftime("%m月%d日") if r.expected_delivery else "—",
            "factory_delivery":  r.factory_delivery.strftime("%m月%d日") if r.factory_delivery else "—",
            "order_qty":         r.order_qty or "—",
            "material_progress": r.material_progress or "",
            "days_left":         days_left if not is_completed else None,
            "urgent":            False if is_completed else (days_left is not None and days_left <= 7),
            "is_completed":      is_completed,
        })

    main_list.sort(key=lambda x: (
        x["is_completed"],
        x["days_left"] is None,
        x["days_left"] or 9999,
    ))
    urgent_list = [r for r in main_list if r["urgent"]]
    return main_list, urgent_list


def _table(rows: list[list[str]], page_size: int = 50) -> dict:
    """构建飞书卡片 table 组件，rows[0] 为表头。"""
    header = rows[0]
    data_rows = rows[1:]
    # page_size 设为实际行数，避免分页导致卡片内容重复
    actual_size = max(len(data_rows), page_size)
    return {
        "tag": "table",
        "page_size": actual_size,
        "row_height": "low",
        "header_style": {
            "background_style": "grey",
            "bold": True,
            "lines": 1,
        },
        "columns": [
            {
                "name": f"col{i}",
                "display_name": col,
                "width": "auto",
                "horizontal_align": "left",
                "data_type": "text",
            }
            for i, col in enumerate(header)
        ],
        "rows": [
            {
                f"col{i}": cell
                for i, cell in enumerate(row)
            }
            for row in data_rows
        ],
    }


def build_card(
    developer: str,
    forecast: list,
    pending: list,
    no_info: list,
    reserve: list,
    cancelled: list,
    bulk_main: list,
    bulk_urgent: list,
) -> dict:
    today_str = date.today().strftime("%Y-%m-%d")
    total = len(pending) + len(forecast) + len(bulk_urgent)
    elements = []

    # ── 板块 1：待下版单跟进（一个表格，状态列区分） ──
    elements.append({"tag": "markdown", "content": "**📋 板块 1 · 待下版单跟进**"})

    version_rows = []
    for r in pending:
        version_rows.append([r["product_no"], r["product_type"], r["season"], "待下版"])
    for r in no_info:
        version_rows.append([r["product_no"], r["product_type"], r["season"], "跳过打版"])
    for r in reserve:
        version_rows.append([r["product_no"], r["product_type"], r["season"], "预备款（暂停）"])
    for r in cancelled:
        version_rows.append([r["product_no"], r["product_type"], r["season"], "已取消"])

    if version_rows:
        elements.append(_table(
            [["款号", "品类", "季节", "状态"]] + version_rows
        ))
    else:
        elements.append({"tag": "markdown", "content": "当前无待跟进产品 ✅"})

    elements.append({"tag": "hr"})

    # ── 板块 2：开发版回版预报（一个表格） ──
    elements.append({"tag": "markdown", "content": "**📅 板块 2 · 开发版回版预报（未来三日）**"})

    if forecast:
        elements.append(_table(
            [["版本编号", "工厂", "回版日期", "审版截止", "来源"]] +
            [[r["sample_no"], r["supplier"], r["return_date"], r["review_date"], r["source"]]
             for r in forecast]
        ))
    else:
        elements.append({"tag": "markdown", "content": "未来三日暂无回版计划"})

    elements.append({"tag": "hr"})

    # ── 板块 3：大货生产进度（进行中一个表，已出完一个表，共两个） ──
    elements.append({"tag": "markdown", "content": "**🏭 板块 3 · 大货生产进度**"})

    urgent_style_nos = {item["style_no"] for item in bulk_urgent}
    in_progress = [r for r in bulk_main
                   if not r["is_completed"] and r["style_no"] not in urgent_style_nos]
    completed = [r for r in bulk_main if r["is_completed"]]

    # 进行中（告急 + 进行中合并成一个表）
    active_rows = []
    for r in bulk_urgent:
        active_rows.append([
            f"⚠️{r['style_no']}", r["product_type"], r["progress_text"],
            f"{r['order_qty']}件",
            f"{r['expected_delivery']}(还剩{r['days_left']}天)",
            r["factory_delivery"],
        ])
    for r in in_progress:
        days_str = f"还剩{r['days_left']}天" if r["days_left"] is not None else "—"
        active_rows.append([
            r["style_no"], r["product_type"], r["progress_text"],
            f"{r['order_qty']}件",
            f"{r['expected_delivery']}({days_str})",
            r["factory_delivery"],
        ])

    if active_rows:
        elements.append({"tag": "markdown", "content": "**进行中：**"})
        elements.append(_table(
            [["款号", "品类", "进度", "下单量", "预计交期", "工厂货期"]] + active_rows
        ))
        # 面辅料进度单独用文字列在表格下方
        material_lines = []
        for r in bulk_urgent + in_progress:
            if r["material_progress"]:
                material_lines.append(
                    f"· **{r['style_no']}** {r['material_progress'][:50]}"
                )
        if material_lines:
            elements.append({"tag": "markdown", "content": "\n".join(material_lines)})

    # 已出完
    if completed:
        elements.append({"tag": "markdown", "content": "**已出完：**"})
        elements.append(_table(
            [["款号", "品类", "下单量", "预计交期", "工厂货期"]] +
            [[r["style_no"], r["product_type"],
              f"{r['order_qty']}件", r["expected_delivery"], r["factory_delivery"]]
             for r in completed]
        ))

    if not active_rows and not completed:
        elements.append({"tag": "markdown", "content": "当前无进行中的大货订单"})

    return {
        "schema": "2.0",
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {
                "tag": "plain_text",
                "content": f"开发工作助手 · 今日早报  {today_str}",
            },
            "template": "purple",
        },
        "body": {
            "elements": [
                {
                    "tag": "markdown",
                    "content": (
                        f"你好 **{developer}**，今日共有 **{total}** 项需关注。"
                        if total > 0
                        else f"你好 **{developer}**，今日暂无需关注的事项 ✅"
                    ),
                },
                {"tag": "hr"},
                *elements,
            ]
        },
    }


def run_daily_report():
    logger.info("早报生成开始")

    init_option_map(
        config.bitable_app_token,
        config.table_dev_product,
        config.table_bulk_order,
        config.table_task,
    )

    dev_products = fetch_dev_product_records(
        config.bitable_app_token, config.table_dev_product
    )
    dev_map = build_dev_product_map(dev_products)
    samples = fetch_sample_records(
        config.bitable_app_token, config.table_sample_detail, dev_map
    )
    bulk_raw = fetch_bulk_order_records(
        config.bitable_app_token, config.table_bulk_order
    )

    developers: dict[str, str] = {}
    for r in dev_products:
        if r.developer and r.developer_id:
            developers[r.developer] = r.developer_id
    for r in samples:
        if r.developer and r.developer_id:
            developers[r.developer] = r.developer_id

    logger.info(f"共识别到 {len(developers)} 位开发人员")

    for dev_name, dev_id in developers.items():
        try:
            forecast = get_return_forecast(samples, dev_name)
            pending, no_info, reserve, cancelled = get_pending_versions(dev_products, dev_name)
            bulk_progress, bulk_urgent = get_bulk_progress(bulk_raw, dev_name)

            if not forecast and not pending and not no_info and not reserve and not bulk_progress:
                logger.info(f"{dev_name}：无需推送，跳过")
                continue

            card = build_card(
                developer=dev_name,
                forecast=forecast,
                pending=pending,
                no_info=no_info,
                reserve=reserve,
                cancelled=cancelled,
                bulk_main=bulk_progress,
                bulk_urgent=bulk_urgent,
            )

            send_card(user_id=dev_id, card=card)
            logger.info(f"{dev_name}：早报推送成功")

        except Exception as e:
            logger.error(f"{dev_name}：早报推送失败 - {e}")

    logger.info("早报生成完成")
