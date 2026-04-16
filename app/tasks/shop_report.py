"""
店长播报。
每日 10:00 推送，每位店长只收到自己店铺的数据，按季节分行展示。
口径与产品经理播报一致：只统计首发款，所有完成率以计划数为分母。

配置方式（.env）：
    SHOP_MANAGER_IDS=REORIA:ou_xxx,品牌B:ou_yyy
    # 格式：店铺名:open_id，多个用英文逗号分隔
    # 店铺名必须与多维表格「店铺」字段值完全一致
"""
import logging
from datetime import date

from app.config import config
from app.feishu.bitable import init_option_map
from app.feishu.message import send_card
from app.tasks.pm_report import calc_pm_data, PmRow

logger = logging.getLogger(__name__)


def build_shop_card(shop: str, rows: list[PmRow]) -> dict:
    """为单个店铺构建卡片，rows 为该店铺所有活跃季节的数据行。"""
    today_str = date.today().strftime("%Y-%m-%d")

    def table(header, data_rows):
        return {
            "tag": "table",
            "page_size": max(len(data_rows), 20),
            "row_height": "low",
            "header_style": {"background_style": "grey", "bold": True, "lines": 1},
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
                {f"col{i}": cell for i, cell in enumerate(row)}
                for row in data_rows
            ],
        }

    elements = []

    # ── 板块1：新品开发进度 ──
    elements.append({"tag": "markdown", "content": "**📋 板块 1 · 新品开发进度**"})
    rows1 = [
        [r.season, str(r.total_plan), str(r.sent_version),
         str(r.yesterday_sent), r.completion_rate]
        for r in rows
    ]
    if rows1:
        elements.append(table(
            ["季节", "计划数", "已下版单", "昨日下版单", "新品完成率"],
            rows1,
        ))
    elements.append({"tag": "hr"})

    # ── 板块2：定版进度 ──
    elements.append({"tag": "markdown", "content": "**📅 板块 2 · 定版进度**"})
    rows2 = [
        [r.season, str(r.total_plan), str(r.finalized),
         str(r.yesterday_finalized), r.finalize_rate]
        for r in rows
    ]
    if rows2:
        elements.append(table(
            ["季节", "计划数", "已定版", "昨日定版数", "定版完成率"],
            rows2,
        ))
    elements.append({"tag": "hr"})

    # ── 板块3：大货生产进度 ──
    elements.append({"tag": "markdown", "content": "**🏭 板块 3 · 大货生产进度**"})
    elements.append({"tag": "markdown", "content": "**进行中：**"})
    rows3a = [
        [r.season, str(r.total_plan), str(r.in_production),
         str(r.yesterday_ordered), r.production_ratio]
        for r in rows
    ]
    if rows3a:
        elements.append(table(
            ["季节", "计划数", "生产中", "昨日下单数", "生产中占比"],
            rows3a,
        ))

    elements.append({"tag": "markdown", "content": "**已出完：**"})
    rows3b = [
        [r.season, str(r.total_plan), str(r.completed),
         str(r.yesterday_delivered), r.bulk_completion_rate]
        for r in rows
    ]
    if rows3b:
        elements.append(table(
            ["季节", "计划数", "已完成大货", "昨日交货数", "大货完成率"],
            rows3b,
        ))

    return {
        "schema": "2.0",
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {
                "tag": "plain_text",
                "content": f"{shop} · 店长播报  {today_str}",
            },
            "template": "green",
        },
        "body": {
            "elements": [
                {
                    "tag": "markdown",
                    "content": f"以下为 **{shop}** 当季首发款开发进度概览。",
                },
                {"tag": "hr"},
                *elements,
            ]
        },
    }


def run_shop_report():
    logger.info("店长播报开始")

    # 店长配置：{店铺名: open_id}
    shop_manager_map = config.shop_manager_ids
    if not shop_manager_map:
        logger.warning("未配置 SHOP_MANAGER_IDS，跳过店长播报")
        return

    init_option_map(
        config.bitable_app_token,
        config.table_dev_product,
        config.table_bulk_order,
        config.table_task,
    )

    # 复用产品经理播报的数据计算（口径完全一致）
    all_rows = calc_pm_data()

    # 按店铺分组
    shop_rows: dict[str, list[PmRow]] = {}
    for row in all_rows:
        shop_rows.setdefault(row.shop, []).append(row)

    for shop, manager_id in shop_manager_map.items():
        rows = shop_rows.get(shop, [])
        if not rows:
            logger.info(f"{shop}：无数据，跳过")
            continue

        # 按季节排序
        rows.sort(key=lambda r: r.season)

        try:
            card = build_shop_card(shop, rows)
            ok = send_card(user_id=manager_id, card=card)
            logger.info(f"店长播报 [{shop}] → {manager_id[:16]}... {'成功' if ok else '失败'}")
        except Exception as e:
            logger.error(f"店长播报 [{shop}] 推送失败: {e}")

    logger.info("店长播报完成")
