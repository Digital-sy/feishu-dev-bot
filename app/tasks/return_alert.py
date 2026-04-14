import json
import logging
from datetime import date
from pathlib import Path

from app.config import config
from app.feishu.bitable import (
    fetch_today_return_samples,
    fetch_dev_product_records,
    build_dev_product_map,
    get_active_seasons,
)
from app.feishu.message import send_card

logger = logging.getLogger(__name__)

NOTIFIED_FILE = Path("/root/feishu-dev-bot/data/notified_returns.json")


def _load_notified() -> set[str]:
    if not NOTIFIED_FILE.exists():
        return set()
    try:
        with open(NOTIFIED_FILE) as f:
            return set(json.load(f).get("notified", []))
    except Exception:
        return set()


def _save_notified(notified: set[str]) -> None:
    NOTIFIED_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(NOTIFIED_FILE, "w") as f:
        json.dump({"notified": list(notified)}, f, ensure_ascii=False)


def _build_alert_card(record) -> dict:
    return {
        "schema": "2.0",
        "config": {"wide_screen_mode": False},
        "header": {
            "title": {"tag": "plain_text", "content": "样衣回版提醒"},
            "template": "green",
        },
        "body": {
            "elements": [
                {
                    "tag": "markdown",
                    "content": f"你好 **{record.developer}**，你名下有样衣已回版，请安排时间前往审版室审版。",
                },
                {"tag": "hr"},
                {
                    "tag": "markdown",
                    "content": (
                        f"**版本编号：** {record.sample_no}\n"
                        f"**打版工厂：** {record.supplier}\n"
                        f"**品类：** {record.product_type}\n"
                        f"**回版日期：** {record.return_date.strftime('%Y-%m-%d')}\n"
                        f"**审版日期：** {record.review_date.strftime('%Y-%m-%d') if record.review_date else '待定'}"
                    ),
                },
            ]
        },
    }


def run_return_alert():
    """
    每5分钟执行，只拉今日回版数据，推送未通知的记录。
    时间窗口：每日 10:00 ~ 22:00，窗口外不发送。
    """
    from datetime import datetime
    now = datetime.now()
    if not (10 <= now.hour < 22):
        logger.debug(f"当前时间 {now.strftime('%H:%M')} 不在通知窗口（10:00-22:00），跳过")
        return

    active_seasons = get_active_seasons()
    notified = _load_notified()

    logger.info(f"回版通知检查，已通知：{len(notified)} 条")

    try:
        dev_products = fetch_dev_product_records(
            config.bitable_app_token, config.table_dev_product
        )
        dev_map = build_dev_product_map(dev_products)
        # 只拉今日回版记录
        samples = fetch_today_return_samples(
            config.bitable_app_token, config.table_sample_detail, dev_map
        )
    except Exception as e:
        logger.error(f"拉取数据失败: {e}")
        return

    to_notify = [
        r for r in samples
        if r.developer
        and r.developer_id
        and r.season in active_seasons
        and r.auto_id not in notified
    ]

    logger.info(f"今日回版待通知：{len(to_notify)} 条")

    new_notified = set()
    for r in to_notify:
        try:
            card = _build_alert_card(r)
            ok = send_card(user_id=r.developer_id, card=card)
            if ok:
                new_notified.add(r.auto_id)
                logger.info(f"回版通知已发送：{r.sample_no} → {r.developer}")
            else:
                logger.error(f"回版通知发送失败：{r.sample_no} → {r.developer}")
        except Exception as e:
            logger.error(f"回版通知异常：{r.sample_no}: {e}")

    if new_notified:
        notified.update(new_notified)
        _save_notified(notified)
        logger.info(f"新增通知 {len(new_notified)} 条，累计 {len(notified)} 条")
