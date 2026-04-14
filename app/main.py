import logging
import os
import sys
import json
from pathlib import Path

sys.path.insert(0, '/root/feishu-dev-bot')
os.chdir('/root/feishu-dev-bot')

from dotenv import load_dotenv
load_dotenv('/root/feishu-dev-bot/.env')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from app.tasks.daily_report import run_daily_report
from app.tasks.return_alert import run_return_alert
from app.feishu.bitable import init_option_map
from app.config import config

NOTIFIED_FILE = Path("/root/feishu-dev-bot/data/notified_returns.json")


def cleanup_notified():
    """每天凌晨清空已通知记录。"""
    if NOTIFIED_FILE.exists():
        with open(NOTIFIED_FILE, "w") as f:
            json.dump({"notified": []}, f)
    logger.info("已清空回版通知记录，开始新的一天")


def init():
    """启动时初始化选项映射。"""
    init_option_map(
        config.bitable_app_token,
        config.table_dev_product,
        config.table_bulk_order,
        config.table_task,
    )
    logger.info("选项映射初始化完成")


scheduler = BlockingScheduler(timezone="Asia/Shanghai")

# 每日早报：10:00
scheduler.add_job(
    run_daily_report,
    CronTrigger(hour=10, minute=0, timezone="Asia/Shanghai"),
    id="daily_report",
    name="每日早报",
    max_instances=1,
    misfire_grace_time=300,
)

# 回版实时通知：每5分钟
scheduler.add_job(
    run_return_alert,
    IntervalTrigger(minutes=5),
    id="return_alert",
    name="回版通知",
    max_instances=1,
    misfire_grace_time=60,
)

# 每天凌晨清理通知记录
scheduler.add_job(
    cleanup_notified,
    CronTrigger(hour=0, minute=0, timezone="Asia/Shanghai"),
    id="cleanup",
    name="清理通知记录",
)

if __name__ == "__main__":
    init()
    logger.info("开发工作助手启动")
    logger.info("  每日早报：10:00")
    logger.info("  回版通知：每5分钟（10:00-22:00）")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("服务已停止")
