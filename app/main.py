import logging
import os
import sys
import json
from pathlib import Path
from datetime import datetime, timedelta

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
from app.tasks.pm_report import run_pm_report
from app.feishu.bitable import init_option_map
from app.feishu.message import send_card
from app.config import config

NOTIFIED_FILE = Path("/root/feishu-dev-bot/data/notified_returns.json")
HEALTH_FILE   = Path("/root/feishu-dev-bot/data/health.json")
ADMIN_ID      = "ou_45d24eddffa044503caf29d6c8a2e003"  # 刘宗霖


def cleanup_notified():
    """每天凌晨清空已通知记录。"""
    if NOTIFIED_FILE.exists():
        with open(NOTIFIED_FILE, "w") as f:
            json.dump({"notified": []}, f)
    logger.info("已清空回版通知记录，开始新的一天")


def write_health():
    """早报执行完后写入健康心跳时间戳。"""
    HEALTH_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(HEALTH_FILE, "w") as f:
        json.dump({"last_run": datetime.now().isoformat()}, f)
    logger.info("健康心跳已更新")


def check_health():
    """每天 11:00 检查早报是否正常执行，超过 25 小时未执行则发告警。"""
    try:
        if not HEALTH_FILE.exists():
            raise FileNotFoundError("健康文件不存在，服务可能从未正常运行过")
        with open(HEALTH_FILE) as f:
            data = json.load(f)
        last_run = datetime.fromisoformat(data["last_run"])
        elapsed = datetime.now() - last_run
        if elapsed > timedelta(hours=25):
            raise RuntimeError(f"早报已超过 {int(elapsed.total_seconds()/3600)} 小时未执行")
        logger.info(f"健康检查通过，上次执行：{last_run.strftime('%Y-%m-%d %H:%M')}")
    except Exception as e:
        logger.error(f"健康检查失败：{e}")
        msg = f"**服务可能已停止运行**\n\n{str(e)}\n\n请登录服务器检查：\n`systemctl status feishu-dev-bot`"
        alert_card = {
            "schema": "2.0",
            "config": {"wide_screen_mode": False},
            "header": {
                "title": {"tag": "plain_text", "content": "开发工作助手异常告警"},
                "template": "red",
            },
            "body": {
                "elements": [{"tag": "markdown", "content": msg}]
            },
        }
        send_card(user_id=ADMIN_ID, card=alert_card)


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

# 产品经理播报：10:00
scheduler.add_job(
    run_pm_report,
    CronTrigger(hour=10, minute=0, timezone="Asia/Shanghai"),
    id="pm_report",
    name="产品经理播报",
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

# 每天 11:00 健康检查
scheduler.add_job(
    check_health,
    CronTrigger(hour=11, minute=0, timezone="Asia/Shanghai"),
    id="health_check",
    name="健康检查",
)

if __name__ == "__main__":
    init()
    logger.info("开发工作助手启动")
    logger.info("  每日早报 + 产品经理播报：10:00")
    logger.info("  回版通知：每5分钟（10:00-22:00）")
    logger.info("  健康检查：11:00")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("服务已停止")
