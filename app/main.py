import logging
import os
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
from app.tasks.daily_report import run_daily_report

scheduler = BlockingScheduler(timezone="Asia/Shanghai")

scheduler.add_job(
    run_daily_report,
    CronTrigger(hour=10, minute=30, timezone="Asia/Shanghai"),
    id="daily_report",
    name="每日早报",
    max_instances=1,
    misfire_grace_time=300,
)

if __name__ == "__main__":
    logger.info("开发工作助手启动，每日 10:30 推送早报")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("服务已停止")
