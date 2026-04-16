import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    feishu_app_id: str
    feishu_app_secret: str
    bitable_app_token: str
    table_sample_detail: str     # 开发版明细表-【产品版本池】
    table_dev_product: str       # 开发产品表-【产品立项】
    table_bulk_order: str        # 大货表-【生产执行】
    table_task: str              # 开款任务表（季节/品类选项的源头）
    llm_api_key: str
    llm_api_base: str
    llm_model: str
    log_level: str
    excluded_seasons: list[str]  # 手动剔除的季节，逗号分隔
    summary_receiver_id: str     # 统筹汇总接收人 open_id
    table_finalized: str         # 定版明细表
    pm_receiver_ids: list[str]   # 产品经理播报接收人列表


def load_config() -> Config:
    required = {
        "FEISHU_APP_ID",
        "FEISHU_APP_SECRET",
        "BITABLE_APP_TOKEN",
        "TABLE_ID_SAMPLE_DETAIL",
        "TABLE_ID_DEV_PRODUCT",
        "TABLE_ID_BULK_ORDER",
        "TABLE_ID_TASK",
    }
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise EnvironmentError(f"缺少必要环境变量: {', '.join(missing)}")

    return Config(
        feishu_app_id=os.environ["FEISHU_APP_ID"],
        feishu_app_secret=os.environ["FEISHU_APP_SECRET"],
        bitable_app_token=os.environ["BITABLE_APP_TOKEN"],
        table_sample_detail=os.environ["TABLE_ID_SAMPLE_DETAIL"],
        table_dev_product=os.environ["TABLE_ID_DEV_PRODUCT"],
        table_bulk_order=os.environ["TABLE_ID_BULK_ORDER"],
        table_task=os.environ["TABLE_ID_TASK"],
        llm_api_key=os.getenv("LLM_API_KEY", ""),
        llm_api_base=os.getenv("LLM_API_BASE", ""),
        llm_model=os.getenv("LLM_MODEL", ""),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        excluded_seasons=[
            s.strip() for s in os.getenv("EXCLUDED_SEASONS", "").split(",")
            if s.strip()
        ],
        summary_receiver_id=os.getenv("SUMMARY_RECEIVER_ID", ""),
        table_finalized=os.getenv("TABLE_ID_FINALIZED", ""),
        pm_receiver_ids=[
            s.strip() for s in os.getenv("PM_RECEIVER_IDS", "").split(",")
            if s.strip()
        ],
    )


config = load_config()
