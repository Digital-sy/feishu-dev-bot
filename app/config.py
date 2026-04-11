import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    feishu_app_id: str
    feishu_app_secret: str
    bitable_token_garment: str
    bitable_token_production: str
    table_sample_detail: str
    table_dev_product: str
    table_bulk_order: str
    claude_api_key: str
    log_level: str


def load_config() -> Config:
    required = {
        "FEISHU_APP_ID",
        "FEISHU_APP_SECRET",
        "BITABLE_APP_TOKEN_GARMENT",
        "BITABLE_APP_TOKEN_PRODUCTION",
        "TABLE_ID_SAMPLE_DETAIL",
        "TABLE_ID_DEV_PRODUCT",
        "TABLE_ID_BULK_ORDER",
        "CLAUDE_API_KEY",
    }
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise EnvironmentError(f"缺少必要环境变量: {', '.join(missing)}")

    return Config(
        feishu_app_id=os.environ["FEISHU_APP_ID"],
        feishu_app_secret=os.environ["FEISHU_APP_SECRET"],
        bitable_token_garment=os.environ["BITABLE_APP_TOKEN_GARMENT"],
        bitable_token_production=os.environ["BITABLE_APP_TOKEN_PRODUCTION"],
        table_sample_detail=os.environ["TABLE_ID_SAMPLE_DETAIL"],
        table_dev_product=os.environ["TABLE_ID_DEV_PRODUCT"],
        table_bulk_order=os.environ["TABLE_ID_BULK_ORDER"],
        claude_api_key=os.environ["CLAUDE_API_KEY"],
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )


config = load_config()
