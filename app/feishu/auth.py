import time
import logging
import requests
from app.config import config

logger = logging.getLogger(__name__)

FEISHU_BASE = "https://open.feishu.cn/open-apis"

_token: str = ""
_token_expires_at: float = 0.0


def get_tenant_token() -> str:
    global _token, _token_expires_at

    if _token and time.time() < _token_expires_at:
        return _token

    url = f"{FEISHU_BASE}/auth/v3/tenant_access_token/internal"
    payload = {
        "app_id": config.feishu_app_id,
        "app_secret": config.feishu_app_secret,
    }

    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            raise RuntimeError(f"获取 token 失败: {data.get('msg')} (code={data.get('code')})")

        _token = data["tenant_access_token"]
        _token_expires_at = time.time() + data.get("expire", 7200) - 300

        logger.info("飞书 tenant_access_token 已刷新")
        return _token

    except requests.RequestException as e:
        logger.error(f"请求飞书 token 接口失败: {e}")
        raise


def get_headers() -> dict:
    return {
        "Authorization": f"Bearer {get_tenant_token()}",
        "Content-Type": "application/json",
    }
