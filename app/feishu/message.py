import logging
import requests
from app.feishu.auth import get_headers, FEISHU_BASE

logger = logging.getLogger(__name__)


def send_card(user_id: str, card: dict) -> bool:
    """
    向指定飞书用户发送消息卡片。
    user_id 是飞书的 open_id（ou_ 开头）。
    """
    url = f"{FEISHU_BASE}/im/v1/messages"
    params = {"receive_id_type": "open_id"}

    payload = {
        "receive_id": user_id,
        "msg_type": "interactive",
        "content": __import__("json").dumps(card, ensure_ascii=False),
    }

    try:
        resp = requests.post(
            url,
            headers=get_headers(),
            params=params,
            json=payload,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            logger.error(f"发送卡片失败: {data.get('msg')} (code={data.get('code')})")
            return False

        logger.info(f"卡片发送成功 → {user_id}")
        return True

    except requests.RequestException as e:
        logger.error(f"发送卡片请求失败: {e}")
        return False


def send_text(user_id: str, text: str) -> bool:
    """向指定飞书用户发送纯文本消息（调试用）。"""
    url = f"{FEISHU_BASE}/im/v1/messages"
    params = {"receive_id_type": "open_id"}

    payload = {
        "receive_id": user_id,
        "msg_type": "text",
        "content": __import__("json").dumps({"text": text}, ensure_ascii=False),
    }

    try:
        resp = requests.post(
            url,
            headers=get_headers(),
            params=params,
            json=payload,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != 0:
            logger.error(f"发送文本失败: {data.get('msg')} (code={data.get('code')})")
            return False

        return True

    except requests.RequestException as e:
        logger.error(f"发送文本请求失败: {e}")
        return False
