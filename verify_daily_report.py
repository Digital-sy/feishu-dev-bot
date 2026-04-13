"""
手动触发早报，验证卡片生成和推送是否正常。
用法：
    python3 verify_daily_report.py preview   # 只打印卡片内容，不发送
    python3 verify_daily_report.py send      # 实际发送给所有开发
    python3 verify_daily_report.py send 程冰  # 只发给指定开发
"""
import sys
import os
import json
import logging

os.chdir('/root/feishu-dev-bot')
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s")

from dotenv import load_dotenv
load_dotenv('/root/feishu-dev-bot/.env')

from app.config import config
from app.feishu.bitable import (
    fetch_sample_records,
    fetch_dev_product_records,
    fetch_bulk_order_records,
    build_dev_product_map,
    filter_active_seasons,
    init_option_map,
)
from app.tasks.daily_report import (
    get_return_forecast,
    get_pending_versions,
    get_bulk_progress,
    build_card,
)
from app.feishu.message import send_card


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "preview"
    target_dev = sys.argv[2] if len(sys.argv) > 2 else None

    if mode not in ("preview", "send"):
        print("用法：python3 verify_daily_report.py [preview|send] [开发人员姓名]")
        sys.exit(1)

    print("\n正在拉取数据...")
    init_option_map(config.bitable_app_token, config.table_dev_product,
                    config.table_bulk_order, config.table_task)

    dev_products = fetch_dev_product_records(config.bitable_app_token, config.table_dev_product)
    dev_map = build_dev_product_map(dev_products)
    samples = fetch_sample_records(config.bitable_app_token, config.table_sample_detail, dev_map)
    bulk_raw = fetch_bulk_order_records(config.bitable_app_token, config.table_bulk_order)

    # 收集开发人员
    developers: dict[str, str] = {}
    for r in dev_products:
        if r.developer and r.developer_id:
            developers[r.developer] = r.developer_id
    for r in samples:
        if r.developer and r.developer_id:
            developers[r.developer] = r.developer_id

    if target_dev:
        if target_dev not in developers:
            print(f"未找到开发人员：{target_dev}")
            print(f"可用的开发人员：{list(developers.keys())[:10]}")
            sys.exit(1)
        developers = {target_dev: developers[target_dev]}

    print(f"共 {len(developers)} 位开发人员需要处理\n")

    for dev_name, dev_id in developers.items():
        forecast = get_return_forecast(samples, dev_name)
        pending, cancelled = get_pending_versions(dev_products, dev_name)
        bulk_progress, bulk_urgent = get_bulk_progress(bulk_raw, dev_name)

        if not forecast and not pending and not bulk_progress:
            print(f"[{dev_name}] 无内容，跳过")
            continue

        card = build_card(
            developer=dev_name,
            forecast=forecast,
            pending=pending,
            cancelled=cancelled,
            bulk_main=bulk_progress,
            bulk_urgent=bulk_urgent,
        )

        print(f"\n{'='*60}")
        print(f"  开发：{dev_name}  (id: {dev_id[:12]}...)")
        print(f"  回版预报：{len(forecast)} 条  待下版单：{len(pending)} 条  大货进度：{len(bulk_progress)} 条")
        print(f"{'='*60}")

        if mode == "preview":
            print(json.dumps(card, ensure_ascii=False, indent=2)[:800], "...")
        else:
            ok = send_card(user_id=dev_id, card=card)
            print(f"  发送结果：{'✅ 成功' if ok else '❌ 失败'}")

    print("\n完成")


if __name__ == "__main__":
    main()
