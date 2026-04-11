import sys
import logging
from datetime import date

logging.basicConfig(level=logging.WARNING, format="%(levelname)-7s  %(message)s")

from app.config import config
from app.feishu.bitable import (
    inspect_fields,
    init_option_map,
    fetch_sample_records,
    fetch_dev_product_records,
    fetch_bulk_order_records,
    build_dev_product_map,
    get_active_seasons,
    filter_active_seasons,
)

TABLES = [
    {
        "label":     "开发版明细表-【产品版本池】",
        "app_token": config.bitable_app_token,
        "table_id":  config.table_sample_detail,
        "expected_fields": {
            "自动编号": "auto_id", "记录编号": "sample_no",
            "打版工厂": "supplier", "开发": "developer",
            "开款任务": "season+product_type",
            "下版日期": "send_date", "回版日期": "return_date", "审版日期": "review_date",
        },
    },
    {
        "label":     "开发产品表-【产品立项】",
        "app_token": config.bitable_app_token,
        "table_id":  config.table_dev_product,
        "expected_fields": {
            "款号": "product_no", "开发": "developer",
            "开款任务": "season+product_type",
            "下开发版": "has_sent_version", "回版状态": "version_status",
        },
    },
    {
        "label":     "大货表-【生产执行】",
        "app_token": config.bitable_app_token,
        "table_id":  config.table_bulk_order,
        "expected_fields": {
            "款号": "style_no", "品类": "product_type",
            "开发": "developer", "季节": "season",
            "大货进度": "progress_text",
            "预计最后一批出货日期": "expected_delivery",
            "实际出完日期": "actual_completion", "供应商": "supplier",
        },
    },
]

SEP = "─" * 62


def cmd_inspect():
    init_option_map(config.bitable_app_token, config.table_dev_product, config.table_bulk_order, config.table_task)
    print(f"\n{'═'*62}\n  阶段一：字段名探查\n{'═'*62}")
    print(f"  App ID: {config.feishu_app_id[:12]}...\n")

    all_ok = True
    for tbl in TABLES:
        print(f"\n{SEP}\n  表：{tbl['label']}\n{SEP}")
        try:
            field_map = inspect_fields(tbl["app_token"], tbl["table_id"])
        except Exception as e:
            print(f"  ❌ 拉取失败: {e}")
            all_ok = False
            continue

        if not field_map:
            print("  ⚠️  空表，无法探查")
            continue

        col_w = max(len(k) for k in field_map) + 2
        print(f"  {'字段名':<{col_w}}  {'类型':<26}  样本值")
        print(f"  {'─'*col_w}  {'─'*26}  {'─'*20}")
        for fname, info in sorted(field_map.items()):
            samples = info.get("samples", [])
            sample_str = repr(samples[0])[:45] if samples else "—"
            print(f"  {fname:<{col_w}}  {info['type']:<26}  {sample_str}")

        missing = [k for k in tbl["expected_fields"] if k not in field_map]
        if missing:
            print(f"\n  ⚠️  以下字段未找到：")
            for m in missing:
                print(f"     ✗  '{m}'  →  {tbl['expected_fields'][m]}")
            all_ok = False
        else:
            print(f"\n  ✅ 所有期望字段均存在")

    print(f"\n{'═'*62}")
    if all_ok:
        print("  字段全部匹配，下一步：python3 verify_bitable.py verify")
    else:
        print("  请按提示修改 bitable.py 后重新运行")
    print(f"{'═'*62}\n")


def _show(label, records, attrs, extra_fn=None):
    print(f"\n{SEP}\n  {label}  共 {len(records)} 条\n{SEP}")
    if not records:
        print("  ⚠️  没有数据，请检查表 ID 和权限")
        return
    for r in records[:3]:
        parts = []
        for a in attrs:
            val = getattr(r, a, None)
            if isinstance(val, date):
                val = val.isoformat()
            parts.append(f"{a}={val!r}")
        print("  " + "  ".join(parts))
    if len(records) > 3:
        print(f"  … 另有 {len(records)-3} 条已省略")
    if extra_fn:
        extra_fn(records)


def cmd_verify():
    init_option_map(config.bitable_app_token, config.table_dev_product, config.table_bulk_order, config.table_task)
    print(f"\n{'═'*62}\n  阶段二：数据验证\n{'═'*62}\n")

    today = date.today()
    active = get_active_seasons()
    print(f"  今日：{today}  当前活跃季节：{active}\n")

    errors = []

    # ── 开发版明细 ──────────────────────────────
    try:
        devp_pre = fetch_dev_product_records(config.bitable_app_token, config.table_dev_product)
        dev_map = build_dev_product_map(devp_pre)
        samples = fetch_sample_records(config.bitable_app_token, config.table_sample_detail, dev_map)
        def se(records):
            no_dev   = [r for r in records if not r.developer]
            in_season = [r for r in records if r.season in active]
            pending  = [r for r in in_season if r.send_date and not r.return_date]
            returned = [r for r in in_season if r.return_date]
            seasons  = {}
            for r in records:
                if r.season:
                    seasons[r.season] = seasons.get(r.season, 0) + 1
            print(f"\n  摘要：")
            print(f"    总记录：{len(records)} 条  当前季节内：{len(in_season)} 条")
            print(f"    已下版未回版：{len(pending)} 条  已回版：{len(returned)} 条")
            if no_dev:
                print(f"    ⚠️  开发字段为空：{len(no_dev)} 条")
            print(f"    季节分布（前5）：")
            for s, c in sorted(seasons.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"      {s}：{c} 条")
        _show("开发版明细表", samples,
              ["auto_id", "sample_no", "supplier", "developer", "season", "product_type", "send_date"], se)
    except Exception as e:
        errors.append(f"开发版明细: {e}"); print(f"  ❌ {e}")

    # ── 开发产品表 ──────────────────────────────
    try:
        devp = fetch_dev_product_records(config.bitable_app_token, config.table_dev_product)
        def de(records):
            in_season = [r for r in records if r.season in active]
            pending   = [r for r in in_season if r.version_status == "未下版单"]
            status_counts: dict[str, int] = {}
            for r in in_season:
                status_counts[r.version_status] = status_counts.get(r.version_status, 0) + 1
            print(f"\n  摘要（当前季节内 {len(in_season)} 条）：")
            print(f"    未下版单：{len(pending)} 条")
            print(f"    各状态分布：")
            for s, c in sorted(status_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"      {s or '(空)'}：{c} 条")
        _show("开发产品表", devp,
              ["product_no", "product_type", "developer", "season", "version_status"], de)
    except Exception as e:
        errors.append(f"开发产品表: {e}"); print(f"  ❌ {e}")

    # ── 大货表 ──────────────────────────────────
    try:
        bulk = fetch_bulk_order_records(config.bitable_app_token, config.table_bulk_order)
        def be(records):
            main_list, excluded = filter_active_seasons(records)
            w7 = [r for r in main_list
                  if r.expected_delivery and 0 <= (r.expected_delivery - today).days <= 7]
            w3 = [r for r in w7 if (r.expected_delivery - today).days <= 3]
            progress_counts: dict[str, int] = {}
            for r in main_list:
                progress_counts[r.progress_text] = progress_counts.get(r.progress_text, 0) + 1
            print(f"\n  摘要：")
            print(f"    主列表（活跃季节，非取消）：{len(main_list)} 条")
            print(f"    取消/备用：{len(excluded)} 条")
            print(f"    交期 ≤7天：{len(w7)} 条  其中 ≤3天：{len(w3)} 条")
            print(f"    进度分布：")
            for p, c in sorted(progress_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"      {p or '(空)'}：{c} 条")
        _show("大货表", bulk,
              ["style_no", "product_type", "developer", "season", "progress_text", "expected_delivery"], be)
    except Exception as e:
        errors.append(f"大货表: {e}"); print(f"  ❌ {e}")

    # ── 汇总 ────────────────────────────────────
    print(f"\n{'═'*62}")
    if errors:
        print("  验证失败：")
        for e in errors:
            print(f"    ❌ {e}")
        sys.exit(1)
    else:
        print("  全部通过 ✅  下一步：早报生成逻辑")
    print(f"{'═'*62}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in ("inspect", "verify"):
        print("\n用法：\n  python3 verify_bitable.py inspect\n  python3 verify_bitable.py verify\n")
        sys.exit(1)
    {"inspect": cmd_inspect, "verify": cmd_verify}[sys.argv[1]]()
