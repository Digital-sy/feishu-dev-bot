"""
历史打版周期均值计算。
数据来源：开发版明细表中已有「下版日期」和「回版日期」的历史记录。
降级规则：
  1. 工厂 × 品类 均值（过去180天，≥3条）
  2. 全表 × 品类 均值（过去180天，≥3条）
  3. 默认兜底值 DEFAULT_DAYS
"""
from datetime import date, timedelta
from collections import defaultdict

DEFAULT_DAYS = 5
MIN_SAMPLES = 3
LOOKBACK_DAYS = 180


def build_cycle_map(samples: list) -> dict:
    """
    从开发版明细表记录构建均值映射。
    返回：
    {
        "factory": { (supplier, product_type): avg_days },
        "category": { product_type: avg_days },
    }
    """
    cutoff = date.today() - timedelta(days=LOOKBACK_DAYS)

    factory_buckets: dict[tuple, list[int]] = defaultdict(list)
    category_buckets: dict[str, list[int]] = defaultdict(list)

    for r in samples:
        if not r.send_date or not r.return_date:
            continue
        if r.send_date < cutoff:
            continue
        cycle = (r.return_date - r.send_date).days
        if cycle <= 0:
            continue
        key = (r.supplier, r.product_type)
        factory_buckets[key].append(cycle)
        category_buckets[r.product_type].append(cycle)

    factory_avg = {
        k: round(sum(v) / len(v))
        for k, v in factory_buckets.items()
        if len(v) >= MIN_SAMPLES
    }
    category_avg = {
        k: round(sum(v) / len(v))
        for k, v in category_buckets.items()
        if len(v) >= MIN_SAMPLES
    }

    return {"factory": factory_avg, "category": category_avg}


def estimate_return_date(
    supplier: str,
    product_type: str,
    send_date: date,
    cycle_map: dict,
) -> tuple[date, str]:
    """
    预估回版日期。
    返回 (预估日期, 依据说明)
    """
    factory_avg = cycle_map.get("factory", {})
    category_avg = cycle_map.get("category", {})

    key = (supplier, product_type)

    if key in factory_avg:
        days = factory_avg[key]
        source = f"工厂均值{days}天"
    elif product_type in category_avg:
        days = category_avg[product_type]
        source = f"品类均值{days}天"
    else:
        days = DEFAULT_DAYS
        source = f"默认{days}天"

    return send_date + timedelta(days=days), source
