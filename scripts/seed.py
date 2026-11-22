"""演示数据初始化。

用法：
    python scripts/seed.py           # 幂等：已存在的 SKU 跳过
    python scripts/seed.py --reset   # 清库重建（只影响 var/ 下的本地数据）
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from stockpact.container import Container  # noqa: E402
from stockpact.domain.order import IllegalArgument  # noqa: E402

CATALOG: list[tuple[str, str, int, int]] = [
    ("SKU-BOOK-001", "《数据密集型应用系统设计》", 12_900, 40),
    ("SKU-BOOK-002", "《软件架构：架构模式、特征及实践指南》", 9_900, 25),
    ("SKU-MUG-001", "单人咖啡杯（陶瓷）", 3_500, 60),
    ("SKU-KEY-001", "机械键盘（87 键）", 45_900, 12),
    ("SKU-HUB-001", "USB-C 扩展坞", 21_900, 8),
    ("SKU-CAM-001", "1080P 摄像头", 15_900, 3),
]


def main() -> int:
    parser = argparse.ArgumentParser(description="初始化 StockPact 演示数据")
    parser.add_argument("--reset", action="store_true", help="清空并重建本地库")
    args = parser.parse_args()

    container = Container()
    if args.reset:
        container.db.drop_schema()
        print("已清空本地库")
    container.init_schema()

    created, skipped = 0, 0
    for sku, name, price, stock in CATALOG:
        try:
            container.catalog.create_product(sku, name, price, stock)
            created += 1
        except IllegalArgument:
            skipped += 1

    print(f"商品：新建 {created} 个，跳过（已存在）{skipped} 个")
    print(f"数据库：{container.db.url}")
    for item in container.inventory.list_snapshots():
        print(
            f"  {item['sku']:<14} 在库={item['on_hand']:>4}  预留={item['reserved']:>3}  "
            f"可售={item['available']:>4}  单价=¥{item['price_cents'] / 100:.2f}"
        )
    container.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
