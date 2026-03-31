"""
Generate synthetic payment platform data with 4 planted reconciliation gaps.

Assumptions:
- Month-end cutoff: December 31, 2024
- Normal settlements arrive 1-2 days after transaction date
- Amounts in USD stored as floats (intentional for rounding gap)
- Refunds are negative amounts; originals are positive
"""

import csv
import os
import random
from datetime import date, timedelta

random.seed(42)
os.makedirs("data", exist_ok=True)

# ── Helpers ──────────────────────────────────────────────────────────────────

def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def random_amount() -> float:
    return round(random.uniform(10.0, 5000.0), 2)

# ── Build base transactions (normal, all within Dec 2024) ────────────────────

TX_START = date(2024, 12, 1)
TX_END   = date(2024, 12, 28)   # leave Dec 29-31 for gap planting

transactions = []   # (tx_id, tx_date, amount, description)
settlements  = []   # (settlement_id, tx_id, settle_date, amount)

for i in range(1, 116):
    tx_id    = f"TX-{i:04d}"
    tx_date  = random_date(TX_START, TX_END)
    amount   = random_amount()
    desc     = f"Payment from customer {i}"
    settle_date = tx_date + timedelta(days=random.randint(1, 2))

    transactions.append((tx_id, tx_date.isoformat(), amount, desc))
    settlements.append((f"SET-{i:04d}", tx_id, settle_date.isoformat(), amount))

# ── GAP 1 — Late settlement (TX date Dec 31, settlement Jan 1) ───────────────
#
# At month-end the reconciler cuts off at Dec 31.  This TX will appear in
# December's books but its settlement hits January — a common "in-flight" gap.

transactions.append(("TX-LATE-001", "2024-12-31", 899.99, "Late settlement payment"))
settlements.append(("SET-LATE-001", "TX-LATE-001", "2025-01-01", 899.99))

# ── GAP 2 — Rounding difference (shows only when summed) ────────────────────
#
# 20 transactions of $100.001 each (platform stores full precision).
# The bank rounds each settlement to 2 decimal places -> $100.00 each.
# Per-row difference: $0.001 — below the $0.005 matching tolerance, so
# each individual pair looks "fine". But summed across 20 rows:
#   platform total = $2000.020
#   bank total     = $2000.000
#   delta          = $0.020   -> exceeds $0.005 aggregate tolerance.
# This is a classic FX/fractional-cent accumulation pattern.

ROUNDING_IDS = [f"TX-ROUND-{j:03d}" for j in range(1, 21)]
for j, tx_id in enumerate(ROUNDING_IDS, 1):
    tx_date     = date(2024, 12, 15)
    settle_date = date(2024, 12, 16)
    tx_amount   = 100.001   # platform precision
    set_amount  = 100.00    # bank rounds down to 2dp

    transactions.append((tx_id, tx_date.isoformat(), tx_amount, f"Fractional-cent payment {j}"))
    settlements.append((f"SET-ROUND-{j:03d}", tx_id, settle_date.isoformat(), set_amount))

# ── GAP 3 — Duplicate settlement entry ──────────────────────────────────────
#
# The bank feed accidentally sent TX-0001's settlement twice.
# The platform only has one TX-0001, so one settlement row is unmatched.

dup_original = settlements[0]   # SET-0001 / TX-0001
dup          = (f"SET-DUP-{dup_original[0]}", dup_original[1], dup_original[2], dup_original[3])
settlements.append(dup)

# ── GAP 4 — Orphan refund (no matching original transaction) ─────────────────
#
# A –$150.00 refund appears in the bank feed but REF-9999 has no corresponding
# TX-9999 in the platform's transaction log.  Could be a manual bank-side
# reversal that the platform never recorded.

settlements.append(("REF-9999", "TX-9999", "2024-12-20", -150.00))

# ── Write CSVs ────────────────────────────────────────────────────────────────

with open("data/transactions.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["tx_id", "tx_date", "amount", "description"])
    w.writerows(transactions)

with open("data/settlements.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["settlement_id", "tx_id", "settle_date", "amount"])
    w.writerows(settlements)

print(f"[OK] transactions.csv  -- {len(transactions)} rows")
print(f"[OK] settlements.csv   -- {len(settlements)} rows")
print()
print("Planted gaps:")
print("  GAP 1 -- TX-LATE-001        : settled 2025-01-01 (next month)")
print("  GAP 2 -- TX-ROUND-001..100  : sum($0.10 x 100) != $10.00 due to float")
print("  GAP 3 -- SET-DUP-SET-0001   : duplicate of SET-0001 in settlements")
print("  GAP 4 -- REF-9999           : refund with no matching transaction")
