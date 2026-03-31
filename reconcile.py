"""
ReconciliationEngine — core logic for matching platform transactions against
bank settlement records and surfacing discrepancies.

Matching rules:
- A transaction is "matched" when a settlement row shares the same tx_id.
- Late settlement: matched, but settle_date > cutoff_date.
- Rounding difference: matched by tx_id, but |tx_amount - settle_amount| > tolerance
  when summed across a group (here we compare individual rows then flag aggregate delta).
- Duplicate settlement: multiple settlement rows sharing the same tx_id.
- Orphan refund: settlement row whose tx_id does not exist in transactions.
"""

from __future__ import annotations

import pandas as pd
from datetime import date
from typing import Optional


class ReconciliationEngine:
    def __init__(self):
        self.transactions: Optional[pd.DataFrame] = None
        self.settlements:  Optional[pd.DataFrame] = None

    # ── Loading ───────────────────────────────────────────────────────────────

    def load(self, tx_path: str, settle_path: str) -> "ReconciliationEngine":
        """Load both CSVs and parse date columns."""
        self.transactions = pd.read_csv(tx_path, parse_dates=["tx_date"])
        self.settlements  = pd.read_csv(settle_path, parse_dates=["settle_date"])
        return self

    def load_dataframes(self, transactions: pd.DataFrame, settlements: pd.DataFrame) -> "ReconciliationEngine":
        """Accept DataFrames directly (used by tests)."""
        self.transactions = transactions.copy()
        self.settlements  = settlements.copy()
        # Ensure date columns are datetime
        if not pd.api.types.is_datetime64_any_dtype(self.transactions["tx_date"]):
            self.transactions["tx_date"] = pd.to_datetime(self.transactions["tx_date"])
        if not pd.api.types.is_datetime64_any_dtype(self.settlements["settle_date"]):
            self.settlements["settle_date"] = pd.to_datetime(self.settlements["settle_date"])
        return self

    # ── Gap detectors ─────────────────────────────────────────────────────────

    def find_unmatched_transactions(self) -> pd.DataFrame:
        """Transactions with no settlement at all (tx_id absent from settlements)."""
        settled_ids = set(self.settlements["tx_id"].unique())
        mask = ~self.transactions["tx_id"].isin(settled_ids)
        return self.transactions[mask].copy()

    def find_unmatched_settlements(self) -> pd.DataFrame:
        """Settlements whose tx_id does not exist in transactions (orphan refunds, etc.)."""
        tx_ids = set(self.transactions["tx_id"].unique())
        mask = ~self.settlements["tx_id"].isin(tx_ids)
        return self.settlements[mask].copy()

    def find_late_settlements(self, cutoff_date: date) -> pd.DataFrame:
        """Transactions that were settled after cutoff_date."""
        cutoff = pd.Timestamp(cutoff_date)
        # Join transactions with their settlements
        merged = self.transactions.merge(
            self.settlements[["tx_id", "settlement_id", "settle_date", "amount"]].rename(
                columns={"amount": "settled_amount"}
            ),
            on="tx_id",
            how="inner",
        )
        late = merged[merged["settle_date"] > cutoff].copy()
        late["days_late"] = (late["settle_date"] - cutoff).dt.days
        return late

    def find_duplicate_settlements(self) -> pd.DataFrame:
        """tx_ids that appear more than once in the settlements table."""
        counts = self.settlements.groupby("tx_id").size().reset_index(name="count")
        dup_ids = counts[counts["count"] > 1]["tx_id"]
        return self.settlements[self.settlements["tx_id"].isin(dup_ids)].copy()

    def find_rounding_differences(self, per_row_tolerance: float = 0.005, aggregate_tolerance: float = 0.005) -> dict:
        """
        Detect rounding gaps that are invisible per-row but visible in aggregate.

        Strategy:
        1. For each matched tx_id, compute |tx_amount - settle_amount| (per-row delta).
        2. Keep only rows where per-row delta <= per_row_tolerance (i.e. "looks fine individually").
        3. Sum all such tx_amounts and settle_amounts separately.
        4. If the aggregate delta exceeds aggregate_tolerance, flag it.

        This catches the pattern: 20 x ($100.001 tx vs $100.000 settle) = $0.02 aggregate gap
        even though no single row exceeds the $0.005 per-row threshold.
        """
        # Deduplicate settlements first (take first occurrence per tx_id for amount comparison)
        settle_dedup = self.settlements.drop_duplicates(subset=["tx_id"], keep="first")

        merged = self.transactions.merge(
            settle_dedup[["tx_id", "amount"]].rename(columns={"amount": "settle_amount"}),
            on="tx_id",
            how="inner",
        )
        merged["per_row_delta"] = (merged["amount"] - merged["settle_amount"]).abs()

        # Rows that look "clean" individually
        clean_rows = merged[merged["per_row_delta"] <= per_row_tolerance]

        tx_sum     = clean_rows["amount"].sum()
        settle_sum = clean_rows["settle_amount"].sum()
        agg_delta  = round(tx_sum - settle_sum, 6)

        return {
            "clean_row_count":  len(clean_rows),
            "tx_sum":           round(tx_sum, 4),
            "settle_sum":       round(settle_sum, 4),
            "aggregate_delta":  agg_delta,
            "gap_detected":     abs(agg_delta) > aggregate_tolerance,
            "detail":           clean_rows[["tx_id", "amount", "settle_amount", "per_row_delta"]],
        }

    def find_global_rounding_difference(self) -> dict:
        """Grand total comparison (convenience wrapper kept for backward compat)."""
        common_ids = set(self.transactions["tx_id"]) & set(self.settlements["tx_id"])
        tx_total     = self.transactions[self.transactions["tx_id"].isin(common_ids)]["amount"].sum()
        settle_total = self.settlements[self.settlements["tx_id"].isin(common_ids)]["amount"].sum()
        delta        = tx_total - settle_total
        return {
            "tx_total":     round(tx_total, 4),
            "settle_total": round(settle_total, 4),
            "delta":        round(delta, 4),
        }

    # ── Run all ───────────────────────────────────────────────────────────────

    def run_all(self, cutoff_date: date = date(2024, 12, 31)) -> dict:
        """Return a dict of every gap category."""
        return {
            "unmatched_transactions": self.find_unmatched_transactions(),
            "orphan_settlements":     self.find_unmatched_settlements(),
            "late_settlements":       self.find_late_settlements(cutoff_date),
            "duplicate_settlements":  self.find_duplicate_settlements(),
            "rounding_differences":   self.find_rounding_differences(),
            "global_rounding":        self.find_global_rounding_difference(),
        }


# ── CLI quick-check ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    engine = ReconciliationEngine().load("data/transactions.csv", "data/settlements.csv")
    results = engine.run_all()

    print("=== Reconciliation Report ===\n")

    print(f"Unmatched transactions : {len(results['unmatched_transactions'])}")
    print(results["unmatched_transactions"][["tx_id", "tx_date", "amount"]].to_string(index=False))

    print(f"\nOrphan settlements     : {len(results['orphan_settlements'])}")
    print(results["orphan_settlements"].to_string(index=False))

    print(f"\nLate settlements       : {len(results['late_settlements'])}")
    print(results["late_settlements"][["tx_id", "tx_date", "settle_date", "days_late"]].to_string(index=False))

    print(f"\nDuplicate settlements  : {len(results['duplicate_settlements'])} rows")
    print(results["duplicate_settlements"].to_string(index=False))

    rd = results["rounding_differences"]
    print(f"\nRounding gap (aggregate): TX sum={rd['tx_sum']}  Bank sum={rd['settle_sum']}  Delta={rd['aggregate_delta']}  Detected={rd['gap_detected']}")
