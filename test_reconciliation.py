"""
Test suite for ReconciliationEngine.

Each test is self-contained — it builds minimal DataFrames directly
rather than reading from disk, so tests never depend on generated CSVs.
"""

import pytest
import pandas as pd
from datetime import date
from reconcile import ReconciliationEngine


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_tx(rows):
    """rows: list of (tx_id, tx_date_str, amount)"""
    return pd.DataFrame(rows, columns=["tx_id", "tx_date", "amount"])


def make_settle(rows):
    """rows: list of (settlement_id, tx_id, settle_date_str, amount)"""
    return pd.DataFrame(rows, columns=["settlement_id", "tx_id", "settle_date", "amount"])


def engine(txs, settles):
    return ReconciliationEngine().load_dataframes(make_tx(txs), make_settle(settles))


# ── Test 1: Clean data → zero gaps ───────────────────────────────────────────

def test_no_gaps_clean_data():
    """Perfectly matched dataset should return empty gap tables."""
    txs = [
        ("TX-001", "2024-12-01", 100.00),
        ("TX-002", "2024-12-02", 250.50),
        ("TX-003", "2024-12-03", 75.25),
    ]
    settles = [
        ("SET-001", "TX-001", "2024-12-02", 100.00),
        ("SET-002", "TX-002", "2024-12-03", 250.50),
        ("SET-003", "TX-003", "2024-12-04", 75.25),
    ]
    e = engine(txs, settles)

    assert len(e.find_unmatched_transactions()) == 0, "No unmatched transactions expected"
    assert len(e.find_unmatched_settlements()) == 0,  "No orphan settlements expected"
    assert len(e.find_late_settlements(date(2024, 12, 31))) == 0, "No late settlements expected"
    assert len(e.find_duplicate_settlements()) == 0,  "No duplicates expected"

    rd = e.find_rounding_differences()
    assert not rd["gap_detected"], "No rounding gap expected"


# ── Test 2: Late settlement ───────────────────────────────────────────────────

def test_detects_late_settlement():
    """TX settled after Dec 31 cutoff must appear in late settlements."""
    txs = [
        ("TX-LATE", "2024-12-31", 500.00),
        ("TX-OK",   "2024-12-15", 200.00),
    ]
    settles = [
        ("SET-LATE", "TX-LATE", "2025-01-01", 500.00),   # next month
        ("SET-OK",   "TX-OK",   "2024-12-16", 200.00),   # on time
    ]
    e = engine(txs, settles)
    late = e.find_late_settlements(date(2024, 12, 31))

    assert len(late) == 1
    assert late.iloc[0]["tx_id"] == "TX-LATE"
    assert late.iloc[0]["days_late"] == 1


# ── Test 3: Rounding difference ───────────────────────────────────────────────

def test_detects_rounding_diff():
    """
    20 rows where tx_amount=$100.001 and settle_amount=$100.00.
    Per-row delta=$0.001 < $0.005 tolerance -> each row looks fine.
    Aggregate delta=$0.020 > $0.005 -> must be flagged.
    """
    txs = [(f"TX-R{i:02d}", "2024-12-15", 100.001) for i in range(20)]
    settles = [(f"SET-R{i:02d}", f"TX-R{i:02d}", "2024-12-16", 100.00) for i in range(20)]

    e = engine(txs, settles)
    rd = e.find_rounding_differences(per_row_tolerance=0.005, aggregate_tolerance=0.005)

    assert rd["gap_detected"], "Aggregate rounding gap should be detected"
    assert abs(rd["aggregate_delta"] - 0.02) < 0.0001, f"Expected delta ~0.02, got {rd['aggregate_delta']}"
    assert rd["clean_row_count"] == 20, "All 20 rows should pass per-row check"


# ── Test 4: Duplicate settlement ─────────────────────────────────────────────

def test_detects_duplicate():
    """Same tx_id with two settlement rows must be flagged."""
    txs = [("TX-001", "2024-12-01", 300.00)]
    settles = [
        ("SET-001",     "TX-001", "2024-12-02", 300.00),
        ("SET-001-DUP", "TX-001", "2024-12-02", 300.00),   # duplicate
    ]
    e = engine(txs, settles)
    dups = e.find_duplicate_settlements()

    assert len(dups) == 2, "Both rows of the duplicate should be returned"
    assert set(dups["tx_id"]) == {"TX-001"}


# ── Test 5: Orphan refund ─────────────────────────────────────────────────────

def test_detects_orphan_refund():
    """Settlement with a tx_id that has no matching transaction must be flagged."""
    txs = [("TX-001", "2024-12-01", 150.00)]
    settles = [
        ("SET-001", "TX-001",  "2024-12-02", 150.00),
        ("REF-999", "TX-9999", "2024-12-10", -150.00),   # orphan refund
    ]
    e = engine(txs, settles)
    orphans = e.find_unmatched_settlements()

    assert len(orphans) == 1
    assert orphans.iloc[0]["settlement_id"] == "REF-999"
    assert orphans.iloc[0]["amount"] == -150.00


# ── Test 6: Full generated dataset has exactly 4 gap categories ───────────────

def test_full_dataset_gap_count():
    """
    Run against the actual generated CSVs and assert all 4 gap types are present.
    Requires generate_data.py to have been run first.
    """
    import os
    if not os.path.exists("data/transactions.csv"):
        pytest.skip("Generated data not found — run python generate_data.py first")

    e = ReconciliationEngine().load("data/transactions.csv", "data/settlements.csv")
    results = e.run_all(cutoff_date=date(2024, 12, 31))

    # GAP 1: at least one late settlement
    assert len(results["late_settlements"]) >= 1, "Expected at least 1 late settlement"

    # GAP 2: rounding gap detected
    assert results["rounding_differences"]["gap_detected"], "Expected rounding gap to be detected"

    # GAP 3: duplicate settlements present
    assert len(results["duplicate_settlements"]) >= 2, "Expected duplicate settlement rows"

    # GAP 4: orphan refund present
    orphans = results["orphan_settlements"]
    assert len(orphans) >= 1, "Expected at least 1 orphan settlement"
    assert (orphans["amount"] < 0).any(), "Expected orphan to be a refund (negative amount)"
