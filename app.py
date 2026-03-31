"""
Payment Reconciliation Dashboard
Streamlit app that loads generated transaction + settlement CSVs,
runs the ReconciliationEngine, and surfaces all 4 gap types.
"""

import streamlit as st
import pandas as pd
from datetime import date
from reconcile import ReconciliationEngine

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Payment Reconciliation",
    page_icon=":bank:",
    layout="wide",
)

st.title("Payment Reconciliation Dashboard")
st.caption("Month-end: December 31, 2024  |  Currency: USD")

# ── Load data ─────────────────────────────────────────────────────────────────

@st.cache_data
def load_and_reconcile():
    engine = ReconciliationEngine().load("data/transactions.csv", "data/settlements.csv")
    results = engine.run_all(cutoff_date=date(2024, 12, 31))
    tx  = engine.transactions
    stl = engine.settlements
    return engine, results, tx, stl

try:
    engine, results, tx, stl = load_and_reconcile()
except FileNotFoundError:
    st.error("data/transactions.csv or data/settlements.csv not found. Run `python generate_data.py` first.")
    st.stop()

# ── Summary metrics ───────────────────────────────────────────────────────────

late_df  = results["late_settlements"]
dup_df   = results["duplicate_settlements"]
orph_df  = results["orphan_settlements"]
rd       = results["rounding_differences"]
unmatched_df = results["unmatched_transactions"]

total_gaps = (
    len(late_df)
    + (len(dup_df) - dup_df["tx_id"].nunique())   # extra rows beyond first occurrence
    + len(orph_df)
    + (1 if rd["gap_detected"] else 0)
)

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Transactions",   len(tx))
col2.metric("Settlements",    len(stl))
col3.metric("Gaps Found",     total_gaps, delta_color="inverse")
col4.metric("Unmatched TXs",  len(unmatched_df))
col5.metric("Rounding Delta", f"${abs(rd['aggregate_delta']):.4f}")

st.divider()

# ── Assumptions sidebar ───────────────────────────────────────────────────────

with st.sidebar:
    st.header("Assumptions")
    st.markdown("""
- Platform records transactions instantly
- Bank settles 1–2 business days later
- Month-end cutoff: **Dec 31, 2024**
- Match = same `tx_id`
- Per-row amount tolerance: **$0.005**
- Aggregate rounding tolerance: **$0.005**
- Refunds are negative amounts
- Positive = original charge
""")
    st.header("Planted Gaps")
    st.markdown("""
1. **Late settlement** — TX-LATE-001 settled Jan 1
2. **Rounding diff** — 20 x $100.001 vs $100.00 (aggregate $0.02 off)
3. **Duplicate** — SET-0001 appears twice in bank feed
4. **Orphan refund** — REF-9999 has no matching transaction
""")

# ── GAP 1: Late Settlements ───────────────────────────────────────────────────

st.subheader("GAP 1 — Late Settlements")
st.caption(
    "Transactions where the bank settlement date falls **after** the Dec 31 cutoff. "
    "These are 'in-flight' — recorded in December but paid in January."
)

if late_df.empty:
    st.success("No late settlements found.")
else:
    display = late_df[["tx_id", "tx_date", "settle_date", "days_late", "amount"]].copy()
    display["tx_date"]     = display["tx_date"].dt.strftime("%Y-%m-%d")
    display["settle_date"] = display["settle_date"].dt.strftime("%Y-%m-%d")
    display.columns = ["Transaction ID", "TX Date", "Settlement Date", "Days Late", "Amount ($)"]
    st.dataframe(display, use_container_width=True, hide_index=True)
    st.warning(f"{len(late_df)} transaction(s) settled after month-end.")

st.divider()

# ── GAP 2: Rounding Differences ───────────────────────────────────────────────

st.subheader("GAP 2 — Rounding Difference (Aggregate)")
st.caption(
    "Individual transaction/settlement amounts differ by only $0.001 (below the $0.005 "
    "per-row tolerance), so each row looks matched. But across 20 rows the accumulated "
    "difference reaches **$0.02** — a classic fractional-cent / FX rounding pattern."
)

r1, r2, r3 = st.columns(3)
r1.metric("Platform Total (matched rows)", f"${rd['tx_sum']:,.4f}")
r2.metric("Bank Total (matched rows)",     f"${rd['settle_sum']:,.4f}")
r3.metric("Aggregate Delta",               f"${rd['aggregate_delta']:,.4f}",
          delta_color="inverse" if rd['gap_detected'] else "normal")

if rd["gap_detected"]:
    st.warning(f"Aggregate rounding gap detected: ${abs(rd['aggregate_delta']):.4f}")
    with st.expander("Show affected rows (per-row delta <= $0.005)"):
        detail = rd["detail"].copy()
        detail.columns = ["TX ID", "TX Amount ($)", "Settled Amount ($)", "Per-row Delta ($)"]
        st.dataframe(detail, use_container_width=True, hide_index=True)
else:
    st.success("No aggregate rounding gap detected.")

st.divider()

# ── GAP 3: Duplicate Settlements ─────────────────────────────────────────────

st.subheader("GAP 3 — Duplicate Settlements")
st.caption(
    "The bank feed contains more than one settlement row for the same transaction ID. "
    "One is legitimate; the rest are duplicates that would cause double-payment."
)

if dup_df.empty:
    st.success("No duplicate settlements found.")
else:
    dup_display = dup_df.copy()
    dup_display["settle_date"] = dup_display["settle_date"].dt.strftime("%Y-%m-%d")
    dup_display.columns = ["Settlement ID", "Transaction ID", "Settlement Date", "Amount ($)"]
    st.dataframe(dup_display, use_container_width=True, hide_index=True)

    extra_rows = len(dup_df) - dup_df["tx_id"].nunique() if "tx_id" in dup_df.columns else len(dup_df) - 1
    st.warning(f"{extra_rows} duplicate settlement row(s) found.")

st.divider()

# ── GAP 4: Orphan Settlements (no matching transaction) ───────────────────────

st.subheader("GAP 4 — Orphan Settlements / Unmatched Refunds")
st.caption(
    "Settlement rows whose transaction ID does not exist in the platform's records. "
    "Could be a manual bank-side reversal, a refund from a cancelled merchant account, "
    "or a data entry error."
)

if orph_df.empty:
    st.success("No orphan settlements found.")
else:
    orph_display = orph_df.copy()
    orph_display["settle_date"] = orph_display["settle_date"].dt.strftime("%Y-%m-%d")
    orph_display.columns = ["Settlement ID", "Transaction ID", "Settlement Date", "Amount ($)"]
    st.dataframe(orph_display, use_container_width=True, hide_index=True)
    st.error(f"{len(orph_df)} orphan settlement(s) — no matching transaction on platform.")

st.divider()

# ── Unmatched Transactions (bonus) ───────────────────────────────────────────

with st.expander("Bonus: Unmatched Transactions (TXs with no settlement at all)"):
    if unmatched_df.empty:
        st.success("All transactions have at least one settlement row.")
    else:
        st.dataframe(unmatched_df, use_container_width=True, hide_index=True)

# ── Raw data viewer ───────────────────────────────────────────────────────────

with st.expander("Raw Data — Transactions"):
    st.dataframe(tx, use_container_width=True, hide_index=True)

with st.expander("Raw Data — Settlements"):
    st.dataframe(stl, use_container_width=True, hide_index=True)

# ── Production limitations footer ────────────────────────────────────────────

st.divider()
st.subheader("What this would get wrong in production")
st.markdown("""
1. **ID-only matching fails at scale.** In production, platform and bank IDs often differ — banks use their own reference numbers. Real reconciliation requires fuzzy matching on amount + timestamp + merchant, not just a `tx_id` join.
2. **Fixed tolerance misses FX and tax rounding at volume.** A $0.005 threshold catches cent-level gaps but misses systematic FX conversion errors (e.g. 0.001% on $10M/day = $100/day undetected drift) or tax rounding that varies by jurisdiction.
3. **No partial/batch settlement support.** Banks frequently batch multiple transactions into one settlement line. This engine expects 1:1 matches, so any batched settlement would flag every constituent transaction as unmatched.
""")
