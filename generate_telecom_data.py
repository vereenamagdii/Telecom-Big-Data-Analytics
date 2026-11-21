#!/usr/bin/env python3
"""
Telecom Big Data Analytics Platform - Data Generator
----------------------------------------------------
Generates:
  1) telecom_cdr.csv      -> main events table (CDR + data + SMS + recharge)
  2) network_kpis.csv     -> Cell_ID / Date / Hour KPIs
  3) complaints.csv       -> customer complaints
  4) anomalies_truth.csv  -> ground truth of injected anomalies (for testing detection)

Usage:
  python generate_telecom_data.py --customers 10000 --days 7 --rows 1000000
  python generate_telecom_data.py --customers 2000 --days 3 --rows 100000 --out ./data

Requires: numpy, pandas
Units: Call_Duration = seconds | Data_Usage = MB | Recharge_Amount = EGP
"""
import argparse
import os
from datetime import date, timedelta

import numpy as np
import pandas as pd

PACKAGES = ["Basic", "Silver", "Gold", "Premium"]
PACKAGE_P = [0.40, 0.30, 0.20, 0.10]
# usage multiplier per package (higher package = heavier usage)
PACKAGE_MULT = {"Basic": 0.7, "Silver": 1.0, "Gold": 1.5, "Premium": 2.2}
RECHARGE_VALUES = [10, 20, 50, 100, 200]
# hourly activity weights: quiet at night, peaks 8-10 and 18-23
HOUR_W = np.array([
    1, 1, 0.5, 0.5, 0.5, 1, 2, 4,      # 00-07
    7, 8, 6, 5, 5, 5, 5, 5,            # 08-15
    6, 7, 9, 10, 10, 9, 8, 4           # 16-23
], dtype=float)
HOUR_W /= HOUR_W.sum()

COMPLAINT_CATEGORIES = ["Network", "Billing", "Data Speed", "Dropped Calls", "Package", "Other"]
COMPLAINT_STATUS = ["Open", "In Progress", "Resolved", "Closed"]

SPECIAL_ANOMALY_CUSTOMER = 10231  # the example from the project brief


def build_customers(n, n_cells, rng):
    ids = np.arange(10000, 10000 + n)
    prefixes = rng.choice(["10", "11", "12", "15"], size=n)
    tails = rng.integers(0, 10**8, size=n)
    msisdn = np.array([f"20{p}{t:08d}" for p, t in zip(prefixes, tails)])
    pkg = rng.choice(PACKAGES, size=n, p=PACKAGE_P)
    home_cell = rng.integers(1, n_cells + 1, size=n)
    # per-customer activity level (some customers are just heavier users)
    activity = rng.lognormal(mean=0.0, sigma=0.5, size=n)
    return pd.DataFrame({
        "Customer_ID": ids, "msisdn": msisdn, "Package": pkg,
        "home_cell": home_cell, "activity": activity,
    })


def generate_events(cust, n_rows, start_date, days, n_cells, rng):
    n = len(cust)
    # heavier customers appear more often
    p = cust["activity"].values / cust["activity"].values.sum()
    idx = rng.choice(n, size=n_rows, p=p)
    c = cust.iloc[idx].reset_index(drop=True)

    day_off = rng.integers(0, days, size=n_rows)
    hour = rng.choice(24, size=n_rows, p=HOUR_W)
    minute = rng.integers(0, 60, size=n_rows)
    second = rng.integers(0, 60, size=n_rows)

    dates = pd.to_datetime(start_date) + pd.to_timedelta(day_off, unit="D")

    # 85% of events in home cell, 15% roaming to a random cell
    roam = rng.random(n_rows) < 0.15
    cell_num = np.where(roam, rng.integers(1, n_cells + 1, size=n_rows), c["home_cell"].values)

    mult = c["Package"].map(PACKAGE_MULT).values * c["activity"].values

    # calls: 55% of events; lognormal ~ median 90s
    has_call = rng.random(n_rows) < 0.55
    call = np.clip(rng.lognormal(4.5, 0.9, n_rows), 5, 1800) * has_call
    # data: 70% of events; lognormal ~ median 40MB
    has_data = rng.random(n_rows) < 0.70
    data = np.clip(rng.lognormal(3.7, 1.3, n_rows) * mult, 0.1, 1500) * has_data
    # sms: 20% of events
    sms = rng.poisson(1.5, n_rows) * (rng.random(n_rows) < 0.20)
    # recharge: ~2% of events
    rech = np.where(rng.random(n_rows) < 0.02, rng.choice(RECHARGE_VALUES, n_rows), 0)

    return pd.DataFrame({
        "Customer_ID": c["Customer_ID"].values,
        "Date": dates.strftime("%Y-%m-%d"),
        "Time": [f"{h:02d}:{m:02d}:{s:02d}" for h, m, s in zip(hour, minute, second)],
        "Cell_ID": [f"CELL_{x:03d}" for x in cell_num],
        "msisdn": c["msisdn"].values,
        "Call_Duration": call.astype(int),
        "Data_Usage": np.round(data, 2),
        "SMS_Count": sms.astype(int),
        "Recharge_Amount": rech.astype(float),
        "Package": c["Package"].values,
    })


def inject_anomalies(df, cust, start_date, days, rate, rng):
    """Add abnormal rows for a small % of customers. Returns (df, truth_df)."""
    rows, truth = [], []
    n_anom = max(4, int(len(cust) * rate))
    chosen = rng.choice(cust["Customer_ID"].values, size=n_anom, replace=False)
    # make sure the project's example customer is included
    if SPECIAL_ANOMALY_CUSTOMER in cust["Customer_ID"].values and SPECIAL_ANOMALY_CUSTOMER not in chosen:
        chosen[0] = SPECIAL_ANOMALY_CUSTOMER
    cust_idx = cust.set_index("Customer_ID")

    types = ["DATA_SPIKE", "LONG_CALL", "SMS_SPAM", "RECHARGE_BURST"]
    for k, cid in enumerate(chosen):
        atype = "DATA_SPIKE" if cid == SPECIAL_ANOMALY_CUSTOMER else types[k % 4]
        info = cust_idx.loc[cid]
        d = (pd.to_datetime(start_date) + timedelta(days=int(rng.integers(0, days)))).strftime("%Y-%m-%d")
        h = int(rng.integers(0, 24))
        cell = f"CELL_{int(info['home_cell']):03d}"

        def row(minute, sec, call=0, data=0.0, sms=0, rech=0.0):
            return {"Customer_ID": cid, "Date": d, "Time": f"{h:02d}:{minute:02d}:{sec:02d}",
                    "Cell_ID": cell, "msisdn": info["msisdn"], "Call_Duration": call,
                    "Data_Usage": data, "SMS_Count": sms, "Recharge_Amount": rech,
                    "Package": info["Package"]}

        if atype == "DATA_SPIKE":       # ~25 GB within one hour
            for m in range(0, 60, 6):
                rows.append(row(m, int(rng.integers(0, 60)), data=round(float(rng.uniform(2200, 2900)), 2)))
        elif atype == "LONG_CALL":      # 5-hour call
            rows.append(row(int(rng.integers(0, 60)), 0, call=int(rng.integers(16000, 20000))))
        elif atype == "SMS_SPAM":       # ~500 SMS in one hour
            for m in range(0, 60, 2):
                rows.append(row(m, int(rng.integers(0, 60)), sms=int(rng.integers(12, 20))))
        else:                           # 10 recharges in a day
            for m in range(10):
                rows.append(row(m * 5, int(rng.integers(0, 60)), rech=float(rng.choice(RECHARGE_VALUES))))
        truth.append({"Customer_ID": cid, "Date": d, "Hour": h, "Anomaly_Type": atype})

    df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    return df, pd.DataFrame(truth)


def make_dirty(df, ratio, rng):
    """Add ~ratio of bad rows: nulls, duplicates, negatives, wrong date format."""
    n = len(df)
    k = max(1, int(n * ratio / 4))
    df = df.copy()
    df["Data_Usage"] = df["Data_Usage"].astype(object)
    df["Date"] = df["Date"].astype(object)

    # 1) nulls
    ix = rng.choice(n, k, replace=False)
    df["Customer_ID"] = df["Customer_ID"].astype("Int64")  # nullable int (no 10231.0)
    df.loc[ix, "Customer_ID"] = pd.NA
    ix = rng.choice(n, k, replace=False)
    df.loc[ix, "Cell_ID"] = None
    # 2) negative / illogical values
    ix = rng.choice(n, k, replace=False)
    df.loc[ix, "Data_Usage"] = -abs(pd.to_numeric(df.loc[ix, "Data_Usage"], errors="coerce").fillna(1)) - 1
    # 3) wrong date format (DD/MM/YYYY)
    ix = rng.choice(n, k, replace=False)
    df.loc[ix, "Date"] = pd.to_datetime(df.loc[ix, "Date"]).dt.strftime("%d/%m/%Y")
    # 4) duplicates
    dups = df.sample(k, random_state=int(rng.integers(0, 10**6)))
    df = pd.concat([df, dups], ignore_index=True)
    return df


def build_network_kpis(n_cells, start_date, days, rng):
    rows = []
    base = pd.to_datetime(start_date)
    for cell in range(1, n_cells + 1):
        quality = rng.uniform(0.5, 1.5)  # bad cells have higher drop rate/latency
        for d in range(days):
            for h in range(24):
                load = HOUR_W[h] * 24
                rows.append({
                    "Cell_ID": f"CELL_{cell:03d}",
                    "Date": (base + timedelta(days=d)).strftime("%Y-%m-%d"),
                    "Hour": h,
                    "Dropped_Call_Rate": round(float(np.clip(rng.normal(1.0 * quality * (0.6 + load / 30), 0.3), 0, 15)), 2),
                    "Latency_ms": round(float(np.clip(rng.normal(35 * quality * (0.7 + load / 25), 6), 5, 400)), 1),
                    "Throughput_Mbps": round(float(np.clip(rng.normal(45 / quality / (0.7 + load / 40), 5), 1, 150)), 1),
                })
    return pd.DataFrame(rows)


def build_complaints(cust, n, start_date, days, rng):
    ids = rng.choice(cust["Customer_ID"].values, size=n)
    base = pd.to_datetime(start_date)
    return pd.DataFrame({
        "Complaint_ID": np.arange(1, n + 1),
        "Customer_ID": ids,
        "Date": [(base + timedelta(days=int(x))).strftime("%Y-%m-%d") for x in rng.integers(0, days, n)],
        "Category": rng.choice(COMPLAINT_CATEGORIES, n, p=[0.30, 0.20, 0.20, 0.15, 0.10, 0.05]),
        "Status": rng.choice(COMPLAINT_STATUS, n, p=[0.2, 0.2, 0.3, 0.3]),
    })


def main():
    ap = argparse.ArgumentParser(description="Telecom data generator")
    ap.add_argument("--customers", type=int, default=10000)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--rows", type=int, default=1_000_000, help="normal event rows (anomalies added on top)")
    ap.add_argument("--cells", type=int, default=50)
    ap.add_argument("--anomaly-rate", type=float, default=0.003, help="fraction of customers with anomalies")
    ap.add_argument("--dirty-ratio", type=float, default=0.02, help="fraction of dirty rows")
    ap.add_argument("--start-date", default=(date.today() - timedelta(days=7)).isoformat())
    ap.add_argument("--out", default="./data")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--no-dirty", action="store_true", help="skip dirty data injection")
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    os.makedirs(args.out, exist_ok=True)

    print(f"[1/5] Building {args.customers} customers ...")
    cust = build_customers(args.customers, args.cells, rng)

    print(f"[2/5] Generating {args.rows:,} events ...")
    df = generate_events(cust, args.rows, args.start_date, args.days, args.cells, rng)

    print("[3/5] Injecting anomalies ...")
    df, truth = inject_anomalies(df, cust, args.start_date, args.days, args.anomaly_rate, rng)

    # sort by date/time so it looks like a real stream
    df = df.sort_values(["Date", "Time"]).reset_index(drop=True)

    if not args.no_dirty:
        print("[4/5] Adding dirty data ...")
        df = make_dirty(df, args.dirty_ratio, rng)
    else:
        print("[4/5] Skipping dirty data")

    print("[5/5] Writing files ...")
    df.to_csv(os.path.join(args.out, "telecom_cdr.csv"), index=False)
    truth.to_csv(os.path.join(args.out, "anomalies_truth.csv"), index=False)
    build_network_kpis(args.cells, args.start_date, args.days, rng).to_csv(
        os.path.join(args.out, "network_kpis.csv"), index=False)
    build_complaints(cust, max(50, args.customers // 20), args.start_date, args.days, rng).to_csv(
        os.path.join(args.out, "complaints.csv"), index=False)

    print(f"\nDone. Files in: {os.path.abspath(args.out)}")
    print(f"  telecom_cdr.csv     : {len(df):,} rows")
    print(f"  anomalies_truth.csv : {len(truth)} injected anomalies")
    print(f"  Customer {SPECIAL_ANOMALY_CUSTOMER} (25GB/hour example) included: "
          f"{SPECIAL_ANOMALY_CUSTOMER in truth['Customer_ID'].values}")


if __name__ == "__main__":
    main()