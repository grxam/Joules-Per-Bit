"""
aggregate_results.py

Aggregates per-run entropy summaries and external power telemetry logs into
a single analysis-ready CSV file.

This script performs the following steps:
    1. Reads per-run summary CSV files produced by experiment_protocol.py
       (summary_<RUN_ID>_<MODE>.csv)
    2. Matches each summary file to a corresponding Intel Power Gadget log
       (run_<RUN_ID>_<MODE>.csv), if available
    3. Parses power telemetry to compute:
           - average power (W)
           - run duration (s)
           - total energy (J)
    4. Optionally loads an idle baseline (logs/idle.csv) and subtracts it to
       compute net (idle-subtracted) power and energy
    5. Writes a consolidated CSV containing one row per run

Power logs are parsed defensively using heuristic column detection to support
variations in Intel Power Gadget CSV formats. Energy is computed by numerical
integration of power over time when possible, with a fallback estimate using
average power multiplied by duration.

If no idle baseline is found, net power and net energy fields are left blank
and a warning is emitted.

Outputs:
    logs/aggregate_results.csv

Assumptions and limitations:
    - Idle baseline represents steady-state system power under similar
      conditions (same power plan, minimal background load)
    - Sampling rates between run logs and idle logs may differ; idle subtraction
      uses average idle power multiplied by run duration
    - This script does not validate experimental correctness, only aggregates
      available data

Intended use:
    Post-processing and analysis of entropy and power measurements for
    order-dependent forced-token intervention experiments.
"""

import csv
import re
from pathlib import Path
from statistics import mean

RAW_DIR = Path("data/raw")
SUMMARY_DIR = RAW_DIR / "summaries"
POWER_DIR = RAW_DIR / "power"

AGG_DIR = Path("data/aggregate")
OUTPUT_CSV = AGG_DIR / "aggregate_results.csv"


SUMMARY_RE = re.compile(r"^summary_(?P<run_id>.+?)_(?P<mode>A2B|B2A|BOTH)\.csv$")
POWER_RE   = re.compile(r"^run_(?P<run_id>.+?)_(?P<mode>A2B|B2A|BOTH)\.csv$")


def parse_summary_csv(path: Path) -> dict:
    m = SUMMARY_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected summary filename: {path.name}")

    run_id = m.group("run_id")
    mode = m.group("mode")

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        row = next(reader, None)

    if row is None:
        raise ValueError(f"Empty summary file: {path}")

    def fnum(x):
        if x is None:
            return None
        x = str(x).strip()
        if x == "" or x.lower() == "none":
            return None
        return float(x)

    out = {
        "run_id": run_id,
        "mode": mode,
        "summary_file": str(path),

        "H_before_A2B_bits": fnum(row.get("H_before_A2B_bits")),
        "H_after_A2B_bits": fnum(row.get("H_after_A2B_bits")),
        "delta_H_A2B_bits": fnum(row.get("delta_H_A2B_bits")),

        "H_before_B2A_bits": fnum(row.get("H_before_B2A_bits")),
        "H_after_B2A_bits": fnum(row.get("H_after_B2A_bits")),
        "delta_H_B2A_bits": fnum(row.get("delta_H_B2A_bits")),

        "order_effect_bits": fnum(row.get("order_effect_bits")),
    }

    # Compute order effect if missing and deltas exist
    if out["order_effect_bits"] is None:
        dA = out["delta_H_A2B_bits"]
        dB = out["delta_H_B2A_bits"]
        if dA is not None and dB is not None:
            out["order_effect_bits"] = dA - dB

    return out


def _pick_col(headers, must_contain_any, prefer_contain_any=()):
    h_lower = {h: h.lower() for h in headers}
    candidates = [h for h in headers if any(tok in h_lower[h] for tok in must_contain_any)]
    if not candidates:
        return None
    if prefer_contain_any:
        preferred = [h for h in candidates if any(tok in h_lower[h] for tok in prefer_contain_any)]
        if preferred:
            return preferred[0]
    return candidates[0]


def parse_power_csv(path: Path) -> dict:

    out = {
        "avg_power_W": None,
        "duration_s": None,
        "energy_J": None,
        "powerlog_file": str(path),
        "power_col_used": None,
        "time_col_used": None,
    }

    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return out

        headers = reader.fieldnames

        time_col = _pick_col(
            headers,
            must_contain_any=("elapsed", "time"),
            prefer_contain_any=("elapsed time", "elapsed"),
        )

        power_col = _pick_col(
            headers,
            must_contain_any=("power", "watt"),
            prefer_contain_any=("package", "processor", "cpu"),
        )

        out["power_col_used"] = power_col
        out["time_col_used"] = time_col

        times, powers = [], []

        for row in reader:
            t = None
            p = None

            if time_col is not None:
                try:
                    t = float(row[time_col])
                except Exception:
                    t = None

            if power_col is not None:
                try:
                    p = float(row[power_col])
                except Exception:
                    p = None

            if t is not None:
                times.append(t)
            if p is not None:
                powers.append(p)

    if powers:
        out["avg_power_W"] = mean(powers)

    if len(times) >= 2:
        out["duration_s"] = times[-1] - times[0]

    # Energy estimate by integrating power over time if possible
    if len(times) >= 2 and len(powers) >= 2:
        n = min(len(times), len(powers))
        times = times[:n]
        powers = powers[:n]

        energy = 0.0
        for i in range(1, n):
            dt = times[i] - times[i - 1]
            if dt > 0:
                energy += powers[i] * dt
        out["energy_J"] = energy

    # Fallback energy estimate
    if out["energy_J"] is None and out["avg_power_W"] is not None and out["duration_s"] is not None:
        out["energy_J"] = out["avg_power_W"] * out["duration_s"]

    return out


def main():
    AGG_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # ---- NEW: load idle baseline ----
    idle_path = POWER_DIR / "idle.csv"
    idle_avg_power_W = None
    if idle_path.exists():
        idle = parse_power_csv(idle_path)
        idle_avg_power_W = idle.get("avg_power_W")
    else:
        print("WARNING: logs/idle.csv not found. Net power/energy will be blank.")

    summary_files = sorted(SUMMARY_DIR.glob("summary_*_*.csv"))
    power_files = {p.name: p for p in POWER_DIR.glob("run_*_*.csv")}


    rows = []

    for sfile in summary_files:
        s = parse_summary_csv(sfile)
        expected_power = f"run_{s['run_id']}_{s['mode']}.csv"
        pfile = power_files.get(expected_power)

        if pfile is not None:
            p = parse_power_csv(pfile)
        else:
            p = {
                "avg_power_W": None,
                "duration_s": None,
                "energy_J": None,
                "powerlog_file": None,
                "power_col_used": None,
                "time_col_used": None,
            }

        # ---- NEW: compute net (idle-subtracted) metrics ----
        net_avg_power_W = None
        net_energy_J = None

        if (p["avg_power_W"] is not None) and (idle_avg_power_W is not None):
            net_avg_power_W = p["avg_power_W"] - idle_avg_power_W

        if (p["energy_J"] is not None) and (idle_avg_power_W is not None) and (p["duration_s"] is not None):
            net_energy_J = p["energy_J"] - (idle_avg_power_W * p["duration_s"])

        rows.append({
            "run_id": s["run_id"],
            "mode": s["mode"],

            "idle_avg_power_W": idle_avg_power_W,
            "energy_J": p["energy_J"],
            "avg_power_W": p["avg_power_W"],
            "duration_s": p["duration_s"],

            "net_energy_J": net_energy_J,
            "net_avg_power_W": net_avg_power_W,

            "H_before_A2B_bits": s["H_before_A2B_bits"],
            "H_after_A2B_bits": s["H_after_A2B_bits"],
            "delta_H_A2B_bits": s["delta_H_A2B_bits"],

            "H_before_B2A_bits": s["H_before_B2A_bits"],
            "H_after_B2A_bits": s["H_after_B2A_bits"],
            "delta_H_B2A_bits": s["delta_H_B2A_bits"],

            "order_effect_bits": s["order_effect_bits"],

            "summary_file": s["summary_file"],
            "powerlog_file": p["powerlog_file"],
            "power_col_used": p["power_col_used"],
            "time_col_used": p["time_col_used"],
        })

    fieldnames = [
        "run_id", "mode",
        "idle_avg_power_W",
        "energy_J", "avg_power_W", "duration_s",
        "net_energy_J", "net_avg_power_W",
        "H_before_A2B_bits", "H_after_A2B_bits", "delta_H_A2B_bits",
        "H_before_B2A_bits", "H_after_B2A_bits", "delta_H_B2A_bits",
        "order_effect_bits",
        "summary_file", "powerlog_file", "power_col_used", "time_col_used",
    ]

    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Wrote {len(rows)} rows to: {OUTPUT_CSV}")



if __name__ == "__main__":
    main()







