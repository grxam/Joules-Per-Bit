import csv
import re
from pathlib import Path
from statistics import mean

# ----------------------------
# CONFIG (minimal change)
# ----------------------------
LOGS_DIR = Path("logs")          # where run_*.csv and summary_*.txt live
OUTPUT_CSV = LOGS_DIR / "aggregate_results.csv"

SUMMARY_RE = re.compile(r"^summary_(?P<run_id>.+?)_(?P<mode>A2B|B2A|BOTH)\.txt$")
POWER_RE   = re.compile(r"^run_(?P<run_id>.+?)_(?P<mode>A2B|B2A|BOTH)\.csv$")

FLOAT_RE = re.compile(r"([-+]?\d+(?:\.\d+)?)")


# ----------------------------
# SUMMARY PARSING
# ----------------------------
def _extract_float(line: str):
    m = FLOAT_RE.search(line)
    return float(m.group(1)) if m else None


def parse_summary(path: Path) -> dict:
    """
    Parses your experiment_protocol.py summary file format.

    Extracts:
      H_before / H_after / ΔH for A→B and B→A blocks (if present)
      Order Effect line (if present)
    """
    m = SUMMARY_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected summary filename: {path.name}")

    run_id = m.group("run_id")
    mode = m.group("mode")

    out = {
        "run_id": run_id,
        "mode": mode,
        "H_before_A2B_bits": None,
        "H_after_A2B_bits": None,
        "delta_H_A2B_bits": None,
        "H_before_B2A_bits": None,
        "H_after_B2A_bits": None,
        "delta_H_B2A_bits": None,
        "order_effect_bits": None,
        "summary_file": str(path),
    }

    text = path.read_text(encoding="utf-8", errors="replace").splitlines()

    section = None  # None, "A2B", "B2A"
    for line in text:
        line_stripped = line.strip()

        if line_stripped == "A → B":
            section = "A2B"
            continue
        if line_stripped == "B → A":
            section = "B2A"
            continue

        if line_stripped.startswith("H_before:"):
            v = _extract_float(line_stripped)
            if section == "A2B":
                out["H_before_A2B_bits"] = v
            elif section == "B2A":
                out["H_before_B2A_bits"] = v

        elif line_stripped.startswith("H_after:"):
            v = _extract_float(line_stripped)
            if section == "A2B":
                out["H_after_A2B_bits"] = v
            elif section == "B2A":
                out["H_after_B2A_bits"] = v

        elif line_stripped.startswith("ΔH:"):
            v = _extract_float(line_stripped)
            if section == "A2B":
                out["delta_H_A2B_bits"] = v
            elif section == "B2A":
                out["delta_H_B2A_bits"] = v

        elif line_stripped.startswith("Order Effect"):
            out["order_effect_bits"] = _extract_float(line_stripped)

    # If order effect not printed (e.g., only one direction ran), compute if possible
    if out["order_effect_bits"] is None:
        dA = out["delta_H_A2B_bits"]
        dB = out["delta_H_B2A_bits"]
        if dA is not None and dB is not None:
            out["order_effect_bits"] = dA - dB

    return out


# ----------------------------
# POWER CSV PARSING
# ----------------------------
def _pick_col(headers, must_contain_any, prefer_contain_any=()):
    """
    Choose a column name from headers that contains any token in must_contain_any.
    If prefer_contain_any provided, prefer those matches.
    """
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
    """
    Robustly parses Intel Power Gadget PowerLog CSV.

    Returns:
      avg_power_W: mean of selected power column
      duration_s: last - first of selected time column (if present)
      energy_J: integrates power over time if possible, else avg_power_W * duration_s
    """
    m = POWER_RE.match(path.name)
    if not m:
        raise ValueError(f"Unexpected powerlog filename: {path.name}")

    run_id = m.group("run_id")
    mode = m.group("mode")

    out = {
        "run_id": run_id,
        "mode": mode,
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

        # Time column heuristics
        time_col = _pick_col(
            headers,
            must_contain_any=("elapsed", "time"),
            prefer_contain_any=("elapsed time", "elapsed"),
        )

        # Power column heuristics (prefer "package"/"processor" if present)
        power_col = _pick_col(
            headers,
            must_contain_any=("power", "watt"),
            prefer_contain_any=("package", "processor", "cpu"),
        )

        out["power_col_used"] = power_col
        out["time_col_used"] = time_col

        times = []
        powers = []

        for row in reader:
            if power_col is not None:
                try:
                    p = float(row[power_col])
                except (ValueError, TypeError, KeyError):
                    p = None
            else:
                p = None

            if time_col is not None:
                try:
                    t = float(row[time_col])
                except (ValueError, TypeError, KeyError):
                    t = None
            else:
                t = None

            if p is not None:
                powers.append(p)
            if t is not None:
                times.append(t)

    if powers:
        out["avg_power_W"] = mean(powers)

    if len(times) >= 2:
        out["duration_s"] = times[-1] - times[0]

    # Energy estimate:
    # If we have both time + power samples, do rectangle integration:
    #   E ≈ Σ p[i] * (t[i]-t[i-1]) for i>=1
    if len(times) >= 2 and len(powers) >= 2:
        # Align by length if one list got fewer valid parses
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


# ----------------------------
# MAIN AGGREGATION
# ----------------------------
def main():
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    summary_files = sorted(LOGS_DIR.glob("summary_*_*.txt"))
    power_files = {p.name: p for p in LOGS_DIR.glob("run_*_*.csv")}

    rows = []

    for sfile in summary_files:
        s = parse_summary(sfile)
        expected_power_name = f"run_{s['run_id']}_{s['mode']}.csv"
        pfile = power_files.get(expected_power_name)

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

        rows.append({
            "run_id": s["run_id"],
            "mode": s["mode"],

            "energy_J": p["energy_J"],
            "avg_power_W": p["avg_power_W"],
            "duration_s": p["duration_s"],

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

    # Write aggregated CSV
    fieldnames = [
        "run_id", "mode",
        "energy_J", "avg_power_W", "duration_s",
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


