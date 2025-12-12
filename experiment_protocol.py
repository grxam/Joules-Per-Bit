"""
experiment_protocol.py

Implements an order-dependent forced-token intervention protocol for
autoregressive language models using llama-cpp-python.

This script measures how the entropy of a model’s next-token probability
distribution changes when a specific token is forcibly injected into the
generation context, and how that change depends on the order of intervention.
Two execution orders are supported:

    1. A to B: measure baseline entropy, force token A, then re-measure entropy
    2. B to A: measure baseline entropy, force token B, then re-measure entropy

For each order, the script computes:
    - H_before: Shannon entropy (in bits) of the next-token distribution
      prior to any intervention
    - H_after: Shannon entropy (in bits) after the forced-token intervention
    - dH: entropy difference (H_after − H_before)

When both orders are executed, an order-effect metric is computed as:
    order_effect = dH_A→B − dH_B→A

Entropy is computed from the model’s top-K log-probabilities using natural
logarithms converted to bits.

The script produces one CSV summary file per run containing entropy metrics
and minimal sanity-check information (forced token and top token after forcing).
Power and energy measurements are not collected here and are expected to be
captured externally (e.g., via Intel Power Gadget).

Key characteristics:
    - CPU-only execution
    - Deterministic probing (temperature = 0)
    - Explicit logit bias for forced-token insertion
    - Order-separated experimental runs for reproducibility

Outputs:
    logs/summary_<RUN_ID>_<MODE>.csv

Intended use:
    Controlled experiments studying order effects, intervention asymmetry,
    and entropy changes in large language models under forced-token conditions.
"""
from llama_cpp import Llama
import math
import os
import csv
import argparse
from pathlib import Path

# ================================================================
# ARGUMENTS
# ================================================================
parser = argparse.ArgumentParser(description="Order-dependent forced-token intervention protocol (AB, BA, BOTH).")
parser.add_argument("--run-id", required=True, help="Unique identifier for this run (used in output filename).")
parser.add_argument("--mode", choices=["A2B", "B2A", "BOTH"], default="BOTH", help="Which protocol direction(s) to run.")
parser.add_argument(
    "--model-path",
    default=os.environ.get("LLAMA_MODEL_PATH", ""),
    help="Path to a local GGUF model file. Can also be set via env var LLAMA_MODEL_PATH.",
)
parser.add_argument(
    "--out-dir",
    default=os.environ.get("LLAMA_OUT_DIR", "logs"),
    help="Directory for output summaries (default: ./logs). Can also be set via env var LLAMA_OUT_DIR.",
)
parser.add_argument("--ctx", type=int, default=4096, help="Context length (default: 4096).")
parser.add_argument("--threads", type=int, default=os.cpu_count() or 1, help="Number of CPU threads (default: CPU count).")
parser.add_argument("--system-msg", default="Reply with exactly one token each time.", help="System message string.")
parser.add_argument("--token-a", default=" Yes", help='Token A text (default: " Yes"). Leading space is intentional.')
parser.add_argument("--token-b", default=" No", help='Token B text (default: " No"). Leading space is intentional.')
parser.add_argument("--topn", type=int, default=10, help="How many top tokens to print in the summary (default: 10).")
parser.add_argument("--logprobs", type=int, default=50, help="How many logprobs to request on probes (default: 50).")
args = parser.parse_args()

RUN_ID = args.run_id
MODE = args.mode
MODEL_PATH = args.model_path
OUT_DIR = Path(args.out_dir)
SYSTEM_MSG = args.system_msg
TOKEN_A_TEXT = args.token_a
TOKEN_B_TEXT = args.token_b
TOPN = args.topn
LOGPROBS_N = args.logprobs

if not MODEL_PATH:
    raise SystemExit(
        "ERROR: --model-path is required (or set env var LLAMA_MODEL_PATH). "
        "Example: --model-path /path/to/model.gguf"
    )

# Ensure output directory exists (safe to create; keep it gitignored)
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ================================================================
# LOAD MODEL
# ================================================================
llm = Llama(
    model_path=MODEL_PATH,
    n_threads=args.threads,
    n_ctx=args.ctx,
    logits_all=True,
)

# ================================================================
# TOKEN IDs
# ================================================================
A_id = llm.tokenize(TOKEN_A_TEXT.encode("utf-8"), add_bos=False)[0]
B_id = llm.tokenize(TOKEN_B_TEXT.encode("utf-8"), add_bos=False)[0]

# ================================================================
# HELPERS
# ================================================================
def build_prompt() -> str:
    return f"System: {SYSTEM_MSG}\nUser: Continue.\nAssistant:"


def entropy_bits(top_logprobs: dict) -> float:
    """
    Compute Shannon entropy in bits from llama_cpp logprobs.
    top_logprobs: dict {token_str: ln(p)}
    """
    H_nats = 0.0
    for _, lp in top_logprobs.items():
        p = math.exp(lp)
        if p > 0.0:
            H_nats -= p * lp
    return H_nats / math.log(2.0)


def unbiased_probe(prompt: str):
    """
    Returns:
      H (entropy in bits),
      sampled_token,
      top_items = [(token_str, prob, logprob), ...] sorted by prob
    """
    r = llm.create_completion(
        prompt=prompt,
        max_tokens=1,
        temperature=0.0,
        logprobs=LOGPROBS_N,
        top_k=LOGPROBS_N,
    )
    c = r["choices"][0]
    top_logprobs = c["logprobs"]["top_logprobs"][0]  # dict token->ln(p)

    top_items = sorted(
        ((tok, math.exp(lp), lp) for tok, lp in top_logprobs.items()),
        key=lambda x: x[1],
        reverse=True,
    )

    H = entropy_bits(top_logprobs)
    return H, c["text"], top_items


def force_token(prompt: str, tokid: int) -> str:
    """
    Force a single token by applying a very large positive logit bias.
    """
    r = llm.create_completion(
        prompt=prompt,
        max_tokens=1,
        temperature=0.0,
        logit_bias={tokid: +100},
        logprobs=1,
    )
    return r["choices"][0]["text"]


def write_summary(run_id, mode, A_res, B_res, out_path: Path):
    (HbA, HaA, dA, tokA, distA) = A_res
    (HbB, HaB, dB, tokB, distB) = B_res

    order_effect = None
    if (dA is not None) and (dB is not None):
        order_effect = dA - dB

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Optional: include top token after forcing as quick sanity signal
    top_tok_A = distA[0][0] if distA else None
    top_p_A   = distA[0][1] if distA else None

    top_tok_B = distB[0][0] if distB else None
    top_p_B   = distB[0][1] if distB else None

    fieldnames = [
        "run_id", "mode",

        "H_before_A2B_bits", "H_after_A2B_bits", "delta_H_A2B_bits", "forced_token_A2B",
        "top_token_after_A2B", "top_token_p_after_A2B",

        "H_before_B2A_bits", "H_after_B2A_bits", "delta_H_B2A_bits", "forced_token_B2A",
        "top_token_after_B2A", "top_token_p_after_B2A",

        "order_effect_bits",
    ]

    row = {
        "run_id": run_id,
        "mode": mode,

        "H_before_A2B_bits": HbA,
        "H_after_A2B_bits": HaA,
        "delta_H_A2B_bits": dA,
        "forced_token_A2B": tokA,

        "top_token_after_A2B": top_tok_A,
        "top_token_p_after_A2B": top_p_A,

        "H_before_B2A_bits": HbB,
        "H_after_B2A_bits": HaB,
        "delta_H_B2A_bits": dB,
        "forced_token_B2A": tokB,

        "top_token_after_B2A": top_tok_B,
        "top_token_p_after_B2A": top_p_B,

        "order_effect_bits": order_effect,
    }

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerow(row)

# ================================================================
# RUN PROTOCOLS
# ================================================================
if MODE in ("A2B", "BOTH"):
    prompt0 = build_prompt()
    H_before, _, _ = unbiased_probe(prompt0)

    tokA = force_token(prompt0, A_id)
    promptA = prompt0 + tokA

    H_afterA, _, dist_afterA = unbiased_probe(promptA)

    dH_A2B = H_afterA - H_before
else:
    H_before = H_afterA = dH_A2B = tokA = dist_afterA = None

if MODE in ("B2A", "BOTH"):
    prompt0 = build_prompt()
    H_before2, _, _ = unbiased_probe(prompt0)

    tokB = force_token(prompt0, B_id)
    promptB = prompt0 + tokB

    H_afterB, _, dist_afterB = unbiased_probe(promptB)

    dH_B2A = H_afterB - H_before2
else:
    H_before2 = H_afterB = dH_B2A = tokB = dist_afterB = None

# ================================================================
# WRITE SUMMARY
# ================================================================
summary_path = OUT_DIR / f"summary_{RUN_ID}_{MODE}.csv"

write_summary(
    RUN_ID,
    MODE,
    (H_before, H_afterA, dH_A2B, tokA, dist_afterA),
    (H_before2, H_afterB, dH_B2A, tokB, dist_afterB),
    summary_path,
)

print("Finished run", RUN_ID, "MODE=", MODE)
print("Wrote summary to", str(summary_path))







