from llama_cpp import Llama
import math
import os
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

    with out_path.open("w", encoding="utf-8") as f:
        f.write(f"=== RUN {run_id} ({mode}) ===\n\n")

        # -------------------------
        # A to B SEQUENCE
        # -------------------------
        if HbA is not None:
            f.write("A to B\n")
            f.write(f"  H_before:  {HbA:.6f} bits\n")
            f.write(f"  H_after:   {HaA:.6f} bits\n")
            f.write(f"  d_H:        {dA:.6f} bits\n")
            f.write(f"  Forced:    {tokA!r}\n\n")

            if distA is not None:
                f.write("  Top tokens after forcing A:\n")
                for tok, p, lp in distA[:TOPN]:
                    f.write(f"    {tok!r}: p={p:.4f}, logp={lp:.4f}\n")
                f.write("\n")

        # -------------------------
        # B to A SEQUENCE
        # -------------------------
        if HbB is not None:
            f.write("B to A\n")
            f.write(f"  H_before:  {HbB:.6f} bits\n")
            f.write(f"  H_after:   {HaB:.6f} bits\n")
            f.write(f"  d_H:        {dB:.6f} bits\n")
            f.write(f"  Forced:    {tokB!r}\n\n")

            if distB is not None:
                f.write("  Top tokens after forcing B:\n")
                for tok, p, lp in distB[:TOPN]:
                    f.write(f"    {tok!r}: p={p:.4f}, logp={lp:.4f}\n")
                f.write("\n")

        if order_effect is not None:
            f.write(f"Order Effect (d_H_AB − d_H_BA): {order_effect:.6f} bits\n")


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
summary_path = OUT_DIR / f"summary_{RUN_ID}_{MODE}.txt"

write_summary(
    RUN_ID,
    MODE,
    (H_before, H_afterA, dH_A2B, tokA, dist_afterA),
    (H_before2, H_afterB, dH_B2A, tokB, dist_afterB),
    summary_path,
)

print("Finished run", RUN_ID, "MODE=", MODE)
print("Wrote summary to", str(summary_path))





